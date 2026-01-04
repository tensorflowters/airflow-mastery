"""
Solution 12.1: API Basics
==========================

Complete Airflow REST API v2 client with authentication,
error handling, and pagination.
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Any
import logging

logger = logging.getLogger(__name__)


class APIError(Exception):
    """Custom exception for API errors."""

    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code


class AirflowAPIClient:
    """
    Robust client for Airflow REST API v2.

    Features:
    - Session-based authentication
    - Automatic retry with backoff
    - Proper error handling
    - Pagination support
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080/api/v2",
        username: str = "admin",
        password: str = "admin",
        timeout: tuple = (5, 30),
        max_retries: int = 3,
    ):
        """
        Initialize API client.

        Args:
            base_url: Airflow API base URL
            username: Authentication username
            password: Authentication password
            timeout: (connect_timeout, read_timeout) in seconds
            max_retries: Maximum retry attempts
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

        # Create session with retry logic
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET", "POST", "PATCH", "DELETE"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _build_url(self, endpoint: str) -> str:
        """Build full URL from endpoint."""
        if not endpoint.startswith("/"):
            endpoint = f"/{endpoint}"
        return f"{self.base_url}{endpoint}"

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict = None,
        json: dict = None,
    ) -> dict:
        """
        Make API request with comprehensive error handling.

        Args:
            method: HTTP method
            endpoint: API endpoint
            params: Query parameters
            json: JSON body

        Returns:
            Parsed JSON response

        Raises:
            APIError: On API errors
            ConnectionError: On connection failures
        """
        url = self._build_url(endpoint)

        try:
            logger.debug(f"API request: {method} {url}")

            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                timeout=self.timeout,
            )

            logger.debug(f"Response status: {response.status_code}")

            # Handle specific status codes
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 204:
                return {}  # No content
            elif response.status_code == 401:
                raise APIError("Authentication failed", 401)
            elif response.status_code == 403:
                raise APIError("Permission denied", 403)
            elif response.status_code == 404:
                raise APIError(f"Resource not found: {endpoint}", 404)
            elif response.status_code == 409:
                raise APIError("Conflict - resource already exists", 409)
            elif response.status_code >= 500:
                raise APIError(
                    f"Server error: {response.text}",
                    response.status_code
                )
            else:
                response.raise_for_status()
                return response.json()

        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(
                f"Cannot connect to Airflow API at {self.base_url}"
            ) from e
        except requests.exceptions.Timeout as e:
            raise APIError("Request timed out", 408) from e

    # =========================================================================
    # HEALTH & INFO
    # =========================================================================

    def health_check(self) -> dict:
        """
        Check API health status.

        Returns:
            Dict with scheduler, metadata_database, triggerer health
        """
        return self._request("GET", "/health")

    def get_version(self) -> dict:
        """
        Get Airflow version information.

        Returns:
            Dict with version, git_version fields
        """
        return self._request("GET", "/version")

    def is_healthy(self) -> bool:
        """
        Check if Airflow is healthy.

        Returns:
            True if all components are healthy
        """
        try:
            health = self.health_check()
            scheduler = health.get("scheduler", {})
            return scheduler.get("status") == "healthy"
        except Exception:
            return False

    # =========================================================================
    # DAG OPERATIONS
    # =========================================================================

    def list_dags(
        self,
        limit: int = 100,
        offset: int = 0,
        tags: list = None,
        only_active: bool = None,
    ) -> dict:
        """
        List DAGs with pagination and filtering.

        Args:
            limit: Maximum DAGs to return
            offset: Starting position
            tags: Filter by tags
            only_active: Filter paused state

        Returns:
            Dict with 'dags' list and 'total_entries'
        """
        params = {
            "limit": limit,
            "offset": offset,
        }

        if tags:
            params["tags"] = tags
        if only_active is not None:
            params["only_active"] = str(only_active).lower()

        return self._request("GET", "/dags", params=params)

    def get_all_dags(self, **filters) -> list:
        """
        Get all DAGs, handling pagination automatically.

        Args:
            **filters: Passed to list_dags (tags, only_active)

        Returns:
            List of all DAG objects
        """
        all_dags = []
        offset = 0
        limit = 100

        while True:
            response = self.list_dags(
                limit=limit,
                offset=offset,
                **filters
            )
            dags = response.get("dags", [])
            all_dags.extend(dags)

            # Check if we have all DAGs
            total = response.get("total_entries", 0)
            if len(all_dags) >= total or len(dags) < limit:
                break

            offset += limit
            logger.debug(f"Fetched {len(all_dags)}/{total} DAGs")

        return all_dags

    def get_dag(self, dag_id: str) -> Optional[dict]:
        """
        Get specific DAG details.

        Args:
            dag_id: DAG identifier

        Returns:
            DAG details or None if not found
        """
        try:
            return self._request("GET", f"/dags/{dag_id}")
        except APIError as e:
            if e.status_code == 404:
                return None
            raise

    def pause_dag(self, dag_id: str, pause: bool = True) -> dict:
        """
        Pause or unpause a DAG.

        Args:
            dag_id: DAG identifier
            pause: True to pause, False to unpause

        Returns:
            Updated DAG details
        """
        return self._request(
            "PATCH",
            f"/dags/{dag_id}",
            json={"is_paused": pause}
        )

    # =========================================================================
    # DAG RUN OPERATIONS
    # =========================================================================

    def list_dag_runs(
        self,
        dag_id: str,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "-start_date",
        state: str = None,
    ) -> dict:
        """
        List DAG runs.

        Args:
            dag_id: DAG identifier
            limit: Max runs to return
            offset: Starting position
            order_by: Sort field (prefix - for desc)
            state: Filter by state

        Returns:
            Dict with 'dag_runs' list
        """
        params = {
            "limit": limit,
            "offset": offset,
            "order_by": order_by,
        }
        if state:
            params["state"] = state

        return self._request(
            "GET",
            f"/dags/{dag_id}/dagRuns",
            params=params
        )

    def get_dag_run(self, dag_id: str, run_id: str) -> Optional[dict]:
        """
        Get specific DAG run details.

        Args:
            dag_id: DAG identifier
            run_id: Run identifier

        Returns:
            DAG run details or None
        """
        try:
            return self._request(
                "GET",
                f"/dags/{dag_id}/dagRuns/{run_id}"
            )
        except APIError as e:
            if e.status_code == 404:
                return None
            raise

    def trigger_dag(
        self,
        dag_id: str,
        conf: dict = None,
        logical_date: str = None,
        note: str = None,
    ) -> dict:
        """
        Trigger a new DAG run.

        Args:
            dag_id: DAG to trigger
            conf: Configuration to pass
            logical_date: Optional execution date
            note: Optional note

        Returns:
            Created DAG run details
        """
        payload = {}
        if conf:
            payload["conf"] = conf
        if logical_date:
            payload["logical_date"] = logical_date
        if note:
            payload["note"] = note

        return self._request(
            "POST",
            f"/dags/{dag_id}/dagRuns",
            json=payload
        )

    # =========================================================================
    # TASK OPERATIONS
    # =========================================================================

    def list_tasks(self, dag_id: str) -> dict:
        """
        List tasks in a DAG.

        Args:
            dag_id: DAG identifier

        Returns:
            Dict with 'tasks' list
        """
        return self._request("GET", f"/dags/{dag_id}/tasks")

    def get_task_instance(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
    ) -> Optional[dict]:
        """
        Get specific task instance.

        Args:
            dag_id: DAG identifier
            run_id: Run identifier
            task_id: Task identifier

        Returns:
            Task instance details
        """
        try:
            return self._request(
                "GET",
                f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}"
            )
        except APIError as e:
            if e.status_code == 404:
                return None
            raise


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test the API client."""
    print("Airflow API Client - Solution 12.1")
    print("=" * 60)

    client = AirflowAPIClient()

    # Health check
    print("\n1. Health Check:")
    try:
        health = client.health_check()
        print(f"   Scheduler: {health.get('scheduler', {}).get('status')}")
        print(f"   Database: {health.get('metadatabase', {}).get('status')}")
        print(f"   Is Healthy: {client.is_healthy()}")
    except Exception as e:
        print(f"   Error: {e}")

    # Version
    print("\n2. Version:")
    try:
        version = client.get_version()
        print(f"   Airflow: {version.get('version')}")
    except Exception as e:
        print(f"   Error: {e}")

    # DAGs
    print("\n3. DAGs:")
    try:
        dags = client.get_all_dags()
        print(f"   Total: {len(dags)}")
        for dag in dags[:5]:
            status = "paused" if dag.get("is_paused") else "active"
            print(f"   - {dag['dag_id']} ({status})")
        if len(dags) > 5:
            print(f"   ... and {len(dags) - 5} more")
    except Exception as e:
        print(f"   Error: {e}")

    # Get specific DAG
    print("\n4. Get Specific DAG:")
    try:
        dag = client.get_dag("nonexistent_dag")
        print(f"   Nonexistent DAG: {dag}")
    except Exception as e:
        print(f"   Error: {e}")

    print("\n" + "=" * 60)
    print("Test complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
