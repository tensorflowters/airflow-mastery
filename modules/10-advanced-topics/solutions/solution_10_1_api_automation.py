"""
Solution 10.1: REST API Automation
===================================

Complete automation script using Airflow's REST API v2.

Features:
1. Comprehensive API client with error handling
2. Stale DAG detection
3. Automated triggering with monitoring
4. Detailed reporting

Usage:
    python solution_10_1_api_automation.py

    # Or with environment variables:
    AIRFLOW_URL=http://localhost:8080/api/v2 \
    AIRFLOW_USER=admin \
    AIRFLOW_PASS=admin \
    python solution_10_1_api_automation.py
"""

import requests
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
import json
import time
import os
import logging
from dataclasses import dataclass
from enum import Enum

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080/api/v2")
AIRFLOW_USERNAME = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASS", "admin")

STALE_THRESHOLD_DAYS = 7
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS = 30


# =============================================================================
# DATA CLASSES
# =============================================================================


class DagRunState(Enum):
    """DAG run states."""
    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class StaleDag:
    """Information about a stale DAG."""
    dag_id: str
    last_run_date: Optional[datetime]
    days_since_run: int
    is_paused: bool
    has_import_errors: bool


@dataclass
class TriggerResult:
    """Result of triggering a DAG."""
    dag_id: str
    success: bool
    run_id: Optional[str] = None
    error: Optional[str] = None
    final_state: Optional[str] = None


# =============================================================================
# API CLIENT
# =============================================================================


class AirflowAPIError(Exception):
    """Exception for Airflow API errors."""
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"API Error {status_code}: {message}")


class AirflowClient:
    """
    Client for Airflow REST API v2.

    Provides methods for:
    - DAG operations (list, get, trigger)
    - DAG Run monitoring
    - Error handling and retry logic
    """

    def __init__(self, base_url: str, username: str, password: str):
        """Initialize client with authentication."""
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })

        # Verify connection
        self._verify_connection()

    def _verify_connection(self):
        """Verify API connection on initialization."""
        try:
            response = self.session.get(f"{self.base_url}/health")
            if response.status_code != 200:
                logger.warning(f"Health check returned {response.status_code}")
        except requests.exceptions.ConnectionError as e:
            raise AirflowAPIError(0, f"Cannot connect to Airflow: {e}")

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Dict = None,
        data: Dict = None,
        retries: int = 3
    ) -> Dict:
        """
        Make API request with retry logic.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            params: Query parameters
            data: Request body
            retries: Number of retries on failure

        Returns:
            JSON response as dict
        """
        url = f"{self.base_url}{endpoint}"

        for attempt in range(retries):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=data,
                    timeout=30
                )

                if response.status_code >= 400:
                    error_msg = response.text
                    try:
                        error_data = response.json()
                        error_msg = error_data.get('detail', error_msg)
                    except:
                        pass
                    raise AirflowAPIError(response.status_code, error_msg)

                return response.json()

            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    logger.warning(f"Request timeout, retrying ({attempt + 1}/{retries})")
                    time.sleep(2 ** attempt)
                else:
                    raise AirflowAPIError(0, "Request timed out")
            except requests.exceptions.ConnectionError:
                if attempt < retries - 1:
                    logger.warning(f"Connection error, retrying ({attempt + 1}/{retries})")
                    time.sleep(2 ** attempt)
                else:
                    raise

    def _get(self, endpoint: str, params: Dict = None) -> Dict:
        """Make GET request."""
        return self._request("GET", endpoint, params=params)

    def _post(self, endpoint: str, data: Dict = None) -> Dict:
        """Make POST request."""
        return self._request("POST", endpoint, data=data)

    # =========================================================================
    # DAG OPERATIONS
    # =========================================================================

    def list_dags(self, only_active: bool = False) -> List[Dict]:
        """
        List all DAGs with pagination handling.

        Args:
            only_active: If True, only return non-paused DAGs

        Returns:
            List of DAG objects
        """
        dags = []
        offset = 0
        limit = 100

        while True:
            params = {
                "offset": offset,
                "limit": limit,
            }
            if only_active:
                params["only_active"] = "true"

            response = self._get("/dags", params=params)
            page_dags = response.get("dags", [])
            dags.extend(page_dags)

            logger.debug(f"Fetched {len(page_dags)} DAGs (offset={offset})")

            if len(page_dags) < limit:
                break
            offset += limit

        logger.info(f"Total DAGs fetched: {len(dags)}")
        return dags

    def get_dag(self, dag_id: str) -> Dict:
        """Get details for a specific DAG."""
        return self._get(f"/dags/{dag_id}")

    def get_dag_runs(
        self,
        dag_id: str,
        limit: int = 10,
        order_by: str = "-start_date",
        state: str = None
    ) -> List[Dict]:
        """
        Get recent DAG runs.

        Args:
            dag_id: The DAG identifier
            limit: Maximum number of runs
            order_by: Sort order (prefix with - for descending)
            state: Filter by state (success, failed, running)

        Returns:
            List of DAG run objects
        """
        params = {
            "limit": limit,
            "order_by": order_by,
        }
        if state:
            params["state"] = state

        response = self._get(f"/dags/{dag_id}/dagRuns", params=params)
        return response.get("dag_runs", [])

    def trigger_dag(
        self,
        dag_id: str,
        conf: Dict = None,
        note: str = None
    ) -> Dict:
        """
        Trigger a new DAG run.

        Args:
            dag_id: The DAG to trigger
            conf: Configuration to pass to the DAG
            note: Optional note for the run

        Returns:
            The created DAG run object
        """
        payload = {
            "conf": conf or {},
        }
        if note:
            payload["note"] = note

        return self._post(f"/dags/{dag_id}/dagRuns", data=payload)

    def get_dag_run(self, dag_id: str, run_id: str) -> Dict:
        """Get details for a specific DAG run."""
        return self._get(f"/dags/{dag_id}/dagRuns/{run_id}")

    def wait_for_dag_run(
        self,
        dag_id: str,
        run_id: str,
        timeout_seconds: int = 300,
        poll_interval: int = 10
    ) -> Dict:
        """
        Wait for a DAG run to complete.

        Args:
            dag_id: The DAG identifier
            run_id: The run identifier
            timeout_seconds: Maximum time to wait
            poll_interval: Seconds between status checks

        Returns:
            Final DAG run state
        """
        end_states = {"success", "failed"}
        start_time = time.time()

        while True:
            run = self.get_dag_run(dag_id, run_id)
            state = run.get("state")

            logger.debug(f"DAG run {dag_id}/{run_id} state: {state}")

            if state in end_states:
                return run

            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                raise TimeoutError(
                    f"DAG run {dag_id}/{run_id} did not complete within {timeout_seconds}s"
                )

            time.sleep(poll_interval)

    def get_import_errors(self) -> List[Dict]:
        """Get all DAG import errors."""
        response = self._get("/importErrors")
        return response.get("import_errors", [])


# =============================================================================
# ANALYSIS FUNCTIONS
# =============================================================================


def find_stale_dags(
    client: AirflowClient,
    threshold_days: int = STALE_THRESHOLD_DAYS
) -> List[StaleDag]:
    """
    Find DAGs that haven't run within the threshold.

    Args:
        client: AirflowClient instance
        threshold_days: Days without a run to be considered stale

    Returns:
        List of StaleDag objects
    """
    stale_dags = []
    threshold = datetime.utcnow() - timedelta(days=threshold_days)

    # Get import errors for reference
    import_errors = {e['filename']: e for e in client.get_import_errors()}

    # Get all DAGs
    all_dags = client.list_dags()

    for dag in all_dags:
        dag_id = dag['dag_id']
        is_paused = dag.get('is_paused', False)

        # Skip if paused (intentionally not running)
        if is_paused:
            continue

        # Check for import errors
        has_errors = any(dag_id in e.get('filename', '') for e in import_errors.values())

        # Get most recent run
        runs = client.get_dag_runs(dag_id, limit=1)

        if not runs:
            # Never run
            stale_dags.append(StaleDag(
                dag_id=dag_id,
                last_run_date=None,
                days_since_run=999,
                is_paused=is_paused,
                has_import_errors=has_errors
            ))
            continue

        last_run = runs[0]
        last_run_date_str = last_run.get('start_date')

        if last_run_date_str:
            # Parse ISO format date
            last_run_date = datetime.fromisoformat(
                last_run_date_str.replace('Z', '+00:00')
            ).replace(tzinfo=None)

            if last_run_date < threshold:
                days_since = (datetime.utcnow() - last_run_date).days
                stale_dags.append(StaleDag(
                    dag_id=dag_id,
                    last_run_date=last_run_date,
                    days_since_run=days_since,
                    is_paused=is_paused,
                    has_import_errors=has_errors
                ))

    # Sort by days since run
    stale_dags.sort(key=lambda x: x.days_since_run, reverse=True)

    logger.info(f"Found {len(stale_dags)} stale DAGs")
    return stale_dags


def find_recent_failures(
    client: AirflowClient,
    hours: int = 24
) -> List[Dict]:
    """
    Find DAGs with recent failures.

    Args:
        client: AirflowClient instance
        hours: Look back period in hours

    Returns:
        List of dicts with dag_id and failure_count
    """
    failures = {}
    threshold = datetime.utcnow() - timedelta(hours=hours)

    all_dags = client.list_dags()

    for dag in all_dags:
        dag_id = dag['dag_id']

        # Get failed runs
        runs = client.get_dag_runs(dag_id, limit=50, state="failed")

        for run in runs:
            start_date_str = run.get('start_date')
            if start_date_str:
                start_date = datetime.fromisoformat(
                    start_date_str.replace('Z', '+00:00')
                ).replace(tzinfo=None)

                if start_date > threshold:
                    failures[dag_id] = failures.get(dag_id, 0) + 1

    # Convert to list and sort
    failure_list = [
        {"dag_id": dag_id, "failure_count": count}
        for dag_id, count in failures.items()
    ]
    failure_list.sort(key=lambda x: x['failure_count'], reverse=True)

    logger.info(f"Found {len(failure_list)} DAGs with recent failures")
    return failure_list


def trigger_maintenance_runs(
    client: AirflowClient,
    stale_dags: List[StaleDag],
    wait_for_completion: bool = False
) -> List[TriggerResult]:
    """
    Trigger maintenance runs for stale DAGs.

    Args:
        client: AirflowClient instance
        stale_dags: List of StaleDag objects
        wait_for_completion: If True, wait for each run to complete

    Returns:
        List of TriggerResult objects
    """
    results = []

    for stale_dag in stale_dags:
        dag_id = stale_dag.dag_id

        # Skip DAGs with import errors
        if stale_dag.has_import_errors:
            results.append(TriggerResult(
                dag_id=dag_id,
                success=False,
                error="DAG has import errors"
            ))
            continue

        try:
            # Trigger the DAG
            run = client.trigger_dag(
                dag_id=dag_id,
                conf={"maintenance_run": True, "triggered_by": "automation"},
                note=f"Maintenance run - DAG inactive for {stale_dag.days_since_run} days"
            )

            run_id = run.get('dag_run_id')
            logger.info(f"Triggered {dag_id}, run_id: {run_id}")

            result = TriggerResult(
                dag_id=dag_id,
                success=True,
                run_id=run_id
            )

            # Optionally wait for completion
            if wait_for_completion:
                try:
                    final_run = client.wait_for_dag_run(
                        dag_id, run_id,
                        timeout_seconds=300
                    )
                    result.final_state = final_run.get('state')
                except TimeoutError as e:
                    result.final_state = "timeout"
                    logger.warning(f"Timeout waiting for {dag_id}")

            results.append(result)

        except AirflowAPIError as e:
            logger.error(f"Failed to trigger {dag_id}: {e.message}")
            results.append(TriggerResult(
                dag_id=dag_id,
                success=False,
                error=e.message
            ))

    return results


# =============================================================================
# REPORT GENERATION
# =============================================================================


def generate_report(
    client: AirflowClient,
    stale_dags: List[StaleDag],
    trigger_results: List[TriggerResult],
    recent_failures: List[Dict]
) -> str:
    """Generate comprehensive maintenance report."""
    lines = [
        "=" * 60,
        "         AIRFLOW DAG MAINTENANCE REPORT",
        "=" * 60,
        f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
    ]

    # DAG Summary
    all_dags = client.list_dags()
    active_count = sum(1 for d in all_dags if not d.get('is_paused'))
    paused_count = sum(1 for d in all_dags if d.get('is_paused'))

    lines.extend([
        "-" * 60,
        "DAG SUMMARY",
        "-" * 60,
        f"  Total DAGs:    {len(all_dags)}",
        f"  Active:        {active_count}",
        f"  Paused:        {paused_count}",
        "",
    ])

    # Stale DAGs
    lines.extend([
        "-" * 60,
        f"STALE DAGS (no run in {STALE_THRESHOLD_DAYS} days)",
        "-" * 60,
    ])

    if stale_dags:
        for i, dag in enumerate(stale_dags, 1):
            last_run = dag.last_run_date.strftime('%Y-%m-%d') if dag.last_run_date else "Never"
            status = "⚠️ Import Errors" if dag.has_import_errors else ""
            lines.append(f"  {i}. {dag.dag_id}")
            lines.append(f"     Last run: {last_run} ({dag.days_since_run} days ago) {status}")
    else:
        lines.append("  No stale DAGs found! ✅")
    lines.append("")

    # Trigger Results
    lines.extend([
        "-" * 60,
        "TRIGGERED MAINTENANCE RUNS",
        "-" * 60,
    ])

    if trigger_results:
        for result in trigger_results:
            if result.success:
                state_str = f" [{result.final_state}]" if result.final_state else ""
                lines.append(f"  ✅ {result.dag_id}")
                lines.append(f"     Run ID: {result.run_id}{state_str}")
            else:
                lines.append(f"  ❌ {result.dag_id}")
                lines.append(f"     Error: {result.error}")
    else:
        lines.append("  No runs triggered")
    lines.append("")

    # Recent Failures
    lines.extend([
        "-" * 60,
        "RECENT FAILURES (last 24 hours)",
        "-" * 60,
    ])

    if recent_failures:
        for failure in recent_failures[:10]:  # Top 10
            lines.append(f"  ⚠️ {failure['dag_id']}: {failure['failure_count']} failure(s)")
    else:
        lines.append("  No recent failures! ✅")
    lines.append("")

    lines.extend([
        "=" * 60,
        "END OF REPORT",
        "=" * 60,
    ])

    return "\n".join(lines)


# =============================================================================
# MAIN EXECUTION
# =============================================================================


def main():
    """Main execution function."""
    logger.info("Starting Airflow DAG Maintenance Script")
    logger.info(f"Connecting to: {AIRFLOW_BASE_URL}")

    try:
        # Initialize client
        client = AirflowClient(
            base_url=AIRFLOW_BASE_URL,
            username=AIRFLOW_USERNAME,
            password=AIRFLOW_PASSWORD
        )
        logger.info("Connected to Airflow API")

        # Find stale DAGs
        logger.info("Analyzing DAGs for staleness...")
        stale_dags = find_stale_dags(client, STALE_THRESHOLD_DAYS)

        # Find recent failures
        logger.info("Checking for recent failures...")
        failures = find_recent_failures(client, hours=24)

        # Trigger maintenance runs (set to False by default for safety)
        trigger_enabled = os.environ.get("ENABLE_TRIGGERS", "false").lower() == "true"

        if trigger_enabled and stale_dags:
            logger.info(f"Triggering {len(stale_dags)} maintenance runs...")
            trigger_results = trigger_maintenance_runs(
                client, stale_dags,
                wait_for_completion=False
            )
        else:
            if not trigger_enabled:
                logger.info("Triggers disabled (set ENABLE_TRIGGERS=true to enable)")
            trigger_results = []

        # Generate report
        report = generate_report(client, stale_dags, trigger_results, failures)
        print("\n" + report)

        # Return summary for programmatic use
        return {
            "stale_dags": len(stale_dags),
            "failures": len(failures),
            "triggered": sum(1 for r in trigger_results if r.success),
        }

    except AirflowAPIError as e:
        logger.error(f"API Error: {e.message}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
