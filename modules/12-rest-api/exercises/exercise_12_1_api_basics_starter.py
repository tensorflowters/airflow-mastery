"""
Exercise 12.1: API Basics (Starter)
====================================

Learn Airflow REST API v2 fundamentals.

Learning Goals:
1. Set up authenticated API client
2. Make basic API requests
3. Handle responses and errors

TODO: Complete the TODOs below.
"""

import requests
from typing import Optional
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# API CLIENT
# =============================================================================


class AirflowAPIClient:
    """
    Basic client for Airflow REST API v2.

    TODO: Implement all methods.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080/api/v2",
        username: str = "admin",
        password: str = "admin",
    ):
        """
        Initialize API client with authentication.

        Args:
            base_url: Airflow API base URL
            username: Authentication username
            password: Authentication password
        """
        self.base_url = base_url.rstrip("/")

        # TODO: Create requests Session with authentication
        self.session = requests.Session()
        # self.session.auth = ...
        # self.session.headers.update(...)

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict = None,
        json: dict = None,
    ) -> dict:
        """
        Make API request with error handling.

        TODO: Implement request method with:
        - Proper URL construction
        - Error handling for different status codes
        - JSON response parsing

        Args:
            method: HTTP method (GET, POST, PATCH, DELETE)
            endpoint: API endpoint (e.g., "/dags")
            params: Query parameters
            json: JSON body for POST/PATCH

        Returns:
            Parsed JSON response

        Raises:
            requests.HTTPError: On HTTP errors
        """
        # TODO: Implement
        pass

    # =========================================================================
    # PART 1: HEALTH & INFO
    # =========================================================================

    def health_check(self) -> dict:
        """
        Check API health.

        TODO: GET /health

        Returns:
            Health status dict
        """
        # TODO: Implement
        pass

    def get_version(self) -> dict:
        """
        Get Airflow version information.

        TODO: GET /version

        Returns:
            Version info dict
        """
        # TODO: Implement
        pass

    # =========================================================================
    # PART 2: DAG OPERATIONS
    # =========================================================================

    def list_dags(self, limit: int = 100, offset: int = 0) -> dict:
        """
        List DAGs with pagination.

        TODO: GET /dags with limit and offset params

        Args:
            limit: Max DAGs to return
            offset: Starting position

        Returns:
            Dict with 'dags' list and 'total_entries'
        """
        # TODO: Implement
        pass

    def get_all_dags(self) -> list:
        """
        Get all DAGs handling pagination.

        TODO: Call list_dags repeatedly until all DAGs retrieved

        Returns:
            List of all DAG objects
        """
        # TODO: Implement pagination handling
        pass

    def get_dag(self, dag_id: str) -> Optional[dict]:
        """
        Get specific DAG details.

        TODO: GET /dags/{dag_id}

        Args:
            dag_id: DAG identifier

        Returns:
            DAG details or None if not found
        """
        # TODO: Implement
        pass

    # =========================================================================
    # PART 3: ERROR HANDLING
    # =========================================================================

    def test_error_handling(self):
        """
        Test error handling for different scenarios.

        TODO: Test these scenarios:
        1. Invalid endpoint (404)
        2. Invalid credentials (401)
        3. Connection error
        """
        # Test 404
        try:
            self._request("GET", "/nonexistent")
        except Exception as e:
            print(f"404 test: {type(e).__name__}: {e}")

        # Test invalid DAG
        result = self.get_dag("nonexistent_dag_id_12345")
        print(f"Nonexistent DAG: {result}")


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test the API client."""
    print("Airflow API Client Test")
    print("=" * 50)

    # Initialize client
    client = AirflowAPIClient()

    # Test health check
    print("\n1. Health Check:")
    try:
        health = client.health_check()
        print(f"   Status: {health}")
    except Exception as e:
        print(f"   Error: {e}")

    # Test version
    print("\n2. Version Info:")
    try:
        version = client.get_version()
        print(f"   Version: {version}")
    except Exception as e:
        print(f"   Error: {e}")

    # Test DAG listing
    print("\n3. DAG Listing:")
    try:
        dags = client.get_all_dags()
        print(f"   Found {len(dags)} DAGs")
        for dag in dags[:5]:  # Show first 5
            print(f"   - {dag.get('dag_id')}")
    except Exception as e:
        print(f"   Error: {e}")

    # Test error handling
    print("\n4. Error Handling:")
    client.test_error_handling()

    print("\n" + "=" * 50)
    print("Test complete!")


if __name__ == "__main__":
    main()
