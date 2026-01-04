"""
Exercise 10.1: REST API Automation (Starter)
=============================================

Build an automation script using Airflow's REST API v2.

Objectives:
1. List and analyze DAGs via API
2. Find stale DAGs (no run in 7 days)
3. Trigger maintenance runs
4. Generate health report

Requirements:
- requests library: pip install requests
- Access to an Airflow instance with API enabled
"""

import requests
from datetime import datetime, timedelta
from typing import Optional
import json
import time


# =============================================================================
# CONFIGURATION
# =============================================================================

# TODO: Configure these for your Airflow instance
AIRFLOW_BASE_URL = "http://localhost:8080/api/v2"
AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "admin"

# Thresholds
STALE_THRESHOLD_DAYS = 7
POLL_INTERVAL_SECONDS = 10
MAX_POLL_ATTEMPTS = 30


# =============================================================================
# API CLIENT
# =============================================================================


class AirflowClient:
    """
    Client for interacting with Airflow REST API v2.

    TODO: Implement the methods below.
    """

    def __init__(self, base_url: str, username: str, password: str):
        """Initialize the API client with authentication."""
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        # TODO: Set up authentication
        # self.session.auth = (username, password)
        # TODO: Set up headers
        # self.session.headers.update({...})

    def _get(self, endpoint: str, params: dict = None) -> dict:
        """
        Make GET request to API.

        Args:
            endpoint: API endpoint (e.g., "/dags")
            params: Query parameters

        Returns:
            JSON response as dict
        """
        # TODO: Implement GET request with error handling
        pass

    def _post(self, endpoint: str, data: dict = None) -> dict:
        """
        Make POST request to API.

        Args:
            endpoint: API endpoint
            data: Request body as dict

        Returns:
            JSON response as dict
        """
        # TODO: Implement POST request with error handling
        pass

    # =========================================================================
    # DAG OPERATIONS
    # =========================================================================

    def list_dags(self, only_active: bool = False) -> list:
        """
        List all DAGs with pagination handling.

        Args:
            only_active: If True, only return active (non-paused) DAGs

        Returns:
            List of DAG objects
        """
        # TODO: Implement with pagination
        # Hint: Use offset and limit parameters
        # while True:
        #     response = self._get(f"/dags?offset={offset}&limit=100")
        #     ...
        pass

    def get_dag(self, dag_id: str) -> dict:
        """
        Get details for a specific DAG.

        Args:
            dag_id: The DAG identifier

        Returns:
            DAG details dict
        """
        # TODO: Implement
        pass

    def get_dag_runs(
        self,
        dag_id: str,
        limit: int = 10,
        order_by: str = "-start_date"
    ) -> list:
        """
        Get recent DAG runs.

        Args:
            dag_id: The DAG identifier
            limit: Maximum number of runs to return
            order_by: Sort order (default: most recent first)

        Returns:
            List of DAG run objects
        """
        # TODO: Implement
        pass

    def trigger_dag(
        self,
        dag_id: str,
        conf: dict = None,
        note: str = None
    ) -> dict:
        """
        Trigger a new DAG run.

        Args:
            dag_id: The DAG to trigger
            conf: Configuration to pass to the DAG
            note: Optional note for the run

        Returns:
            The created DAG run object
        """
        # TODO: Implement
        # POST to /dags/{dag_id}/dagRuns
        pass

    def get_dag_run(self, dag_id: str, run_id: str) -> dict:
        """
        Get details for a specific DAG run.

        Args:
            dag_id: The DAG identifier
            run_id: The run identifier

        Returns:
            DAG run details
        """
        # TODO: Implement
        pass

    def wait_for_dag_run(
        self,
        dag_id: str,
        run_id: str,
        timeout_seconds: int = 300
    ) -> dict:
        """
        Wait for a DAG run to complete.

        Args:
            dag_id: The DAG identifier
            run_id: The run identifier
            timeout_seconds: Maximum time to wait

        Returns:
            Final DAG run state
        """
        # TODO: Implement polling loop
        # Check state until it's not 'running' or 'queued'
        pass


# =============================================================================
# ANALYSIS FUNCTIONS
# =============================================================================


def find_stale_dags(
    client: AirflowClient,
    threshold_days: int = STALE_THRESHOLD_DAYS
) -> list:
    """
    Find DAGs that haven't run within the threshold.

    Args:
        client: AirflowClient instance
        threshold_days: Number of days without a run to be considered stale

    Returns:
        List of stale DAG information
    """
    stale_dags = []
    threshold = datetime.utcnow() - timedelta(days=threshold_days)

    # TODO: Implement
    # 1. Get all active DAGs
    # 2. For each DAG, get the most recent run
    # 3. If no recent run or last run is older than threshold, add to stale list

    return stale_dags


def find_recent_failures(
    client: AirflowClient,
    hours: int = 24
) -> list:
    """
    Find DAGs with recent failures.

    Args:
        client: AirflowClient instance
        hours: Look back period in hours

    Returns:
        List of DAGs with failure counts
    """
    # TODO: Implement
    # Use /dags/{dag_id}/dagRuns with state filter
    pass


def trigger_maintenance_runs(
    client: AirflowClient,
    dag_ids: list
) -> dict:
    """
    Trigger maintenance runs for specified DAGs.

    Args:
        client: AirflowClient instance
        dag_ids: List of DAG IDs to trigger

    Returns:
        Dict with success and failure lists
    """
    results = {
        "triggered": [],
        "failed": [],
    }

    # TODO: Implement
    # For each DAG:
    # 1. Trigger with maintenance flag in conf
    # 2. Record success or failure

    return results


# =============================================================================
# REPORT GENERATION
# =============================================================================


def generate_report(
    client: AirflowClient,
    stale_dags: list,
    triggered_results: dict,
    recent_failures: list
) -> str:
    """
    Generate a comprehensive maintenance report.

    Returns:
        Formatted report string
    """
    report_lines = [
        "=" * 50,
        "AIRFLOW DAG MAINTENANCE REPORT",
        f"Generated: {datetime.utcnow().isoformat()}",
        "=" * 50,
        "",
    ]

    # TODO: Add DAG summary section
    # - Total DAGs
    # - Active vs paused

    # TODO: Add stale DAGs section

    # TODO: Add triggered runs section

    # TODO: Add recent failures section

    return "\n".join(report_lines)


# =============================================================================
# MAIN EXECUTION
# =============================================================================


def main():
    """Main execution function."""
    print("Airflow DAG Maintenance Script")
    print("-" * 40)

    # Initialize client
    client = AirflowClient(
        base_url=AIRFLOW_BASE_URL,
        username=AIRFLOW_USERNAME,
        password=AIRFLOW_PASSWORD
    )

    # TODO: Implement main workflow
    # 1. Find stale DAGs
    # stale_dags = find_stale_dags(client)

    # 2. Optionally trigger maintenance runs
    # triggered = trigger_maintenance_runs(client, [d['dag_id'] for d in stale_dags])

    # 3. Find recent failures
    # failures = find_recent_failures(client)

    # 4. Generate and print report
    # report = generate_report(client, stale_dags, triggered, failures)
    # print(report)

    print("TODO: Implement the main workflow!")


if __name__ == "__main__":
    main()
