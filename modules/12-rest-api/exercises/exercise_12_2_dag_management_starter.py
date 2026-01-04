"""
Exercise 12.2: DAG Management (Starter)
========================================

Build DAG management operations via REST API.

TODO: Implement the management functions.
"""

import time
from typing import Optional
from solution_12_1_api_basics import AirflowAPIClient


class DAGManager:
    """DAG management operations via API."""

    def __init__(self, client: AirflowAPIClient):
        self.client = client

    def trigger_and_wait(
        self,
        dag_id: str,
        conf: dict = None,
        timeout: int = 3600,
        poll_interval: int = 10,
    ) -> dict:
        """
        Trigger DAG and wait for completion.

        TODO: Implement:
        1. Trigger DAG run
        2. Poll status until done
        3. Return final state
        """
        # TODO: Implement
        pass

    def get_run_tasks(self, dag_id: str, run_id: str) -> list:
        """
        Get all task instances for a run.

        TODO: Implement task instance retrieval.
        """
        # TODO: Implement
        pass

    def pause_dags_by_tag(self, tag: str, pause: bool = True) -> list:
        """
        Pause/unpause all DAGs with tag.

        TODO: Implement bulk pause operation.
        """
        # TODO: Implement
        pass

    def clear_failed_runs(self, dag_id: str) -> int:
        """
        Clear all failed runs for a DAG.

        TODO: Implement failed run clearing.
        """
        # TODO: Implement
        pass


if __name__ == "__main__":
    client = AirflowAPIClient()
    manager = DAGManager(client)

    # Test operations
    print("DAG Manager Test")
