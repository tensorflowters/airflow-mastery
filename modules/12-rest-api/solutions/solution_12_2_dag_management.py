"""
Solution 12.2: DAG Management
==============================

Complete DAG management operations via REST API including
triggering runs, monitoring progress, and bulk operations.
"""

import time
from typing import Optional
from solution_12_1_api_basics import AirflowAPIClient, APIError
import logging

logger = logging.getLogger(__name__)


class DAGManager:
    """
    DAG management operations via API.

    Provides high-level operations for:
    - Triggering and monitoring DAG runs
    - Task instance management
    - Bulk DAG operations
    - Failed run recovery
    """

    # Terminal states for DAG runs
    TERMINAL_STATES = {"success", "failed", "upstream_failed"}

    def __init__(self, client: AirflowAPIClient):
        """
        Initialize DAG manager.

        Args:
            client: Configured AirflowAPIClient instance
        """
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

        Args:
            dag_id: DAG to trigger
            conf: Configuration to pass to DAG
            timeout: Maximum wait time in seconds
            poll_interval: Seconds between status checks

        Returns:
            Final DAG run details including state

        Raises:
            TimeoutError: If run doesn't complete within timeout
            APIError: On API errors
        """
        # Step 1: Trigger the DAG run
        logger.info(f"Triggering DAG: {dag_id}")
        run = self.client.trigger_dag(dag_id, conf=conf)
        run_id = run["dag_run_id"]
        logger.info(f"Triggered run: {run_id}")

        # Step 2: Poll for completion
        start_time = time.time()
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                raise TimeoutError(
                    f"DAG run {run_id} did not complete within {timeout}s"
                )

            # Get current status
            run_status = self.client.get_dag_run(dag_id, run_id)
            state = run_status.get("state", "unknown")

            logger.debug(f"Run {run_id} state: {state} (elapsed: {elapsed:.0f}s)")

            # Check if complete
            if state in self.TERMINAL_STATES:
                logger.info(f"Run {run_id} completed with state: {state}")
                return run_status

            # Wait before next check
            time.sleep(poll_interval)

    def get_run_tasks(
        self,
        dag_id: str,
        run_id: str,
        state: str = None,
    ) -> list:
        """
        Get all task instances for a run.

        Args:
            dag_id: DAG identifier
            run_id: Run identifier
            state: Optional filter by state

        Returns:
            List of task instance details
        """
        params = {"limit": 100, "offset": 0}
        if state:
            params["state"] = state

        all_tasks = []
        while True:
            response = self.client._request(
                "GET",
                f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
                params=params,
            )

            tasks = response.get("task_instances", [])
            all_tasks.extend(tasks)

            # Check for more pages
            total = response.get("total_entries", 0)
            if len(all_tasks) >= total or len(tasks) < params["limit"]:
                break

            params["offset"] += params["limit"]

        return all_tasks

    def get_task_logs(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        try_number: int = 1,
    ) -> str:
        """
        Get logs for a specific task instance.

        Args:
            dag_id: DAG identifier
            run_id: Run identifier
            task_id: Task identifier
            try_number: Attempt number (default 1)

        Returns:
            Task log content as string
        """
        response = self.client._request(
            "GET",
            f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}",
        )
        return response.get("content", "")

    def pause_dags_by_tag(self, tag: str, pause: bool = True) -> list:
        """
        Pause or unpause all DAGs with a specific tag.

        Args:
            tag: Tag to filter by
            pause: True to pause, False to unpause

        Returns:
            List of affected DAG IDs
        """
        # Get all DAGs with the tag
        dags = self.client.get_all_dags(tags=[tag])

        affected = []
        for dag in dags:
            dag_id = dag["dag_id"]
            current_pause = dag.get("is_paused", False)

            # Only update if state needs to change
            if current_pause != pause:
                try:
                    self.client.pause_dag(dag_id, pause=pause)
                    affected.append(dag_id)
                    action = "paused" if pause else "unpaused"
                    logger.info(f"{action.title()} DAG: {dag_id}")
                except APIError as e:
                    logger.warning(f"Failed to update {dag_id}: {e}")

        return affected

    def clear_failed_runs(
        self,
        dag_id: str,
        dry_run: bool = False,
    ) -> int:
        """
        Clear all failed runs for a DAG.

        This allows failed runs to be re-executed.

        Args:
            dag_id: DAG identifier
            dry_run: If True, just count without clearing

        Returns:
            Number of runs cleared (or would be cleared)
        """
        # Get failed runs
        response = self.client.list_dag_runs(
            dag_id,
            state="failed",
            limit=100,
        )
        failed_runs = response.get("dag_runs", [])

        if dry_run:
            return len(failed_runs)

        cleared_count = 0
        for run in failed_runs:
            run_id = run["dag_run_id"]
            try:
                # Clear all task instances in the run
                self.client._request(
                    "POST",
                    f"/dags/{dag_id}/dagRuns/{run_id}/clear",
                    json={"dry_run": False},
                )
                cleared_count += 1
                logger.info(f"Cleared run: {run_id}")
            except APIError as e:
                logger.warning(f"Failed to clear {run_id}: {e}")

        return cleared_count

    def retry_failed_tasks(
        self,
        dag_id: str,
        run_id: str,
    ) -> list:
        """
        Retry all failed tasks in a specific run.

        Args:
            dag_id: DAG identifier
            run_id: Run identifier

        Returns:
            List of task IDs that were cleared for retry
        """
        # Get failed tasks
        failed_tasks = self.get_run_tasks(dag_id, run_id, state="failed")

        retried = []
        for task in failed_tasks:
            task_id = task["task_id"]
            try:
                # Clear specific task to allow retry
                self.client._request(
                    "POST",
                    f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/clear",
                    json={"dry_run": False},
                )
                retried.append(task_id)
                logger.info(f"Cleared task for retry: {task_id}")
            except APIError as e:
                logger.warning(f"Failed to clear {task_id}: {e}")

        return retried

    def get_run_summary(self, dag_id: str, run_id: str) -> dict:
        """
        Get a summary of a DAG run including task states.

        Args:
            dag_id: DAG identifier
            run_id: Run identifier

        Returns:
            Summary dict with run info and task state counts
        """
        # Get run details
        run = self.client.get_dag_run(dag_id, run_id)

        # Get all tasks
        tasks = self.get_run_tasks(dag_id, run_id)

        # Count task states
        state_counts = {}
        for task in tasks:
            state = task.get("state", "unknown")
            state_counts[state] = state_counts.get(state, 0) + 1

        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "state": run.get("state"),
            "start_date": run.get("start_date"),
            "end_date": run.get("end_date"),
            "total_tasks": len(tasks),
            "task_states": state_counts,
        }

    def wait_for_dag_active(
        self,
        dag_id: str,
        timeout: int = 60,
        poll_interval: int = 2,
    ) -> bool:
        """
        Wait for a DAG to become active (not paused).

        Args:
            dag_id: DAG to check
            timeout: Maximum wait time
            poll_interval: Seconds between checks

        Returns:
            True if DAG is active, False if timeout
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            dag = self.client.get_dag(dag_id)
            if dag and not dag.get("is_paused", True):
                return True
            time.sleep(poll_interval)
        return False


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test the DAG manager."""
    print("DAG Manager - Solution 12.2")
    print("=" * 60)

    client = AirflowAPIClient()
    manager = DAGManager(client)

    # Check health first
    if not client.is_healthy():
        print("ERROR: Airflow is not healthy")
        return

    # List available DAGs
    print("\n1. Available DAGs:")
    try:
        dags = client.get_all_dags()
        for dag in dags[:5]:
            status = "paused" if dag.get("is_paused") else "active"
            print(f"   - {dag['dag_id']} ({status})")
        if len(dags) > 5:
            print(f"   ... and {len(dags) - 5} more")
    except Exception as e:
        print(f"   Error: {e}")

    # Example: Trigger and wait (commented out to avoid accidental triggers)
    print("\n2. Trigger and Wait Example:")
    print("   # manager.trigger_and_wait('example_dag', conf={'key': 'value'})")
    print("   # Would trigger DAG and poll until complete")

    # Example: Bulk operations
    print("\n3. Bulk Operations:")
    print("   # manager.pause_dags_by_tag('development', pause=True)")
    print("   # Would pause all DAGs tagged 'development'")

    # Example: Clear failed runs
    print("\n4. Clear Failed Runs:")
    print("   # count = manager.clear_failed_runs('etl_pipeline', dry_run=True)")
    print("   # Would count failed runs without clearing")

    print("\n" + "=" * 60)
    print("Test complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
