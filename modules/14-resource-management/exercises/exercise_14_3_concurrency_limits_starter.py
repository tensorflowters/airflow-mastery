"""
Exercise 14.3: Concurrency Limits (Starter)
============================================

Implement concurrency controls at DAG and task levels.

TODO: Complete all the implementation sections.
"""

from typing import Optional
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# PART 1: CONCURRENCY CONFIGURATION
# =============================================================================


class ConcurrencyConfig:
    """
    Manage concurrency settings for DAGs and tasks.

    Concurrency levels:
    - System: Global parallelism across all DAGs
    - DAG: Max active runs per DAG
    - Task: Max active instances per task
    """

    # Default concurrency settings
    DEFAULTS = {
        "parallelism": 32,              # Global max tasks
        "max_active_runs_per_dag": 16,  # Per DAG runs
        "max_active_tasks_per_dag": 16, # Per DAG tasks
    }

    def get_dag_concurrency(self, dag_id: str) -> Optional[dict]:
        """
        Get DAG concurrency settings.

        TODO: Implement:
        1. Query DAG model
        2. Return concurrency settings

        Returns:
            Dict with max_active_runs, max_active_tasks, etc.
        """
        # TODO: Implement
        pass

    def set_dag_concurrency(
        self,
        dag_id: str,
        max_active_runs: int = None,
        max_active_tasks: int = None,
    ) -> bool:
        """
        Update DAG concurrency settings.

        TODO: Implement:
        1. Query DAG
        2. Update settings
        3. Commit changes

        Returns:
            True if successful
        """
        # TODO: Implement
        pass

    def get_system_concurrency(self) -> dict:
        """
        Get global concurrency settings.

        TODO: Read from Airflow configuration.

        Returns:
            Dict with parallelism, limits, etc.
        """
        # TODO: Implement
        pass

    def validate_settings(self, settings: dict) -> tuple:
        """
        Validate concurrency settings.

        TODO: Implement validation:
        - max_active_runs > 0
        - max_active_tasks > 0
        - task concurrency <= dag concurrency

        Returns:
            (is_valid, list of errors)
        """
        # TODO: Implement
        pass


# =============================================================================
# PART 2: CONCURRENCY-CONTROLLED DAG (Template)
# =============================================================================


"""
TODO: Create a DAG that demonstrates concurrency controls.

Requirements:
1. Use TaskFlow API
2. Set DAG-level concurrency:
   - max_active_runs=3
   - max_active_tasks=10

3. Create tasks with different concurrency settings:
   - light_task: max_active_tis_per_dag=10
   - medium_task: max_active_tis_per_dag=5
   - heavy_task: max_active_tis_per_dag=2, max_active_tis_per_dagrun=1

4. Show how limits interact
"""

# from airflow.sdk import dag, task
# from datetime import datetime
#
# @dag(
#     max_active_runs=3,
#     ...
# )
# def controlled_pipeline():
#     pass


# =============================================================================
# PART 3: CONCURRENCY MONITOR
# =============================================================================


class ConcurrencyMonitor:
    """
    Monitor concurrency usage across the Airflow system.

    Tracks:
    - Running DAG runs
    - Running task instances
    - Available capacity
    """

    def get_running_dags(self, dag_id: str = None) -> dict:
        """
        Get currently running DAG runs.

        TODO: Implement:
        1. Query DagRun with RUNNING state
        2. Return summary

        Args:
            dag_id: Optional filter by DAG

        Returns:
            Dict with running run info
        """
        # TODO: Implement
        pass

    def get_running_tasks(self, dag_id: str = None) -> dict:
        """
        Get currently running task instances.

        TODO: Implement:
        1. Query TaskInstance with RUNNING state
        2. Return summary by DAG and task

        Returns:
            Dict with running task info
        """
        # TODO: Implement
        pass

    def check_capacity(self, dag_id: str = None) -> dict:
        """
        Check available capacity.

        TODO: Implement:
        1. Get current concurrency settings
        2. Count running instances
        3. Calculate available capacity

        Returns:
            Dict with capacity info
        """
        # TODO: Implement
        pass

    def get_concurrency_report(self) -> str:
        """
        Generate a formatted concurrency report.

        TODO: Implement report generation.

        Returns:
            Formatted report string
        """
        # TODO: Implement
        pass

    def is_at_capacity(self, dag_id: str = None) -> bool:
        """
        Check if system/DAG is at capacity.

        TODO: Implement capacity check.

        Returns:
            True if at or over capacity
        """
        # TODO: Implement
        pass


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test concurrency limits."""
    print("Concurrency Limits - Exercise 14.3")
    print("=" * 50)

    config = ConcurrencyConfig()
    monitor = ConcurrencyMonitor()

    # Test 1: System concurrency
    print("\n1. System Concurrency:")
    # TODO: Test get_system_concurrency

    # Test 2: DAG concurrency
    print("\n2. DAG Concurrency:")
    # TODO: Test get_dag_concurrency

    # Test 3: Running instances
    print("\n3. Running Instances:")
    # TODO: Test get_running_dags and get_running_tasks

    # Test 4: Capacity check
    print("\n4. Capacity Check:")
    # TODO: Test check_capacity

    print("\n" + "=" * 50)
    print("Test complete!")


if __name__ == "__main__":
    main()
