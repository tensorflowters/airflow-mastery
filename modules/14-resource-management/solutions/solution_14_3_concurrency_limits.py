"""
Solution 14.3: Concurrency Limits
==================================

Complete implementation of concurrency controls
at DAG and task levels.
"""

import logging
from typing import Optional
from datetime import datetime
from collections import defaultdict

from airflow.sdk import dag, task
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.session import create_session
from airflow.configuration import conf

logger = logging.getLogger(__name__)


# =============================================================================
# PART 1: CONCURRENCY CONFIGURATION
# =============================================================================


class ConcurrencyConfig:
    """
    Manage concurrency settings for DAGs and tasks.

    Concurrency Hierarchy:
    1. System-wide parallelism (core.parallelism)
    2. DAG max_active_runs (per DAG)
    3. DAG max_active_tasks (per DAG)
    4. Task max_active_tis_per_dag (per task type)
    5. Task max_active_tis_per_dagrun (per task per run)
    """

    DEFAULTS = {
        "parallelism": 32,
        "max_active_runs_per_dag": 16,
        "max_active_tasks_per_dag": 16,
    }

    def get_dag_concurrency(self, dag_id: str) -> Optional[dict]:
        """
        Get DAG concurrency settings.

        Args:
            dag_id: DAG identifier

        Returns:
            Dict with concurrency settings
        """
        try:
            with create_session() as session:
                dag_model = session.query(DagModel).filter(
                    DagModel.dag_id == dag_id
                ).first()

                if not dag_model:
                    return None

                return {
                    "dag_id": dag_id,
                    "max_active_runs": dag_model.max_active_runs,
                    "max_active_tasks": dag_model.max_active_tasks,
                    "is_paused": dag_model.is_paused,
                }
        except Exception as e:
            logger.error(f"Failed to get DAG concurrency: {e}")
            return None

    def set_dag_concurrency(
        self,
        dag_id: str,
        max_active_runs: int = None,
        max_active_tasks: int = None,
    ) -> bool:
        """
        Update DAG concurrency settings.

        Args:
            dag_id: DAG identifier
            max_active_runs: Max concurrent DAG runs
            max_active_tasks: Max concurrent tasks

        Returns:
            True if successful
        """
        try:
            with create_session() as session:
                dag_model = session.query(DagModel).filter(
                    DagModel.dag_id == dag_id
                ).first()

                if not dag_model:
                    logger.warning(f"DAG not found: {dag_id}")
                    return False

                if max_active_runs is not None:
                    dag_model.max_active_runs = max_active_runs
                if max_active_tasks is not None:
                    dag_model.max_active_tasks = max_active_tasks

                session.commit()
                logger.info(f"Updated concurrency for {dag_id}")
                return True

        except Exception as e:
            logger.error(f"Failed to set DAG concurrency: {e}")
            return False

    def get_system_concurrency(self) -> dict:
        """
        Get global concurrency settings from Airflow configuration.

        Returns:
            Dict with system-wide settings
        """
        try:
            return {
                "parallelism": conf.getint("core", "parallelism", fallback=32),
                "max_active_runs_per_dag": conf.getint(
                    "core", "max_active_runs_per_dag", fallback=16
                ),
                "max_active_tasks_per_dag": conf.getint(
                    "core", "max_active_tasks_per_dag", fallback=16
                ),
                "dag_concurrency": conf.getint(
                    "core", "dag_concurrency", fallback=16
                ),
            }
        except Exception as e:
            logger.error(f"Failed to get system concurrency: {e}")
            return self.DEFAULTS.copy()

    def validate_settings(self, settings: dict) -> tuple:
        """
        Validate concurrency settings.

        Args:
            settings: Dict of settings to validate

        Returns:
            (is_valid, list of errors)
        """
        errors = []

        if "max_active_runs" in settings:
            if settings["max_active_runs"] < 1:
                errors.append("max_active_runs must be >= 1")
            elif settings["max_active_runs"] > 100:
                errors.append("max_active_runs should not exceed 100")

        if "max_active_tasks" in settings:
            if settings["max_active_tasks"] < 1:
                errors.append("max_active_tasks must be >= 1")

        if "max_active_tis_per_dag" in settings:
            if settings["max_active_tis_per_dag"] < 1:
                errors.append("max_active_tis_per_dag must be >= 1")

        if "max_active_tis_per_dagrun" in settings:
            if settings["max_active_tis_per_dagrun"] < 1:
                errors.append("max_active_tis_per_dagrun must be >= 1")

        return (len(errors) == 0, errors)


# =============================================================================
# PART 2: CONCURRENCY-CONTROLLED DAG
# =============================================================================


@dag(
    dag_id="solution_controlled_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",  # Every 15 minutes
    catchup=False,
    max_active_runs=3,        # Only 3 DAG runs at once
    max_active_tasks=10,      # Max 10 tasks running per DAG run
    tags=["exercise", "concurrency", "module-14"],
)
def controlled_pipeline():
    """
    Pipeline with comprehensive concurrency controls.

    Demonstrates:
    - DAG-level max_active_runs
    - DAG-level max_active_tasks
    - Task-level max_active_tis_per_dag
    - Task-level max_active_tis_per_dagrun
    """

    @task(max_active_tis_per_dag=10)
    def light_processing(batch_id: int):
        """
        Light task - many can run in parallel.

        max_active_tis_per_dag=10 means up to 10 instances
        of this task can run across all DAG runs.
        """
        logger.info(f"Processing light batch {batch_id}")
        return {"batch_id": batch_id, "status": "processed"}

    @task(max_active_tis_per_dag=5, max_active_tis_per_dagrun=2)
    def medium_processing(batch_result: dict):
        """
        Medium task - limited parallelism.

        max_active_tis_per_dag=5: Max 5 across all runs
        max_active_tis_per_dagrun=2: Max 2 per individual run
        """
        logger.info(f"Medium processing for {batch_result['batch_id']}")
        return {"result": "transformed", **batch_result}

    @task(max_active_tis_per_dag=2, max_active_tis_per_dagrun=1)
    def heavy_processing(batch_results: list):
        """
        Heavy task - very limited parallelism.

        Only 2 across all runs, and only 1 per run.
        This ensures heavy tasks don't overwhelm the system.
        """
        logger.info(f"Heavy processing {len(batch_results)} batches")
        return {
            "total_processed": len(batch_results),
            "status": "complete",
        }

    @task(max_active_tis_per_dag=1)
    def exclusive_finalization(result: dict):
        """
        Exclusive task - only one instance system-wide.

        Use for tasks that must have exclusive access
        to a resource (e.g., database schema changes).
        """
        logger.info(f"Finalizing: {result}")
        return {"finalized": True}

    # Create batch processing structure
    # Process 5 batches with varying concurrency
    batch_ids = list(range(5))

    # Light processing - can run many in parallel
    light_results = [light_processing(i) for i in batch_ids]

    # Medium processing - limited parallelism
    medium_results = [medium_processing(result) for result in light_results]

    # Heavy processing - very limited
    heavy_result = heavy_processing(medium_results)

    # Exclusive finalization
    exclusive_finalization(heavy_result)


# =============================================================================
# PART 3: CONCURRENCY MONITOR
# =============================================================================


class ConcurrencyMonitor:
    """
    Monitor concurrency usage across the Airflow system.

    Provides real-time visibility into:
    - Running DAG runs
    - Running task instances
    - Available capacity
    """

    def __init__(self):
        self.config = ConcurrencyConfig()

    def get_running_dags(self, dag_id: str = None) -> dict:
        """
        Get currently running DAG runs.

        Args:
            dag_id: Optional filter by DAG

        Returns:
            Dict with running DAG run information
        """
        try:
            with create_session() as session:
                query = session.query(DagRun).filter(
                    DagRun.state == DagRunState.RUNNING
                )

                if dag_id:
                    query = query.filter(DagRun.dag_id == dag_id)

                runs = query.all()

                # Group by DAG
                by_dag = defaultdict(list)
                for run in runs:
                    by_dag[run.dag_id].append({
                        "run_id": run.run_id,
                        "start_date": run.start_date.isoformat() if run.start_date else None,
                        "execution_date": run.execution_date.isoformat() if run.execution_date else None,
                    })

                return {
                    "total_running": len(runs),
                    "by_dag": dict(by_dag),
                }

        except Exception as e:
            logger.error(f"Failed to get running DAGs: {e}")
            return {"total_running": 0, "by_dag": {}}

    def get_running_tasks(self, dag_id: str = None) -> dict:
        """
        Get currently running task instances.

        Args:
            dag_id: Optional filter by DAG

        Returns:
            Dict with running task information
        """
        try:
            with create_session() as session:
                query = session.query(TaskInstance).filter(
                    TaskInstance.state == TaskInstanceState.RUNNING
                )

                if dag_id:
                    query = query.filter(TaskInstance.dag_id == dag_id)

                tasks = query.all()

                # Group by DAG and task
                by_dag = defaultdict(lambda: defaultdict(int))
                for ti in tasks:
                    by_dag[ti.dag_id][ti.task_id] += 1

                return {
                    "total_running": len(tasks),
                    "by_dag": {
                        dag: dict(tasks) for dag, tasks in by_dag.items()
                    },
                }

        except Exception as e:
            logger.error(f"Failed to get running tasks: {e}")
            return {"total_running": 0, "by_dag": {}}

    def check_capacity(self, dag_id: str = None) -> dict:
        """
        Check available capacity.

        Args:
            dag_id: Optional DAG to check

        Returns:
            Dict with capacity information
        """
        system = self.config.get_system_concurrency()
        running_dags = self.get_running_dags(dag_id)
        running_tasks = self.get_running_tasks(dag_id)

        result = {
            "system": {
                "parallelism": system["parallelism"],
                "running_tasks": running_tasks["total_running"],
                "available": system["parallelism"] - running_tasks["total_running"],
                "utilization_pct": (
                    running_tasks["total_running"] / system["parallelism"] * 100
                ) if system["parallelism"] > 0 else 0,
            },
        }

        if dag_id:
            dag_config = self.config.get_dag_concurrency(dag_id)
            if dag_config:
                dag_running_runs = len(running_dags["by_dag"].get(dag_id, []))
                dag_running_tasks = sum(
                    running_tasks["by_dag"].get(dag_id, {}).values()
                )

                result["dag"] = {
                    "dag_id": dag_id,
                    "max_active_runs": dag_config["max_active_runs"],
                    "running_runs": dag_running_runs,
                    "available_runs": dag_config["max_active_runs"] - dag_running_runs,
                    "max_active_tasks": dag_config["max_active_tasks"],
                    "running_tasks": dag_running_tasks,
                    "available_tasks": dag_config["max_active_tasks"] - dag_running_tasks,
                }

        return result

    def get_concurrency_report(self) -> str:
        """
        Generate a formatted concurrency report.

        Returns:
            Formatted report string
        """
        system = self.config.get_system_concurrency()
        running_dags = self.get_running_dags()
        running_tasks = self.get_running_tasks()

        lines = [
            "=" * 60,
            "CONCURRENCY STATUS REPORT",
            f"Generated: {datetime.utcnow().isoformat()}",
            "=" * 60,
            "",
            "SYSTEM CAPACITY",
            "-" * 40,
            f"  Parallelism: {system['parallelism']}",
            f"  Running Tasks: {running_tasks['total_running']}",
            f"  Available: {system['parallelism'] - running_tasks['total_running']}",
            f"  Utilization: {running_tasks['total_running'] / system['parallelism'] * 100:.1f}%",
            "",
            "RUNNING DAG RUNS",
            "-" * 40,
            f"  Total: {running_dags['total_running']}",
        ]

        for dag_id, runs in running_dags["by_dag"].items():
            lines.append(f"  {dag_id}: {len(runs)} runs")

        lines.extend([
            "",
            "RUNNING TASKS BY DAG",
            "-" * 40,
        ])

        for dag_id, tasks in running_tasks["by_dag"].items():
            total = sum(tasks.values())
            lines.append(f"  {dag_id}: {total} tasks")
            for task_id, count in tasks.items():
                lines.append(f"    - {task_id}: {count}")

        lines.append("=" * 60)
        return "\n".join(lines)

    def is_at_capacity(self, dag_id: str = None) -> bool:
        """
        Check if system/DAG is at capacity.

        Args:
            dag_id: Optional DAG to check

        Returns:
            True if at or over capacity
        """
        capacity = self.check_capacity(dag_id)

        # Check system capacity
        if capacity["system"]["available"] <= 0:
            return True

        # Check DAG capacity if specified
        if dag_id and "dag" in capacity:
            if capacity["dag"]["available_runs"] <= 0:
                return True
            if capacity["dag"]["available_tasks"] <= 0:
                return True

        return False


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test concurrency limits."""
    print("Concurrency Limits - Solution 14.3")
    print("=" * 60)

    config = ConcurrencyConfig()
    monitor = ConcurrencyMonitor()

    # Test 1: System concurrency
    print("\n1. SYSTEM CONCURRENCY")
    print("-" * 40)

    system = config.get_system_concurrency()
    for key, value in system.items():
        print(f"   {key}: {value}")

    # Test 2: Settings validation
    print("\n2. SETTINGS VALIDATION")
    print("-" * 40)

    test_settings = [
        {"max_active_runs": 5, "max_active_tasks": 10},  # Valid
        {"max_active_runs": 0},  # Invalid
        {"max_active_runs": 150},  # Warning
    ]

    for settings in test_settings:
        valid, errors = config.validate_settings(settings)
        status = "VALID" if valid else "INVALID"
        print(f"   {settings}: {status}")
        for error in errors:
            print(f"      - {error}")

    # Test 3: Concurrency concepts
    print("\n3. CONCURRENCY LEVELS EXPLAINED")
    print("-" * 40)
    print("   System Level:")
    print("     - parallelism: Max tasks across entire system")
    print("")
    print("   DAG Level:")
    print("     - max_active_runs: Max concurrent runs of this DAG")
    print("     - max_active_tasks: Max tasks per DAG run")
    print("")
    print("   Task Level:")
    print("     - max_active_tis_per_dag: Max across all runs")
    print("     - max_active_tis_per_dagrun: Max per single run")

    # Test 4: Sample report
    print("\n4. SAMPLE CONCURRENCY REPORT")
    print("-" * 40)
    print("   (Requires Airflow database connection)")
    print("   In production, call: monitor.get_concurrency_report()")

    print("\n" + "=" * 60)
    print("Test complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()


# Export the DAG
controlled_pipeline_dag = controlled_pipeline()
