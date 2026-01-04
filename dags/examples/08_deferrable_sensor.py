"""
Example DAG: Deferrable Operators and Async Sensors

Demonstrates deferrable operators that free up worker slots
while waiting for external conditions. Uses async execution
with triggers for efficient resource usage.

Module: 11-sensors-deferrable
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.sensors.time_sensor import TimeSensorAsync
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator


# =============================================================================
# DAG 1: Async Time Sensor
# =============================================================================

with DAG(
    dag_id="08_deferrable_time_sensor",
    description="Demonstrates async/deferrable time sensor",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-11", "deferrable", "async"],
):
    start = EmptyOperator(task_id="start")

    # Deferrable time sensor - releases worker immediately
    # Uses triggerer process to monitor time
    wait_async = TimeSensorAsync(
        task_id="wait_until_time_async",
        target_time=(datetime.now() + timedelta(minutes=2)).time(),
        # No mode parameter needed - always deferred
    )

    @task
    def after_wait():
        """Execute after the async wait completes."""
        print("Async wait complete!")
        return {"status": "complete", "timestamp": datetime.now().isoformat()}

    start >> wait_async >> after_wait()


# =============================================================================
# DAG 2: Comparing Sync vs Async Sensors
# =============================================================================

with DAG(
    dag_id="08_sync_vs_async_comparison",
    description="Compares sync and async sensor behavior",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-11", "deferrable", "comparison"],
):
    start = EmptyOperator(task_id="start")

    # =========================================================================
    # Traditional Sensor (holds worker slot)
    # =========================================================================

    # Poke mode: Holds worker slot, checks every 30 seconds
    sync_sensor_poke = FileSensor(
        task_id="sync_sensor_poke",
        filepath="/tmp/sync_test_1.txt",
        poke_interval=30,
        timeout=300,
        mode="poke",  # Worker slot held entire time
        soft_fail=True,
    )

    # Reschedule mode: Releases between checks, but still uses workers
    sync_sensor_reschedule = FileSensor(
        task_id="sync_sensor_reschedule",
        filepath="/tmp/sync_test_2.txt",
        poke_interval=60,
        timeout=300,
        mode="reschedule",  # Worker released between checks
        soft_fail=True,
    )

    # =========================================================================
    # Resource Impact Comparison
    # =========================================================================

    @task
    def explain_differences():
        """Explain the resource usage differences."""
        comparison = """
        Sensor Mode Comparison:

        1. POKE MODE (mode='poke'):
           - Holds worker slot continuously
           - Fast response time (checks every poke_interval)
           - Use for short waits (< 5 minutes)
           - Resource cost: HIGH

        2. RESCHEDULE MODE (mode='reschedule'):
           - Releases worker between checks
           - Task shows as 'up_for_reschedule' state
           - Use for medium waits (5 min - 1 hour)
           - Resource cost: MEDIUM

        3. DEFERRABLE (Async):
           - Uses triggerer instead of worker
           - Task shows as 'deferred' state
           - Use for long waits (> 1 hour)
           - Resource cost: LOW
           - Requires triggerer component running
        """
        print(comparison)
        return {"explained": True}

    # Dependencies
    start >> [sync_sensor_poke, sync_sensor_reschedule]
    [sync_sensor_poke, sync_sensor_reschedule] >> explain_differences()


# =============================================================================
# DAG 3: Deferrable Operator Pattern
# =============================================================================

with DAG(
    dag_id="08_deferrable_pattern",
    description="Shows the deferrable operator pattern",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-11", "deferrable", "pattern"],
):
    @task
    def setup_work():
        """Setup before deferrable wait."""
        print("Setting up work before async wait...")
        return {"setup": "complete"}

    # Async sensor defers to triggerer
    async_wait = TimeSensorAsync(
        task_id="async_wait",
        target_time=(datetime.now() + timedelta(minutes=1)).time(),
    )

    @task
    def resume_after_defer():
        """
        This task runs after the deferred operator completes.

        The execution flow:
        1. setup_work runs (uses worker)
        2. async_wait starts and immediately defers
        3. Worker is released - can run other tasks
        4. Triggerer monitors the condition
        5. When condition met, task is rescheduled
        6. resume_after_defer runs (uses worker)
        """
        print("Resuming work after deferral!")
        return {"status": "resumed"}

    @task
    def final_processing(result: dict):
        """Final processing after all waits complete."""
        print(f"Final processing with result: {result}")
        return {"final": "complete"}

    setup = setup_work()
    setup >> async_wait >> final_processing(resume_after_defer())


# =============================================================================
# DAG 4: When to Use Deferrable Operators
# =============================================================================

with DAG(
    dag_id="08_when_to_use_deferrable",
    description="Guidelines for when to use deferrable operators",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-11", "deferrable", "guidelines"],
):
    @task
    def show_guidelines():
        """Display guidelines for deferrable operator usage."""
        guidelines = """
        When to Use Deferrable Operators:

        ✅ USE DEFERRABLE WHEN:
        - Waiting for external APIs or services
        - Long polling operations (> 1 hour)
        - High volume of concurrent waiting tasks
        - Worker pool is a bottleneck
        - Waiting for time-based conditions

        ❌ AVOID DEFERRABLE WHEN:
        - Very short waits (< 1 minute)
        - Triggerer is not configured
        - Custom sensors not yet converted
        - Debugging/development (harder to trace)

        🔧 REQUIREMENTS:
        - Triggerer component must be running
        - Operator must support deferrable mode
        - Network access from triggerer to target

        📊 RESOURCE IMPACT:
        - Sync (poke): 1 worker per waiting task
        - Sync (reschedule): ~0.1 worker per task
        - Async (deferrable): ~0.01 worker per task
        """
        print(guidelines)
        return {"guidelines": "displayed"}

    @task
    def list_deferrable_operators():
        """List commonly available deferrable operators."""
        operators = {
            "TimeSensorAsync": "Wait until specific time",
            "DateTimeSensorAsync": "Wait until datetime",
            "HttpSensorAsync": "Wait for HTTP endpoint",
            "S3KeySensorAsync": "Wait for S3 key",
            "GCSObjectExistenceSensorAsync": "Wait for GCS object",
            "BigQueryInsertJobOperator": "BigQuery with deferral",
            "EmrContainerOperator": "EMR with deferral",
        }

        print("Commonly Available Deferrable Operators:")
        for op, desc in operators.items():
            print(f"  - {op}: {desc}")

        return operators

    show_guidelines() >> list_deferrable_operators()
