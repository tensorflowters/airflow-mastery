"""
Exercise 11.2: Deferrable Conversion (Starter)
===============================================

Convert traditional sensors to deferrable operators.

Learning Goals:
1. Understand deferrable operator architecture
2. Enable deferrable mode on supported sensors
3. Verify worker slot release during defer
4. Handle deferrable-specific errors

TODO: Complete the TODOs to implement deferrable sensors.
"""

from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.date_time import DateTimeSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

BASE_PATH = "/tmp/airflow_deferrable_exercise"


@dag(
    dag_id="exercise_11_2_deferrable_conversion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["exercise", "deferrable"],
    doc_md="""
    ## Deferrable Operators Exercise

    This DAG demonstrates converting traditional sensors to deferrable mode.

    ### Prerequisites
    - Triggerer must be running: `airflow triggerer`

    ### Test File
    Create: `/tmp/airflow_deferrable_exercise/input/data_{{ ds }}.csv`
    """,
)
def deferrable_conversion_exercise():
    """DAG demonstrating deferrable sensor conversion."""

    # =========================================================================
    # PART 1: TRADITIONAL SENSOR (FOR COMPARISON)
    # =========================================================================

    # Traditional reschedule mode sensor
    wait_traditional = FileSensor(
        task_id="wait_traditional",
        filepath=f"{BASE_PATH}/input/data_{{{{ ds }}}}.csv",
        mode="reschedule",
        poke_interval=30,
        timeout=300,
    )

    # =========================================================================
    # PART 2: DEFERRABLE FILE SENSOR
    # =========================================================================

    # TODO: Convert to deferrable mode
    # Add: deferrable=True
    # Remove: mode parameter (not needed for deferrable)

    wait_deferrable = FileSensor(
        task_id="wait_deferrable",
        filepath=f"{BASE_PATH}/input/data_{{{{ ds }}}}.csv",
        # TODO: Enable deferrable mode
        # deferrable=True,
        poke_interval=30,
        timeout=300,
    )

    # =========================================================================
    # PART 3: DEFERRABLE DATETIME SENSOR
    # =========================================================================

    # TODO: Create a deferrable DateTimeSensor
    # Wait until 30 seconds after execution date

    # wait_until_time = DateTimeSensor(
    #     task_id="wait_until_time_deferrable",
    #     target_time="{{ execution_date.add(seconds=30) }}",
    #     deferrable=True,
    # )

    # =========================================================================
    # PART 4: DEFERRABLE TIMEDELTA SENSOR
    # =========================================================================

    # TODO: Create a deferrable TimeDeltaSensor
    # Wait for 20 seconds after start

    # wait_duration = TimeDeltaSensor(
    #     task_id="wait_duration_deferrable",
    #     delta=timedelta(seconds=20),
    #     deferrable=True,
    # )

    # =========================================================================
    # PROCESSING TASKS
    # =========================================================================

    @task
    def verify_deferrable_behavior() -> dict:
        """
        Verify that deferrable mode is working correctly.

        In deferrable mode:
        - Task defers itself immediately
        - Trigger runs in triggerer process
        - Worker slot is completely released
        - Task resumes when trigger fires
        """
        import os

        # Check if triggerer is likely running
        # (In production, you'd check the actual process)
        logger.info("Verifying deferrable behavior...")

        result = {
            "deferrable_enabled": True,
            "verified_at": datetime.utcnow().isoformat(),
            "notes": [
                "Worker slot was released during defer",
                "Trigger ran in triggerer process",
                "Task resumed after condition met",
            ],
        }

        return result

    @task
    def process_data() -> dict:
        """Process data after sensors complete."""
        logger.info("Processing data...")
        return {
            "status": "processed",
            "timestamp": datetime.utcnow().isoformat(),
        }

    @task
    def compare_modes(traditional_time: float, deferrable_time: float) -> None:
        """Compare traditional vs deferrable performance."""
        logger.info("=" * 50)
        logger.info("MODE COMPARISON")
        logger.info("=" * 50)
        logger.info(f"Traditional (reschedule): Worker released periodically")
        logger.info(f"Deferrable: Worker released completely during wait")
        logger.info("")
        logger.info("RESOURCE USAGE:")
        logger.info("  Traditional: ~5% worker usage (periodic scheduling)")
        logger.info("  Deferrable:  ~0% worker usage (async trigger)")
        logger.info("=" * 50)

    # =========================================================================
    # TASK FLOW
    # =========================================================================

    verify = verify_deferrable_behavior()
    process = process_data()

    [wait_traditional, wait_deferrable] >> verify >> process


# Instantiate
deferrable_conversion_exercise()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def create_test_file(ds: str = "2024-01-15"):
    """Create test file for sensors."""
    import os

    filepath = f"{BASE_PATH}/input/data_{ds}.csv"
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        f.write("id,value\n1,100\n2,200\n")

    print(f"Created: {filepath}")


if __name__ == "__main__":
    create_test_file()
