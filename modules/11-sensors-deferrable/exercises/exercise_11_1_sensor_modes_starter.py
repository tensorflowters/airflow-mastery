"""
Exercise 11.1: Sensor Modes (Starter)
======================================

Compare poke vs reschedule sensor modes.

Learning Goals:
1. Understand worker slot usage in different modes
2. Configure appropriate poke intervals
3. Handle timeouts gracefully
4. Use multiple sensor types

TODO: Complete the TODOs below to implement the sensors.
"""

from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.date_time import DateTimeSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Configuration
BASE_PATH = "/tmp/airflow_sensor_exercise"


# =============================================================================
# CUSTOM CALLBACKS FOR MONITORING
# =============================================================================


def on_poke_callback(context):
    """
    Called on each poke attempt.

    This helps visualize how often sensors check their condition.
    """
    task_id = context["task_instance"].task_id
    try_number = context["task_instance"].try_number
    logger.info(f"[POKE] Task {task_id} - Attempt #{try_number}")


def on_success_callback(context):
    """Called when sensor condition is met."""
    task_id = context["task_instance"].task_id
    duration = context["task_instance"].duration
    logger.info(f"[SUCCESS] Task {task_id} completed after {duration:.1f}s")


def on_timeout_callback(context):
    """Called when sensor times out."""
    task_id = context["task_instance"].task_id
    logger.warning(f"[TIMEOUT] Task {task_id} timed out waiting for condition")


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="exercise_11_1_sensor_modes",
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["exercise", "sensors"],
    doc_md="""
    ## Sensor Modes Exercise

    This DAG demonstrates the difference between poke and reschedule sensor modes.

    ### File Structure
    Create test file at: `/tmp/airflow_sensor_exercise/input/data_{{ ds }}.csv`

    ### Expected Behavior
    - Poke mode: Worker slot held continuously
    - Reschedule mode: Worker slot released between checks
    """,
)
def sensor_modes_exercise():
    """DAG demonstrating sensor mode differences."""

    # =========================================================================
    # PART 1: POKE MODE SENSOR
    # =========================================================================

    # TODO: Create a FileSensor with poke mode
    # - mode="poke"
    # - poke_interval=10 (check every 10 seconds)
    # - timeout=300 (5 minutes)
    # - Use templated filepath: f"{BASE_PATH}/input/data_{{{{ ds }}}}.csv"

    wait_poke_mode = FileSensor(
        task_id="wait_poke_mode",
        # TODO: Configure the sensor
        filepath=f"{BASE_PATH}/input/data_{{{{ ds }}}}.csv",
        # mode=...,
        # poke_interval=...,
        # timeout=...,
    )

    # =========================================================================
    # PART 2: RESCHEDULE MODE SENSOR
    # =========================================================================

    # TODO: Create a FileSensor with reschedule mode
    # - mode="reschedule"
    # - poke_interval=30 (check every 30 seconds)
    # - timeout=3600 (1 hour)
    # - soft_fail=True (skip instead of fail on timeout)

    wait_reschedule_mode = FileSensor(
        task_id="wait_reschedule_mode",
        filepath=f"{BASE_PATH}/input/data_{{{{ ds }}}}.csv",
        # TODO: Configure reschedule mode
        # mode=...,
        # poke_interval=...,
        # timeout=...,
        # soft_fail=...,
    )

    # =========================================================================
    # PART 3: ADDITIONAL SENSOR TYPES
    # =========================================================================

    # TODO: Create a DateTimeSensor
    # Wait until 1 minute after the task starts

    # wait_until_time = DateTimeSensor(
    #     task_id="wait_until_time",
    #     target_time=...,  # Use template: "{{ execution_date.add(minutes=1) }}"
    # )

    # TODO: Create a TimeDeltaSensor
    # Wait for 30 seconds after task starts

    # wait_duration = TimeDeltaSensor(
    #     task_id="wait_duration",
    #     delta=timedelta(seconds=30),
    # )

    # =========================================================================
    # PART 4: PROCESSING TASKS
    # =========================================================================

    @task
    def log_sensor_complete(sensor_name: str) -> dict:
        """Log when a sensor completes."""
        import time

        logger.info(f"Sensor '{sensor_name}' condition was met!")
        return {
            "sensor": sensor_name,
            "completed_at": time.time(),
        }

    @task
    def process_file() -> dict:
        """Process the file after sensors complete."""
        import os

        # In a real scenario, read and process the file
        logger.info("Processing file...")

        # Simulate processing
        return {
            "status": "processed",
            "records": 100,
        }

    @task
    def generate_report(poke_result: dict, reschedule_result: dict, process_result: dict) -> None:
        """Generate final report comparing sensor behaviors."""
        logger.info("=" * 50)
        logger.info("SENSOR MODE COMPARISON REPORT")
        logger.info("=" * 50)
        logger.info(f"Poke mode sensor: {poke_result}")
        logger.info(f"Reschedule mode sensor: {reschedule_result}")
        logger.info(f"Processing result: {process_result}")
        logger.info("=" * 50)

    # =========================================================================
    # TASK DEPENDENCIES
    # =========================================================================

    # TODO: Set up the task flow
    # Both sensors should run in parallel, then processing happens

    poke_complete = log_sensor_complete.override(task_id="log_poke_complete")("poke_mode")
    reschedule_complete = log_sensor_complete.override(task_id="log_reschedule_complete")("reschedule_mode")

    wait_poke_mode >> poke_complete
    wait_reschedule_mode >> reschedule_complete

    process_result = process_file()

    [poke_complete, reschedule_complete] >> process_result

    # generate_report(poke_complete, reschedule_complete, process_result)


# Instantiate the DAG
sensor_modes_exercise()


# =============================================================================
# HELPER: CREATE TEST FILE
# =============================================================================


def create_test_file(ds: str = "2024-01-15"):
    """
    Helper function to create test file for the sensor.

    Run this to trigger the sensors:
    python -c "from exercise_11_1_sensor_modes_starter import create_test_file; create_test_file()"
    """
    import os

    filepath = f"{BASE_PATH}/input/data_{ds}.csv"
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        f.write("id,value\n")
        f.write("1,100\n")
        f.write("2,200\n")

    print(f"Created test file: {filepath}")


if __name__ == "__main__":
    create_test_file()
