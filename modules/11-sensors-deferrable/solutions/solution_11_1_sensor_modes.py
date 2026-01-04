"""
Solution 11.1: Sensor Modes
============================

Complete implementation comparing poke vs reschedule sensor modes.

Key Concepts Demonstrated:
1. Poke mode: Worker slot held continuously during wait
2. Reschedule mode: Worker released between checks
3. Multiple sensor types (File, DateTime, TimeDelta)
4. Soft fail and timeout handling
5. Callbacks for monitoring sensor behavior
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
# CALLBACKS FOR MONITORING
# =============================================================================


def on_execute_callback(context):
    """Called when sensor task starts executing."""
    task_id = context["task_instance"].task_id
    mode = context["task"].mode if hasattr(context["task"], "mode") else "N/A"
    logger.info(f"[START] Sensor '{task_id}' starting in {mode} mode")


def on_success_callback(context):
    """Called when sensor condition is met."""
    ti = context["task_instance"]
    duration = ti.duration or 0

    logger.info(
        f"[SUCCESS] Sensor '{ti.task_id}' completed | "
        f"Duration: {duration:.1f}s | "
        f"State: {ti.state}"
    )


def on_failure_callback(context):
    """Called when sensor fails (timeout or error)."""
    ti = context["task_instance"]
    exception = context.get("exception", "Unknown")

    logger.error(
        f"[FAILURE] Sensor '{ti.task_id}' failed | "
        f"Error: {exception}"
    )


def on_retry_callback(context):
    """Called when sensor task is retried."""
    ti = context["task_instance"]
    logger.warning(
        f"[RETRY] Sensor '{ti.task_id}' retrying | "
        f"Try #{ti.try_number}"
    )


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="solution_11_1_sensor_modes",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "sensors", "modes"],
    doc_md="""
    ## Sensor Modes Demonstration

    This DAG demonstrates the differences between sensor operating modes.

    ### Modes Compared

    | Mode | Worker Usage | Best For |
    |------|--------------|----------|
    | poke | 100% during wait | Short waits (<5 min) |
    | reschedule | ~0% between checks | Long waits (>5 min) |

    ### Test Instructions

    1. Trigger the DAG manually
    2. Create test file: `mkdir -p /tmp/airflow_sensor_exercise/input && echo "data" > /tmp/airflow_sensor_exercise/input/data_2024-01-15.csv`
    3. Observe sensor behavior in logs

    ### File Location
    `/tmp/airflow_sensor_exercise/input/data_{{ ds }}.csv`
    """,
    default_args={
        "owner": "airflow",
        "retries": 0,
        "on_execute_callback": on_execute_callback,
        "on_success_callback": on_success_callback,
        "on_failure_callback": on_failure_callback,
        "on_retry_callback": on_retry_callback,
    },
)
def sensor_modes_demo():
    """DAG demonstrating comprehensive sensor mode comparison."""

    # =========================================================================
    # POKE MODE SENSOR
    # =========================================================================
    #
    # Poke mode keeps the worker slot occupied throughout the wait.
    # The task sleeps between poke attempts but doesn't release resources.
    #
    # Best for: Short waits where quick response is needed

    wait_poke_mode = FileSensor(
        task_id="wait_poke_mode",
        filepath=f"{BASE_PATH}/input/data_{{{{ ds }}}}.csv",
        mode="poke",
        poke_interval=10,        # Check every 10 seconds
        timeout=300,             # 5 minute timeout
        soft_fail=False,         # Fail task on timeout
        exponential_backoff=False,
    )

    # =========================================================================
    # RESCHEDULE MODE SENSOR
    # =========================================================================
    #
    # Reschedule mode releases the worker slot between checks.
    # The task is rescheduled by the scheduler for each poke.
    #
    # Best for: Long waits where worker efficiency matters

    wait_reschedule_mode = FileSensor(
        task_id="wait_reschedule_mode",
        filepath=f"{BASE_PATH}/input/data_{{{{ ds }}}}.csv",
        mode="reschedule",
        poke_interval=30,        # Check every 30 seconds
        timeout=3600,            # 1 hour timeout
        soft_fail=True,          # Skip instead of fail on timeout
        exponential_backoff=True,  # Increase interval on consecutive checks
    )

    # =========================================================================
    # DATETIME SENSOR
    # =========================================================================
    #
    # Waits until a specific point in time.
    # Useful for coordinating with external schedules.

    wait_until_time = DateTimeSensor(
        task_id="wait_until_time",
        target_time="{{ execution_date.add(seconds=30) }}",
        mode="reschedule",
        poke_interval=5,
    )

    # =========================================================================
    # TIME DELTA SENSOR
    # =========================================================================
    #
    # Waits for a specific duration after task starts.
    # Useful for rate limiting or cooldown periods.

    wait_duration = TimeDeltaSensor(
        task_id="wait_duration",
        delta=timedelta(seconds=15),
        mode="poke",  # Short wait, poke is fine
    )

    # =========================================================================
    # PROCESSING TASKS
    # =========================================================================

    @task
    def log_poke_sensor_complete() -> dict:
        """Log poke mode sensor completion."""
        import time

        result = {
            "sensor_type": "poke_mode",
            "mode": "poke",
            "description": "Worker held continuously",
            "completed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"Poke mode sensor completed: {result}")
        return result

    @task
    def log_reschedule_sensor_complete() -> dict:
        """Log reschedule mode sensor completion."""
        result = {
            "sensor_type": "reschedule_mode",
            "mode": "reschedule",
            "description": "Worker released between checks",
            "completed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"Reschedule mode sensor completed: {result}")
        return result

    @task
    def log_time_sensors_complete() -> dict:
        """Log time-based sensors completion."""
        result = {
            "sensors": ["datetime", "timedelta"],
            "description": "Time-based waiting complete",
            "completed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"Time sensors completed: {result}")
        return result

    @task
    def process_file() -> dict:
        """Process the input file after all sensors complete."""
        import os

        filepath = f"{BASE_PATH}/input/data_2024-01-15.csv"

        # Check if file exists
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                lines = f.readlines()
            record_count = len(lines) - 1  # Exclude header
        else:
            record_count = 0

        result = {
            "status": "processed",
            "filepath": filepath,
            "records_found": record_count,
            "processed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"File processed: {result}")
        return result

    @task
    def generate_comparison_report(
        poke_result: dict,
        reschedule_result: dict,
        time_result: dict,
        process_result: dict,
    ) -> None:
        """Generate final comparison report."""
        report = f"""
{'=' * 60}
SENSOR MODES COMPARISON REPORT
{'=' * 60}

FILE SENSORS:
-------------
Poke Mode:
  - Mode: {poke_result['mode']}
  - Behavior: {poke_result['description']}
  - Completed: {poke_result['completed_at']}

Reschedule Mode:
  - Mode: {reschedule_result['mode']}
  - Behavior: {reschedule_result['description']}
  - Completed: {reschedule_result['completed_at']}

TIME SENSORS:
-------------
  - Types: {', '.join(time_result['sensors'])}
  - Completed: {time_result['completed_at']}

PROCESSING:
-----------
  - Status: {process_result['status']}
  - Records: {process_result['records_found']}
  - Processed: {process_result['processed_at']}

KEY DIFFERENCES:
----------------
1. POKE MODE:
   - Worker slot: HELD continuously
   - Scheduler load: LOW (no rescheduling)
   - Best for: Short waits (<5 min)
   - Trade-off: Uses worker resources during wait

2. RESCHEDULE MODE:
   - Worker slot: RELEASED between checks
   - Scheduler load: HIGHER (frequent rescheduling)
   - Best for: Long waits (>5 min)
   - Trade-off: More scheduler overhead

RECOMMENDATIONS:
----------------
- Use POKE for quick checks (seconds to few minutes)
- Use RESCHEDULE for longer waits (minutes to hours)
- Consider DEFERRABLE for very long waits (see Exercise 11.2)

{'=' * 60}
"""
        logger.info(report)
        print(report)

    # =========================================================================
    # TASK FLOW
    # =========================================================================

    # File sensors run in parallel
    poke_complete = log_poke_sensor_complete()
    reschedule_complete = log_reschedule_sensor_complete()
    time_complete = log_time_sensors_complete()

    wait_poke_mode >> poke_complete
    wait_reschedule_mode >> reschedule_complete
    [wait_until_time, wait_duration] >> time_complete

    # Process file after file sensors complete
    process_result = process_file()
    [poke_complete, reschedule_complete] >> process_result

    # Generate report after everything completes
    generate_comparison_report(
        poke_complete,
        reschedule_complete,
        time_complete,
        process_result,
    )


# Instantiate the DAG
sensor_modes_demo()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def create_test_file(ds: str = "2024-01-15"):
    """
    Create test file to trigger sensors.

    Usage:
        python -c "from solution_11_1_sensor_modes import create_test_file; create_test_file()"
    """
    import os

    filepath = f"{BASE_PATH}/input/data_{ds}.csv"
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        f.write("id,name,value\n")
        for i in range(1, 11):
            f.write(f"{i},item_{i},{i * 100}\n")

    print(f"Created test file: {filepath}")
    print(f"Contains 10 data records")


def cleanup_test_files():
    """
    Remove test files and directories.

    Usage:
        python -c "from solution_11_1_sensor_modes import cleanup_test_files; cleanup_test_files()"
    """
    import shutil

    if os.path.exists(BASE_PATH):
        shutil.rmtree(BASE_PATH)
        print(f"Cleaned up: {BASE_PATH}")
    else:
        print(f"Nothing to clean: {BASE_PATH} doesn't exist")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "cleanup":
        cleanup_test_files()
    else:
        create_test_file()
