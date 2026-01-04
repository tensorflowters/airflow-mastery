"""
Solution 11.2: Deferrable Conversion
=====================================

Complete implementation of deferrable sensors showing maximum
resource efficiency with zero worker usage during waits.

Architecture:
    1. Task starts on Worker
    2. Task defers itself, creates Trigger
    3. Worker slot released (0% usage)
    4. Triggerer process monitors condition
    5. Condition met → Trigger fires event
    6. Task resumes on Worker

Requirements:
    - Triggerer must be running: `airflow triggerer`
"""

from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.date_time import DateTimeSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

BASE_PATH = "/tmp/airflow_deferrable_exercise"


# =============================================================================
# CALLBACKS
# =============================================================================


def on_execute_callback(context):
    """Log when task starts execution."""
    ti = context["task_instance"]
    logger.info(f"[EXECUTE] Task '{ti.task_id}' started on worker")


def on_success_callback(context):
    """Log successful completion with timing."""
    ti = context["task_instance"]
    duration = ti.duration or 0
    logger.info(
        f"[SUCCESS] Task '{ti.task_id}' completed | Duration: {duration:.1f}s"
    )


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="solution_11_2_deferrable_conversion",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "deferrable", "sensors"],
    doc_md="""
    ## Deferrable Sensors Demo

    This DAG demonstrates deferrable operators that release worker
    slots completely during waits.

    ### Prerequisites
    ```bash
    # Start the triggerer (required for deferrable operators)
    airflow triggerer
    ```

    ### Test Instructions
    ```bash
    # Create test file
    mkdir -p /tmp/airflow_deferrable_exercise/input
    echo "id,value\\n1,100" > /tmp/airflow_deferrable_exercise/input/data_2024-01-15.csv

    # Trigger DAG
    airflow dags trigger solution_11_2_deferrable_conversion
    ```

    ### Monitoring
    ```bash
    # List active triggers
    airflow triggers list

    # Watch triggerer logs
    tail -f $AIRFLOW_HOME/logs/triggerer/*.log
    ```

    ### Resource Comparison

    | Mode | Worker During Wait | Scheduler Load |
    |------|-------------------|----------------|
    | poke | 100% | Low |
    | reschedule | ~5% | Medium |
    | **deferrable** | **0%** | **Very Low** |
    """,
    default_args={
        "owner": "airflow",
        "on_execute_callback": on_execute_callback,
        "on_success_callback": on_success_callback,
    },
)
def deferrable_sensors_demo():
    """DAG demonstrating deferrable sensor patterns."""

    # =========================================================================
    # TRADITIONAL SENSOR (FOR COMPARISON)
    # =========================================================================

    wait_traditional = FileSensor(
        task_id="wait_traditional_reschedule",
        filepath=f"{BASE_PATH}/input/data_{{{{ ds }}}}.csv",
        mode="reschedule",
        poke_interval=30,
        timeout=300,
        soft_fail=True,
    )

    # =========================================================================
    # DEFERRABLE FILE SENSOR
    # =========================================================================
    #
    # The deferrable=True flag enables async waiting:
    # - Task defers immediately after checking condition
    # - FileTrigger monitors file existence asynchronously
    # - Worker slot is 100% released during wait
    # - Task resumes when file appears

    wait_deferrable_file = FileSensor(
        task_id="wait_deferrable_file",
        filepath=f"{BASE_PATH}/input/data_{{{{ ds }}}}.csv",
        deferrable=True,           # Enable async mode
        poke_interval=10,          # Trigger check interval
        timeout=300,               # Max wait time
    )

    # =========================================================================
    # DEFERRABLE DATETIME SENSOR
    # =========================================================================
    #
    # Waits until a specific time without holding worker.
    # Perfect for scheduling tasks to run at exact times.

    wait_until_time = DateTimeSensor(
        task_id="wait_until_time_deferrable",
        target_time="{{ execution_date.add(seconds=20) }}",
        deferrable=True,
        poke_interval=5,
    )

    # =========================================================================
    # DEFERRABLE TIMEDELTA SENSOR
    # =========================================================================
    #
    # Waits for a duration without holding worker.
    # Useful for cooldown periods or rate limiting.

    wait_duration = TimeDeltaSensor(
        task_id="wait_duration_deferrable",
        delta=timedelta(seconds=15),
        deferrable=True,
    )

    # =========================================================================
    # VERIFICATION AND PROCESSING TASKS
    # =========================================================================

    @task
    def log_traditional_complete() -> dict:
        """Log when traditional sensor completes."""
        result = {
            "sensor_type": "traditional_reschedule",
            "worker_usage": "~5% (periodic scheduling)",
            "completed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"Traditional sensor completed: {result}")
        return result

    @task
    def log_deferrable_file_complete() -> dict:
        """Log when deferrable file sensor completes."""
        result = {
            "sensor_type": "deferrable_file",
            "worker_usage": "0% during defer",
            "async_component": "FileTrigger",
            "completed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"Deferrable file sensor completed: {result}")
        return result

    @task
    def log_time_sensors_complete() -> dict:
        """Log when time-based deferrable sensors complete."""
        result = {
            "sensor_types": ["datetime_deferrable", "timedelta_deferrable"],
            "worker_usage": "0% during defer",
            "completed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"Time sensors completed: {result}")
        return result

    @task
    def process_data() -> dict:
        """Process data after all sensors complete."""
        import os

        filepath = f"{BASE_PATH}/input/data_2024-01-15.csv"

        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                content = f.read()
            status = "processed"
        else:
            content = ""
            status = "no_file"

        result = {
            "status": status,
            "file_content_length": len(content),
            "processed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"Data processed: {result}")
        return result

    @task
    def generate_efficiency_report(
        traditional: dict,
        deferrable_file: dict,
        time_sensors: dict,
        process_result: dict,
    ) -> None:
        """Generate report comparing sensor efficiency."""
        report = f"""
{'=' * 70}
DEFERRABLE SENSORS EFFICIENCY REPORT
{'=' * 70}

SENSORS COMPLETED:
------------------
1. Traditional (reschedule mode):
   - Worker usage during wait: {traditional['worker_usage']}
   - Completed: {traditional['completed_at']}

2. Deferrable File Sensor:
   - Worker usage during wait: {deferrable_file['worker_usage']}
   - Async component: {deferrable_file['async_component']}
   - Completed: {deferrable_file['completed_at']}

3. Deferrable Time Sensors:
   - Types: {', '.join(time_sensors['sensor_types'])}
   - Worker usage during wait: {time_sensors['worker_usage']}
   - Completed: {time_sensors['completed_at']}

PROCESSING RESULT:
------------------
- Status: {process_result['status']}
- Content length: {process_result['file_content_length']} bytes

EFFICIENCY COMPARISON:
----------------------
┌─────────────────┬──────────────────┬─────────────────┬─────────────────┐
│ Mode            │ Worker Usage     │ Scheduler Load  │ Best For        │
├─────────────────┼──────────────────┼─────────────────┼─────────────────┤
│ poke            │ 100%             │ Low             │ < 5 min waits   │
│ reschedule      │ ~5%              │ Medium          │ 5-60 min waits  │
│ deferrable      │ 0%               │ Very Low        │ > 60 min, scale │
└─────────────────┴──────────────────┴─────────────────┴─────────────────┘

KEY BENEFITS OF DEFERRABLE:
---------------------------
✓ Zero worker slot usage during wait
✓ Minimal scheduler overhead
✓ Scales to thousands of waiting tasks
✓ No worker pool exhaustion risk
✓ Efficient use of triggerer (lightweight async process)

REQUIREMENTS:
-------------
- Triggerer process must be running
- Sensor must support deferrable mode
- Async dependencies installed (usually included)

{'=' * 70}
"""
        logger.info(report)
        print(report)

    # =========================================================================
    # TASK FLOW
    # =========================================================================

    # Traditional and deferrable file sensors run in parallel
    trad_complete = log_traditional_complete()
    defer_file_complete = log_deferrable_file_complete()
    time_complete = log_time_sensors_complete()

    wait_traditional >> trad_complete
    wait_deferrable_file >> defer_file_complete
    [wait_until_time, wait_duration] >> time_complete

    # Process after file sensors complete
    process_result = process_data()
    [trad_complete, defer_file_complete] >> process_result

    # Generate report after everything
    generate_efficiency_report(
        trad_complete,
        defer_file_complete,
        time_complete,
        process_result,
    )


# Instantiate
deferrable_sensors_demo()


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def create_test_file(ds: str = "2024-01-15"):
    """Create test file for sensors."""
    import os

    filepath = f"{BASE_PATH}/input/data_{ds}.csv"
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w") as f:
        f.write("id,name,value\n")
        for i in range(1, 6):
            f.write(f"{i},item_{i},{i * 100}\n")

    print(f"Created test file: {filepath}")


def cleanup():
    """Remove test files."""
    import shutil

    if os.path.exists(BASE_PATH):
        shutil.rmtree(BASE_PATH)
        print(f"Cleaned up: {BASE_PATH}")


if __name__ == "__main__":
    create_test_file()
