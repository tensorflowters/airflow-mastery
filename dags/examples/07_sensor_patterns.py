"""
Example DAG: Sensor Patterns

Demonstrates various sensor types and modes (poke vs reschedule).
Shows file sensors, time sensors, and external task sensors.

Module: 11-sensors-deferrable
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.time_sensor import TimeSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator


# =============================================================================
# DAG 1: File Sensor Patterns
# =============================================================================

with DAG(
    dag_id="07_file_sensor_demo",
    description="Demonstrates FileSensor for waiting on files",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-11", "sensors"],
):
    start = EmptyOperator(task_id="start")

    # Poke Mode: Keeps worker slot while waiting
    # Good for files expected soon (< 5 minutes)
    wait_for_file_poke = FileSensor(
        task_id="wait_for_file_poke_mode",
        filepath="/tmp/airflow_sensor_test.txt",
        poke_interval=30,  # Check every 30 seconds
        timeout=300,       # Timeout after 5 minutes
        mode="poke",       # Hold worker slot
        soft_fail=True,    # Don't fail DAG if timeout
    )

    # Reschedule Mode: Releases worker between checks
    # Good for files that may take longer (> 5 minutes)
    wait_for_file_reschedule = FileSensor(
        task_id="wait_for_file_reschedule_mode",
        filepath="/tmp/airflow_sensor_test_2.txt",
        poke_interval=60,      # Check every 60 seconds
        timeout=3600,          # Timeout after 1 hour
        mode="reschedule",     # Release worker between checks
        exponential_backoff=True,  # Increase interval over time
    )

    @task
    def process_file():
        """Process the file after it arrives."""
        print("File detected! Processing...")
        return {"status": "processed"}

    start >> wait_for_file_poke >> process_file()
    start >> wait_for_file_reschedule


# =============================================================================
# DAG 2: Time Sensors
# =============================================================================

with DAG(
    dag_id="07_time_sensor_demo",
    description="Demonstrates time-based sensors",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # Run daily at 6 AM
    catchup=False,
    tags=["example", "module-11", "sensors"],
):
    # Wait until a specific time
    wait_for_market_open = TimeSensor(
        task_id="wait_for_market_open",
        target_time=datetime.now().replace(hour=9, minute=30).time(),
        mode="reschedule",
    )

    # Wait for a duration after DAG starts
    wait_5_minutes = TimeDeltaSensor(
        task_id="wait_5_minutes",
        delta=timedelta(minutes=5),
    )

    @task
    def execute_trades():
        """Execute trades after market opens."""
        print("Market is open! Executing trades...")
        return {"trades": 10, "status": "executed"}

    @task
    def delayed_report():
        """Run a report after waiting."""
        print("Running delayed report...")
        return {"report": "generated"}

    wait_for_market_open >> execute_trades()
    wait_5_minutes >> delayed_report()


# =============================================================================
# DAG 3: External Task Sensor
# =============================================================================

with DAG(
    dag_id="07_external_sensor_producer",
    description="Producer DAG for external task sensor demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "module-11", "sensors", "producer"],
):
    @task
    def produce_data():
        """Produce data that downstream DAGs depend on."""
        print("Producing data...")
        return {"data": "produced", "timestamp": datetime.now().isoformat()}

    produce_data()


with DAG(
    dag_id="07_external_sensor_consumer",
    description="Consumer DAG that waits for producer",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example", "module-11", "sensors", "consumer"],
):
    # Wait for external DAG/task to complete
    wait_for_producer = ExternalTaskSensor(
        task_id="wait_for_producer",
        external_dag_id="07_external_sensor_producer",
        external_task_id="produce_data",
        # Wait for same execution_date by default
        # Use execution_delta for offset:
        # execution_delta=timedelta(hours=-1),  # Wait for previous hour
        mode="reschedule",
        timeout=3600,
        poke_interval=60,
    )

    @task
    def consume_data():
        """Consume data after producer completes."""
        print("Producer complete! Consuming data...")
        return {"status": "consumed"}

    wait_for_producer >> consume_data()


# =============================================================================
# DAG 4: Sensor Best Practices
# =============================================================================

with DAG(
    dag_id="07_sensor_best_practices",
    description="Demonstrates sensor configuration best practices",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    # Pool to limit concurrent sensors
    default_args={
        "pool": "sensor_pool",
    },
    tags=["example", "module-11", "sensors", "best-practices"],
):
    # Best Practice 1: Use soft_fail to prevent DAG failures
    check_optional_file = FileSensor(
        task_id="check_optional_file",
        filepath="/tmp/optional_file.txt",
        timeout=60,
        soft_fail=True,  # Skip instead of fail
        mode="reschedule",
    )

    # Best Practice 2: Use exponential backoff for long waits
    long_wait_sensor = FileSensor(
        task_id="long_wait_with_backoff",
        filepath="/tmp/long_wait_file.txt",
        poke_interval=60,
        timeout=7200,  # 2 hours
        mode="reschedule",
        exponential_backoff=True,  # Reduces polling load
    )

    # Best Practice 3: Chain sensors with actual work
    @task
    def process_if_file_exists():
        """Process only runs if sensor succeeds."""
        print("Processing file...")
        return "processed"

    @task
    def run_anyway():
        """This runs regardless of optional sensor."""
        print("Running anyway...")
        return "complete"

    # Optional file doesn't block main workflow
    check_optional_file >> process_if_file_exists()

    # Create a join point
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed_min_one_success",
    )

    [process_if_file_exists(), run_anyway()] >> end
