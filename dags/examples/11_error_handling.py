"""
Example DAG: Error Handling Patterns

Demonstrates retry strategies, callbacks, and error recovery
patterns for robust DAG execution.

Module: 09-production-patterns
"""

from datetime import datetime, timedelta
import random

from airflow.sdk import DAG, task
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException


# =============================================================================
# Callback Functions
# =============================================================================

def on_failure_callback(context):
    """
    Called when a task fails.

    Context contains:
    - task_instance: The failed task instance
    - exception: The exception that caused failure
    - dag_run: The DAG run
    - logical_date: Execution date
    """
    ti = context["task_instance"]
    exception = context.get("exception", "Unknown")

    print(f"FAILURE CALLBACK: Task {ti.task_id} failed!")
    print(f"Exception: {exception}")
    print(f"Try number: {ti.try_number}")

    # In production, you might:
    # - Send Slack/email notification
    # - Create incident ticket
    # - Log to monitoring system


def on_success_callback(context):
    """Called when a task succeeds."""
    ti = context["task_instance"]
    print(f"SUCCESS CALLBACK: Task {ti.task_id} completed successfully!")


def on_retry_callback(context):
    """Called when a task is retried."""
    ti = context["task_instance"]
    print(f"RETRY CALLBACK: Task {ti.task_id} is being retried (attempt {ti.try_number})")


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Called when SLA is missed."""
    print(f"SLA MISS: DAG {dag.dag_id} missed SLA!")
    print(f"Tasks: {task_list}")


# =============================================================================
# DAG 1: Retry Strategies
# =============================================================================

with DAG(
    dag_id="11_error_retry_strategies",
    description="Demonstrates different retry strategies",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": on_failure_callback,
        "on_success_callback": on_success_callback,
        "on_retry_callback": on_retry_callback,
    },
    tags=["example", "module-09", "error-handling"],
):
    start = EmptyOperator(task_id="start")

    # Basic retry with fixed delay
    @task(retries=3, retry_delay=timedelta(seconds=30))
    def basic_retry():
        """Task with basic fixed retry delay."""
        if random.random() < 0.5:  # 50% failure rate
            raise Exception("Simulated failure")
        return {"status": "success"}

    # Exponential backoff retry
    @task(
        retries=5,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
    )
    def exponential_backoff():
        """
        Retry with exponential backoff.

        Delays: 10s, 20s, 40s, 80s, 160s (capped at 10min)
        """
        if random.random() < 0.4:  # 40% failure rate
            raise Exception("Simulated API timeout")
        return {"status": "success"}

    # Custom retry conditions
    @task(
        retries=3,
        retry_delay=timedelta(seconds=5),
    )
    def conditional_retry():
        """
        Task that only retries on specific errors.

        In practice, wrap in try/except to control retry behavior.
        """
        error_type = random.choice(["transient", "permanent", "success"])

        if error_type == "transient":
            # This will trigger retry
            raise Exception("Transient error - please retry")
        elif error_type == "permanent":
            # Use AirflowFailException to fail without retry
            raise AirflowFailException("Permanent error - do not retry")

        return {"status": "success"}

    # Dependencies
    start >> [basic_retry(), exponential_backoff(), conditional_retry()]


# =============================================================================
# DAG 2: Error Recovery Patterns
# =============================================================================

with DAG(
    dag_id="11_error_recovery_patterns",
    description="Patterns for error recovery and graceful degradation",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-09", "error-handling", "recovery"],
):
    @task
    def primary_data_source():
        """Try to get data from primary source."""
        if random.random() < 0.3:  # 30% failure
            raise Exception("Primary source unavailable")
        return {"source": "primary", "data": [1, 2, 3]}

    @task(trigger_rule="all_failed")
    def fallback_data_source():
        """Fallback if primary fails."""
        print("Using fallback data source")
        return {"source": "fallback", "data": [1, 2]}

    @task(trigger_rule="none_failed_min_one_success")
    def process_data(data: dict = None):
        """Process whatever data we got."""
        # In practice, use XCom to get from whichever succeeded
        print(f"Processing data from: {data}")
        return {"processed": True}

    # Pattern: Try primary, fall back if needed
    primary = primary_data_source()
    fallback = fallback_data_source()

    # Both feed into processing
    [primary, fallback] >> process_data()


# =============================================================================
# DAG 3: Skip vs Fail Patterns
# =============================================================================

with DAG(
    dag_id="11_skip_vs_fail",
    description="When to skip vs when to fail",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-09", "error-handling"],
):
    @task
    def check_data_availability():
        """Check if data is available."""
        available = random.random() > 0.3
        return {"available": available}

    @task
    def skip_if_no_data(check_result: dict):
        """
        Skip (not fail) if no data available.

        Use AirflowSkipException when:
        - Condition is expected and okay
        - Downstream tasks should be skipped
        - Not an error condition
        """
        if not check_result["available"]:
            raise AirflowSkipException("No data available - skipping")

        print("Data available - processing...")
        return {"processed": True}

    @task
    def fail_if_invalid_data():
        """
        Fail (not skip) if data is invalid.

        Use Exception when:
        - Condition indicates a real problem
        - Task should be retried
        - Alerting is needed
        """
        data_valid = random.random() > 0.2

        if not data_valid:
            raise Exception("Data validation failed!")

        return {"valid": True}

    @task(trigger_rule="none_failed_min_one_success")
    def final_task():
        """Runs if not failed (skips are okay)."""
        return {"status": "complete"}

    check = check_data_availability()
    skip_if_no_data(check) >> final_task()
    fail_if_invalid_data() >> final_task()


# =============================================================================
# DAG 4: Cleanup on Failure
# =============================================================================

with DAG(
    dag_id="11_cleanup_on_failure",
    description="Cleanup resources even when tasks fail",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-09", "error-handling", "cleanup"],
):
    @task
    def create_temp_resources():
        """Create temporary resources that need cleanup."""
        print("Creating temporary resources...")
        return {"temp_file": "/tmp/airflow_temp_123", "created": True}

    @task
    def risky_processing(resources: dict):
        """Processing that might fail."""
        if random.random() < 0.5:
            raise Exception("Processing failed!")
        return {"processed": True}

    @task(trigger_rule="all_done")
    def cleanup_resources(resources: dict):
        """
        Cleanup runs regardless of upstream success/failure.

        trigger_rule='all_done' ensures this runs after all
        upstream tasks complete (success, failure, or skip).
        """
        print(f"Cleaning up: {resources}")
        # In practice: delete temp files, close connections, etc.
        return {"cleaned": True}

    @task(trigger_rule="all_done")
    def send_completion_notification():
        """Always send notification when done."""
        print("Sending completion notification...")
        return {"notified": True}

    resources = create_temp_resources()
    processing = risky_processing(resources)

    # Cleanup always runs
    [resources, processing] >> cleanup_resources(resources)
    [resources, processing] >> send_completion_notification()


# =============================================================================
# DAG 5: Circuit Breaker Pattern
# =============================================================================

with DAG(
    dag_id="11_circuit_breaker",
    description="Circuit breaker pattern for failing services",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-09", "error-handling", "circuit-breaker"],
):
    @task
    def check_service_health():
        """
        Check if service is healthy before calling.

        Implements circuit breaker pattern:
        - If recent failures > threshold, skip calling service
        - Prevents cascading failures
        """
        # In practice, check a shared state (Variable, Redis, etc.)
        recent_failures = random.randint(0, 10)
        failure_threshold = 5

        status = {
            "recent_failures": recent_failures,
            "threshold": failure_threshold,
            "circuit_open": recent_failures > failure_threshold,
        }

        print(f"Service health check: {status}")
        return status

    @task
    def call_service_if_healthy(health_status: dict):
        """Only call service if circuit is closed."""
        if health_status["circuit_open"]:
            raise AirflowSkipException(
                f"Circuit breaker OPEN - {health_status['recent_failures']} recent failures"
            )

        # Call the service
        if random.random() < 0.2:  # 20% failure
            raise Exception("Service call failed")

        return {"response": "success"}

    @task(trigger_rule="none_failed_min_one_success")
    def process_response(response: dict = None):
        """Process response if we got one."""
        if response:
            print(f"Processing response: {response}")
        else:
            print("No response to process (circuit was open)")
        return {"processed": True}

    health = check_service_health()
    response = call_service_if_healthy(health)
    process_response(response)
