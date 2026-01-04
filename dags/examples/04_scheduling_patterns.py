"""
Example DAG: Scheduling Patterns

Demonstrates various scheduling configurations and trigger rules.
Shows cron expressions, timetables, and execution control.

Module: 04-scheduling
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, task
from airflow.operators.empty import EmptyOperator


# =============================================================================
# DAG 1: Business Hours Schedule (Cron)
# =============================================================================

with DAG(
    dag_id="04_scheduling_business_hours",
    description="Runs every hour during business hours (9 AM - 5 PM)",
    start_date=datetime(2024, 1, 1),
    # Cron: minute hour day-of-month month day-of-week
    # This runs at minute 0, hours 9-17, every day
    schedule="0 9-17 * * MON-FRI",
    catchup=False,
    tags=["example", "module-04", "scheduling"],
):
    @task
    def hourly_report():
        """Generate hourly business report."""
        current_hour = datetime.now().hour
        print(f"Generating report for hour {current_hour}")
        return {"hour": current_hour, "status": "complete"}

    hourly_report()


# =============================================================================
# DAG 2: Multi-Schedule with Dataset Triggers
# =============================================================================

with DAG(
    dag_id="04_scheduling_interval",
    description="Demonstrates interval-based scheduling",
    start_date=datetime(2024, 1, 1),
    # timedelta for interval scheduling
    schedule=timedelta(hours=6),
    catchup=False,
    # Don't run if previous run still in progress
    max_active_runs=1,
    tags=["example", "module-04", "scheduling"],
):
    @task
    def six_hour_sync():
        """Run every 6 hours."""
        print("Running 6-hour synchronization job")
        return {"synced_at": datetime.now().isoformat()}

    six_hour_sync()


# =============================================================================
# DAG 3: Trigger Rules Demonstration
# =============================================================================

with DAG(
    dag_id="04_trigger_rules_demo",
    description="Demonstrates various trigger rules",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["example", "module-04", "scheduling", "trigger-rules"],
):
    start = EmptyOperator(task_id="start")

    # Parallel tasks - some may fail
    @task
    def task_success():
        """This task always succeeds."""
        print("Task succeeded!")
        return "success"

    @task
    def task_may_fail():
        """This task might fail (simulated)."""
        import random
        if random.random() < 0.3:  # 30% chance of failure
            raise Exception("Simulated failure")
        print("Task succeeded!")
        return "success"

    # Trigger rules determine when downstream tasks run

    # ALL_SUCCESS (default): All upstream tasks must succeed
    all_success = EmptyOperator(
        task_id="all_success_gate",
        trigger_rule="all_success",
    )

    # ALL_FAILED: All upstream tasks must fail
    all_failed = EmptyOperator(
        task_id="all_failed_gate",
        trigger_rule="all_failed",
    )

    # ONE_SUCCESS: At least one upstream task succeeded
    one_success = EmptyOperator(
        task_id="one_success_gate",
        trigger_rule="one_success",
    )

    # ONE_FAILED: At least one upstream task failed
    one_failed = EmptyOperator(
        task_id="one_failed_gate",
        trigger_rule="one_failed",
    )

    # NONE_FAILED: No upstream tasks failed (but can be skipped)
    none_failed = EmptyOperator(
        task_id="none_failed_gate",
        trigger_rule="none_failed",
    )

    # NONE_FAILED_MIN_ONE_SUCCESS: Common for branch joins
    branch_join = EmptyOperator(
        task_id="branch_join_gate",
        trigger_rule="none_failed_min_one_success",
    )

    # ALL_DONE: All upstream tasks completed (any state)
    always_run = EmptyOperator(
        task_id="always_run_cleanup",
        trigger_rule="all_done",
    )

    # Task instances
    t_success = task_success()
    t_may_fail = task_may_fail()

    # Dependencies
    start >> [t_success, t_may_fail]
    [t_success, t_may_fail] >> all_success
    [t_success, t_may_fail] >> all_failed
    [t_success, t_may_fail] >> one_success
    [t_success, t_may_fail] >> one_failed
    [t_success, t_may_fail] >> none_failed
    [t_success, t_may_fail] >> branch_join
    [t_success, t_may_fail] >> always_run


# =============================================================================
# DAG 4: Execution Parameters
# =============================================================================

with DAG(
    dag_id="04_execution_params",
    description="Demonstrates DAG execution parameters",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    # Execution parameters
    max_active_runs=2,           # Max concurrent DAG runs
    max_active_tasks=4,          # Max concurrent tasks per run
    dagrun_timeout=timedelta(hours=1),  # Timeout for entire DAG run
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["example", "module-04", "scheduling"],
):
    @task
    def parameterized_task():
        """Task inherits default_args settings."""
        print("Running with inherited retry and timeout settings")
        return "complete"

    @task(
        retries=5,  # Override default
        retry_delay=timedelta(minutes=1),
    )
    def custom_retry_task():
        """Task with custom retry settings."""
        print("Running with custom retry settings")
        return "complete"

    parameterized_task() >> custom_retry_task()
