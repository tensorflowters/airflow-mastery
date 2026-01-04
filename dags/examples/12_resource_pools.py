"""
Example DAG: Resource Pools and Priority

Demonstrates using pools to limit concurrent task execution
and priority weights to control execution order.

Module: 14-resource-management
"""

from datetime import datetime
import time

from airflow.sdk import DAG, task
from airflow.operators.empty import EmptyOperator


# =============================================================================
# DAG 1: Pool-Controlled Execution
# =============================================================================

with DAG(
    dag_id="12_pool_controlled_execution",
    description="Uses pools to limit concurrent database operations",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-14", "pools"],
):
    """
    This DAG demonstrates pool usage.

    Before running, create the pool via CLI or UI:
        airflow pools set database_pool 3 "Limit concurrent DB operations"

    The pool ensures only 3 database operations run at once,
    even if the scheduler has capacity for more.
    """

    start = EmptyOperator(task_id="start")

    # Create multiple tasks that share the database pool
    @task(pool="default_pool")  # Use default_pool as fallback
    def db_operation(table_id: int):
        """
        Database operation limited by pool.

        Without pools, all 10 would run simultaneously.
        With pool slots=3, only 3 run at once.
        """
        print(f"Starting DB operation for table {table_id}")
        time.sleep(2)  # Simulate work
        print(f"Completed DB operation for table {table_id}")
        return {"table_id": table_id, "status": "success"}

    # Create 10 parallel database operations
    db_tasks = [db_operation.override(task_id=f"db_op_{i}")(i) for i in range(10)]

    @task
    def summarize(results: list):
        """Summarize all database operations."""
        print(f"Completed {len(results)} database operations")
        return {"total": len(results)}

    end = EmptyOperator(task_id="end")

    start >> db_tasks >> summarize(db_tasks) >> end


# =============================================================================
# DAG 2: Multiple Pool Slots
# =============================================================================

with DAG(
    dag_id="12_multiple_pool_slots",
    description="Tasks can consume multiple pool slots",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-14", "pools"],
):
    """
    Tasks can consume multiple slots from a pool.

    If pool has 10 slots and task uses 5, only 2 such tasks
    can run concurrently.
    """

    @task(pool="default_pool", pool_slots=1)
    def light_task(task_num: int):
        """Light task uses 1 slot."""
        print(f"Light task {task_num} running (1 slot)")
        time.sleep(1)
        return f"light_{task_num}"

    @task(pool="default_pool", pool_slots=2)
    def medium_task(task_num: int):
        """Medium task uses 2 slots."""
        print(f"Medium task {task_num} running (2 slots)")
        time.sleep(2)
        return f"medium_{task_num}"

    @task(pool="default_pool", pool_slots=4)
    def heavy_task(task_num: int):
        """
        Heavy task uses 4 slots.

        This limits concurrent heavy tasks and reserves
        resources proportionally.
        """
        print(f"Heavy task {task_num} running (4 slots)")
        time.sleep(3)
        return f"heavy_{task_num}"

    # Mix of task weights
    light_tasks = [light_task.override(task_id=f"light_{i}")(i) for i in range(5)]
    medium_tasks = [medium_task.override(task_id=f"medium_{i}")(i) for i in range(3)]
    heavy_tasks = [heavy_task.override(task_id=f"heavy_{i}")(i) for i in range(2)]

    @task
    def complete():
        return "complete"

    [*light_tasks, *medium_tasks, *heavy_tasks] >> complete()


# =============================================================================
# DAG 3: Priority Weights
# =============================================================================

with DAG(
    dag_id="12_priority_weights",
    description="Priority weights control execution order",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-14", "priority"],
):
    """
    Priority weights determine execution order when resources are limited.

    Higher weight = higher priority = runs first.
    Default weight is 1.
    """

    start = EmptyOperator(task_id="start")

    # Critical priority - runs first
    @task(priority_weight=100)
    def critical_task():
        """Highest priority - executes first when resources available."""
        print("CRITICAL task executing")
        time.sleep(1)
        return {"priority": "critical"}

    # High priority
    @task(priority_weight=75)
    def high_priority_task():
        """High priority task."""
        print("HIGH priority task executing")
        time.sleep(1)
        return {"priority": "high"}

    # Normal priority (default is 1, using 50 for clarity)
    @task(priority_weight=50)
    def normal_task():
        """Normal priority task."""
        print("NORMAL priority task executing")
        time.sleep(1)
        return {"priority": "normal"}

    # Low priority
    @task(priority_weight=25)
    def low_priority_task():
        """Low priority task."""
        print("LOW priority task executing")
        time.sleep(1)
        return {"priority": "low"}

    # Background priority
    @task(priority_weight=1)
    def background_task():
        """Lowest priority - runs when nothing else is waiting."""
        print("BACKGROUND task executing")
        time.sleep(1)
        return {"priority": "background"}

    end = EmptyOperator(task_id="end")

    # All start at same time, but priority determines order
    start >> [
        critical_task(),
        high_priority_task(),
        normal_task(),
        low_priority_task(),
        background_task(),
    ] >> end


# =============================================================================
# DAG 4: Combining Pools and Priority
# =============================================================================

with DAG(
    dag_id="12_pools_and_priority",
    description="Combining pools with priority for fine-grained control",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-14", "pools", "priority"],
):
    """
    Pools + Priority = Powerful resource management.

    Pool limits HOW MANY can run.
    Priority determines WHO runs first.
    """

    @task(pool="default_pool", priority_weight=100)
    def vip_customer_report():
        """VIP customers get priority access to database."""
        print("Generating VIP customer report (priority=100)")
        time.sleep(2)
        return {"report": "vip_customers"}

    @task(pool="default_pool", priority_weight=75)
    def financial_report():
        """Financial reports are high priority."""
        print("Generating financial report (priority=75)")
        time.sleep(2)
        return {"report": "financial"}

    @task(pool="default_pool", priority_weight=50)
    def operational_report():
        """Standard operational report."""
        print("Generating operational report (priority=50)")
        time.sleep(2)
        return {"report": "operational"}

    @task(pool="default_pool", priority_weight=25)
    def analytics_report():
        """Analytics can wait."""
        print("Generating analytics report (priority=25)")
        time.sleep(2)
        return {"report": "analytics"}

    @task(pool="default_pool", priority_weight=10)
    def archive_data():
        """Archiving is lowest priority."""
        print("Archiving old data (priority=10)")
        time.sleep(2)
        return {"archived": True}

    # All compete for pool slots, priority determines order
    [
        vip_customer_report(),
        financial_report(),
        operational_report(),
        analytics_report(),
        archive_data(),
    ]


# =============================================================================
# DAG 5: Concurrency Limits
# =============================================================================

with DAG(
    dag_id="12_concurrency_limits",
    description="DAG and task level concurrency limits",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    # DAG-level concurrency limits
    max_active_runs=2,      # Only 2 DAG runs at once
    max_active_tasks=5,     # Max 5 tasks running per DAG run
    tags=["example", "module-14", "concurrency"],
):
    """
    Concurrency Hierarchy:

    1. System parallelism (core.parallelism)
    2. DAG max_active_runs (per DAG)
    3. DAG max_active_tasks (per DAG run)
    4. Pool slots (shared resource)
    5. Task max_active_tis_per_dag (per task type)
    """

    # Task-level concurrency
    @task(max_active_tis_per_dag=3)
    def limited_across_runs(batch_id: int):
        """
        Only 3 instances of this task can run across ALL DAG runs.

        If 2 DAG runs are active, they share the 3 slots.
        """
        print(f"Processing batch {batch_id}")
        time.sleep(2)
        return {"batch": batch_id}

    @task(max_active_tis_per_dagrun=2)
    def limited_per_run(item_id: int):
        """
        Only 2 instances per DAG run.

        Each DAG run can have 2 running, regardless of other runs.
        """
        print(f"Processing item {item_id}")
        time.sleep(1)
        return {"item": item_id}

    @task(max_active_tis_per_dag=2, max_active_tis_per_dagrun=1)
    def most_restricted(job_id: int):
        """
        Most restrictive: 2 total, but only 1 per run.

        Even with 3 DAG runs, only 2 of this task can run.
        And each run can only have 1 running.
        """
        print(f"Processing job {job_id}")
        time.sleep(3)
        return {"job": job_id}

    # Create multiple instances
    batches = [limited_across_runs.override(task_id=f"batch_{i}")(i) for i in range(6)]
    items = [limited_per_run.override(task_id=f"item_{i}")(i) for i in range(4)]
    jobs = [most_restricted.override(task_id=f"job_{i}")(i) for i in range(3)]

    @task
    def finalize():
        return "complete"

    [*batches, *items, *jobs] >> finalize()
