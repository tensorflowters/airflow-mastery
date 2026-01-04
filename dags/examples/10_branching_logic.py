"""
Example DAG: Branching Logic

Demonstrates conditional execution with BranchPythonOperator,
ShortCircuitOperator, and trigger rules for complex flows.

Module: 03-operators-hooks
"""

from datetime import datetime
import random

from airflow.sdk import DAG, task
from airflow.operators.python import BranchPythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator


# =============================================================================
# DAG 1: Basic Branching
# =============================================================================

with DAG(
    dag_id="10_branching_basic",
    description="Demonstrates basic branching patterns",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-03", "branching"],
):
    start = EmptyOperator(task_id="start")

    def choose_branch(**context):
        """
        Branching function returns the task_id to execute.

        This can be based on:
        - Data values
        - External conditions
        - Configuration
        - Time-based logic
        """
        # Simulate different data sizes
        data_size = random.choice(["small", "medium", "large"])

        print(f"Data size detected: {data_size}")

        # Return the task_id to execute
        if data_size == "small":
            return "process_small"
        elif data_size == "medium":
            return "process_medium"
        else:
            return "process_large"

    branch = BranchPythonOperator(
        task_id="determine_processing",
        python_callable=choose_branch,
    )

    # Branch targets
    process_small = EmptyOperator(task_id="process_small")
    process_medium = EmptyOperator(task_id="process_medium")
    process_large = EmptyOperator(task_id="process_large")

    # Join point - must handle skipped branches
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(task_id="end")

    # Dependencies
    start >> branch >> [process_small, process_medium, process_large] >> join >> end


# =============================================================================
# DAG 2: Multi-Branch with TaskFlow
# =============================================================================

with DAG(
    dag_id="10_branching_taskflow",
    description="Branching with TaskFlow API",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-03", "branching", "taskflow"],
):
    @task
    def get_data_status():
        """Determine the status of incoming data."""
        statuses = ["complete", "partial", "missing"]
        status = random.choice(statuses)
        print(f"Data status: {status}")
        return status

    @task.branch
    def branch_on_status(status: str):
        """
        Branch based on data status.

        The @task.branch decorator creates a branching task.
        Return value must be a task_id or list of task_ids.
        """
        if status == "complete":
            return "process_complete_data"
        elif status == "partial":
            return "handle_partial_data"
        else:
            return "handle_missing_data"

    @task
    def process_complete_data():
        print("Processing complete dataset...")
        return {"status": "processed"}

    @task
    def handle_partial_data():
        print("Handling partial data - filling gaps...")
        return {"status": "filled"}

    @task
    def handle_missing_data():
        print("No data available - sending alert...")
        return {"status": "alerted"}

    @task(trigger_rule="none_failed_min_one_success")
    def finalize():
        print("Finalizing workflow...")
        return {"status": "complete"}

    status = get_data_status()
    branch_on_status(status) >> [
        process_complete_data(),
        handle_partial_data(),
        handle_missing_data(),
    ] >> finalize()


# =============================================================================
# DAG 3: Multi-Path Branching
# =============================================================================

with DAG(
    dag_id="10_branching_multipath",
    description="Multiple branches can be selected",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-03", "branching", "multipath"],
):
    start = EmptyOperator(task_id="start")

    def select_multiple_paths(**context):
        """
        Return a LIST of task_ids to execute multiple branches.

        Useful when data should flow through multiple pipelines.
        """
        # Determine which pipelines need to run
        pipelines_needed = []

        # Simulate conditions
        if random.random() > 0.3:
            pipelines_needed.append("pipeline_analytics")
        if random.random() > 0.3:
            pipelines_needed.append("pipeline_reports")
        if random.random() > 0.3:
            pipelines_needed.append("pipeline_ml")

        # Must return at least one
        if not pipelines_needed:
            pipelines_needed = ["pipeline_default"]

        print(f"Selected pipelines: {pipelines_needed}")
        return pipelines_needed

    branch = BranchPythonOperator(
        task_id="select_pipelines",
        python_callable=select_multiple_paths,
    )

    # Multiple possible paths
    analytics = EmptyOperator(task_id="pipeline_analytics")
    reports = EmptyOperator(task_id="pipeline_reports")
    ml = EmptyOperator(task_id="pipeline_ml")
    default = EmptyOperator(task_id="pipeline_default")

    join = EmptyOperator(
        task_id="join_pipelines",
        trigger_rule="none_failed_min_one_success",
    )

    start >> branch >> [analytics, reports, ml, default] >> join


# =============================================================================
# DAG 4: ShortCircuit Operator
# =============================================================================

with DAG(
    dag_id="10_shortcircuit_demo",
    description="ShortCircuitOperator for conditional skipping",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-03", "branching", "shortcircuit"],
):
    start = EmptyOperator(task_id="start")

    def check_should_continue(**context):
        """
        ShortCircuit returns True/False.

        True: Continue to downstream tasks
        False: Skip all downstream tasks
        """
        # Simulate a condition check
        should_run = random.random() > 0.3  # 70% chance to continue
        print(f"Should continue: {should_run}")
        return should_run

    # ShortCircuit skips ALL downstream tasks if False
    gate = ShortCircuitOperator(
        task_id="check_gate",
        python_callable=check_should_continue,
    )

    @task
    def expensive_operation():
        """Only runs if gate returns True."""
        print("Running expensive operation...")
        return {"result": "success"}

    @task
    def send_notification(result: dict):
        """Also skipped if gate returns False."""
        print(f"Sending notification: {result}")
        return {"notified": True}

    # If gate returns False, both tasks are skipped
    start >> gate >> expensive_operation() >> send_notification(expensive_operation())


# =============================================================================
# DAG 5: Complex Branching Patterns
# =============================================================================

with DAG(
    dag_id="10_branching_complex",
    description="Complex branching with nested conditions",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-03", "branching", "complex"],
):
    @task
    def analyze_input():
        """Analyze input to determine processing path."""
        return {
            "data_type": random.choice(["structured", "unstructured"]),
            "priority": random.choice(["high", "normal", "low"]),
        }

    @task.branch
    def branch_by_type(analysis: dict):
        """First level branch: by data type."""
        if analysis["data_type"] == "structured":
            return "process_structured"
        return "process_unstructured"

    @task
    def process_structured():
        return {"processed": "structured"}

    @task
    def process_unstructured():
        return {"processed": "unstructured"}

    # Second level branching within structured path
    @task.branch(trigger_rule="none_failed_min_one_success")
    def branch_by_priority():
        """Second level branch: by priority."""
        # In real scenario, read from XCom
        priority = random.choice(["high", "normal", "low"])
        if priority == "high":
            return "urgent_delivery"
        return "standard_delivery"

    urgent = EmptyOperator(task_id="urgent_delivery")
    standard = EmptyOperator(task_id="standard_delivery")

    @task(trigger_rule="none_failed_min_one_success")
    def complete():
        return {"status": "complete"}

    # Build the flow
    analysis = analyze_input()
    branch_by_type(analysis) >> [process_structured(), process_unstructured()]
    [process_structured(), process_unstructured()] >> branch_by_priority()
    branch_by_priority() >> [urgent, standard]
    [urgent, standard] >> complete()
