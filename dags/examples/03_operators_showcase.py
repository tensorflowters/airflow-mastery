"""
Example DAG: Operators Showcase

Demonstrates various built-in operators and their usage patterns.
Shows BashOperator, PythonOperator, and BranchPythonOperator.

Module: 03-operators-hooks
"""

from datetime import datetime
import random

from airflow.sdk import DAG, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator


def _process_data(**context):
    """Python callable for PythonOperator.

    Demonstrates accessing context and returning values.
    """
    execution_date = context["logical_date"]
    print(f"Processing data for {execution_date}")

    # Simulated processing
    result = {
        "records_processed": 1000,
        "timestamp": datetime.now().isoformat(),
    }

    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="processing_result", value=result)
    return result


def _choose_branch(**context):
    """Branching logic for conditional execution.

    Returns the task_id of the branch to follow.
    This simulates choosing a path based on data conditions.
    """
    # Simulate a condition (in reality, check data or external state)
    random_choice = random.choice(["large", "small"])

    if random_choice == "large":
        print("Dataset is large - running full processing")
        return "process_large_dataset"
    else:
        print("Dataset is small - running quick processing")
        return "process_small_dataset"


with DAG(
    dag_id="03_operators_showcase",
    description="Demonstrates various Airflow operators",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-03", "operators"],
):
    # ==========================================================================
    # BashOperator: Run shell commands
    # ==========================================================================

    start = EmptyOperator(task_id="start")

    check_environment = BashOperator(
        task_id="check_environment",
        bash_command="""
            echo "=== Environment Check ==="
            echo "Date: $(date)"
            echo "User: $USER"
            echo "PWD: $PWD"
            echo "Python: $(python3 --version 2>&1)"
        """,
    )

    # BashOperator with templated command
    create_temp_file = BashOperator(
        task_id="create_temp_file",
        bash_command="""
            echo "Creating temp file for {{ ds }}"
            echo "Execution date: {{ ds }}" > /tmp/airflow_example_{{ ds_nodash }}.txt
            cat /tmp/airflow_example_{{ ds_nodash }}.txt
        """,
    )

    # ==========================================================================
    # PythonOperator: Execute Python callables
    # ==========================================================================

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=_process_data,
        # op_kwargs for additional arguments
        op_kwargs={"custom_param": "value"},
    )

    # ==========================================================================
    # BranchPythonOperator: Conditional branching
    # ==========================================================================

    branch_task = BranchPythonOperator(
        task_id="branch_based_on_size",
        python_callable=_choose_branch,
    )

    # Branch targets
    process_large = BashOperator(
        task_id="process_large_dataset",
        bash_command='echo "Processing LARGE dataset with full resources"',
    )

    process_small = BashOperator(
        task_id="process_small_dataset",
        bash_command='echo "Processing SMALL dataset quickly"',
    )

    # ==========================================================================
    # EmptyOperator: Join point after branching
    # ==========================================================================

    join = EmptyOperator(
        task_id="join_branches",
        trigger_rule="none_failed_min_one_success",  # Important for branches
    )

    cleanup = BashOperator(
        task_id="cleanup",
        bash_command='echo "Cleanup complete"',
    )

    # ==========================================================================
    # Define Dependencies
    # ==========================================================================

    start >> check_environment >> create_temp_file >> process_data
    process_data >> branch_task
    branch_task >> [process_large, process_small]
    [process_large, process_small] >> join >> cleanup
