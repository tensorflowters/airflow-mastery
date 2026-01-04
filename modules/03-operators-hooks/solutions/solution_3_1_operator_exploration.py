"""
Solution 3.1: Operator Exploration
==================================

Complete solution demonstrating:
- BashOperator for file generation
- PythonOperator for data analysis
- BranchPythonOperator for conditional routing
- EmptyOperator as branch endpoints
- Trigger rules for branch convergence
"""

from datetime import datetime
import random
from typing import Any
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


def analyze_data_func(**context: Any) -> str:
    """
    Analyze the data file and determine its validation status.

    In a real scenario, this would:
    1. Read the file created by BashOperator
    2. Parse the content
    3. Extract the STATUS field

    For this exercise, we simulate the analysis.
    """
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="generate_data")

    print("=" * 60)
    print("ANALYZING DATA")
    print("=" * 60)
    print(f"File path received: {file_path}")

    # In production, you would read the actual file:
    # with open(file_path.strip(), 'r') as f:
    #     content = f.read()
    #     # Parse STATUS from content

    # For exercise, simulate random status
    # (The bash command also randomizes, but we simulate here for reliability)
    status = random.choice(["VALID", "INVALID"])

    print(f"Analysis result - Status: {status}")
    print("=" * 60)

    return status


def decide_branch_func(**context: Any) -> str:
    """
    Decide which branch to execute based on analysis result.

    BranchPythonOperator requires returning the task_id(s) to execute.
    Tasks not returned will be skipped.

    Returns:
        str: The task_id of the branch to execute
    """
    ti = context["ti"]
    status = ti.xcom_pull(task_ids="analyze_data")

    print("=" * 60)
    print("BRANCH DECISION")
    print("=" * 60)
    print(f"Status from analysis: {status}")

    if status == "VALID":
        decision = "process_valid"
        print("Decision: Route to VALID processing path")
    else:
        decision = "handle_invalid"
        print("Decision: Route to INVALID handling path")

    print(f"Returning task_id: {decision}")
    print("=" * 60)

    return decision


with DAG(
    dag_id="solution_3_1_operator_exploration",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-03", "operators", "branching"],
    description="Demonstrates multiple operator types with conditional branching",
) as dag:

    # =========================================================================
    # TASK 1: Generate Random Data File
    # =========================================================================

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="""
            # Create temporary file
            TEMP_FILE=$(mktemp /tmp/data_XXXXXX)

            # Generate random status
            if [ $((RANDOM % 2)) -eq 0 ]; then
                STATUS="VALID"
            else
                STATUS="INVALID"
            fi

            # Write data to file
            echo "timestamp=$(date -Iseconds)" > $TEMP_FILE
            echo "random_value=$RANDOM" >> $TEMP_FILE
            echo "STATUS=$STATUS" >> $TEMP_FILE

            # Log to stderr (doesn't affect XCom)
            echo "Generated file with STATUS=$STATUS" >&2
            cat $TEMP_FILE >&2

            # Echo file path as XCom return value
            echo $TEMP_FILE
        """,
        doc_md="""
        ### Generate Data Task

        Creates a temporary file with:
        - Timestamp
        - Random value
        - Random STATUS (VALID or INVALID)

        The file path is returned via XCom for downstream tasks.
        """,
    )

    # =========================================================================
    # TASK 2: Analyze the Data
    # =========================================================================

    analyze_data = PythonOperator(
        task_id="analyze_data",
        python_callable=analyze_data_func,
        doc_md="""
        ### Analyze Data Task

        Reads the file from upstream and determines the STATUS.
        Returns the status string via XCom.
        """,
    )

    # =========================================================================
    # TASK 3: Branch Decision
    # =========================================================================

    decide_branch = BranchPythonOperator(
        task_id="decide_branch",
        python_callable=decide_branch_func,
        doc_md="""
        ### Branch Decision Task

        Uses BranchPythonOperator to route execution:
        - VALID status → process_valid task
        - INVALID status → handle_invalid task

        **Note**: The non-selected branch will be SKIPPED (not failed).
        """,
    )

    # =========================================================================
    # TASK 4-5: Branch Endpoints
    # =========================================================================

    process_valid = EmptyOperator(
        task_id="process_valid",
        doc_md="Executed when data is VALID",
    )

    handle_invalid = EmptyOperator(
        task_id="handle_invalid",
        doc_md="Executed when data is INVALID",
    )

    # =========================================================================
    # TASK 6: Final Convergence Task
    # =========================================================================

    final_task = EmptyOperator(
        task_id="final_task",
        trigger_rule="none_failed_min_one_success",
        doc_md="""
        ### Final Task

        Uses `trigger_rule="none_failed_min_one_success"` to:
        - Execute after EITHER branch completes
        - Not fail if one branch was skipped

        **Trigger Rules:**
        - `all_success` (default): All parents must succeed
        - `one_success`: At least one parent succeeds
        - `none_failed`: No parent failed (skipped is OK)
        - `none_failed_min_one_success`: No failures AND at least one success
        """,
    )

    # =========================================================================
    # DEPENDENCIES
    # =========================================================================

    # Linear flow to branch point
    generate_data >> analyze_data >> decide_branch

    # Branch to endpoints
    decide_branch >> [process_valid, handle_invalid]

    # Converge to final task
    [process_valid, handle_invalid] >> final_task
