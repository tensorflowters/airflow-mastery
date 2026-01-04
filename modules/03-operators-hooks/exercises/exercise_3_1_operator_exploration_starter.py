"""
Exercise 3.1: Operator Exploration
==================================

Explore different operator types and branching patterns.

You'll practice:
- BashOperator for shell commands
- PythonOperator for Python functions
- BranchPythonOperator for conditional routing
- EmptyOperator as task placeholders
- Trigger rules for branch convergence
"""

from datetime import datetime

# TODO: Import DAG from airflow.sdk
# from airflow.sdk import DAG

# TODO: Import operators from providers
# from airflow.providers.standard.operators.bash import BashOperator
# from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
# from airflow.providers.standard.operators.empty import EmptyOperator


# =========================================================================
# PYTHON CALLABLES
# =========================================================================

# TODO: Create analyze_data function
# def analyze_data_func(**context):
#     """
#     Analyze the data file and determine status.
#
#     Steps:
#     1. Get file path from upstream XCom
#     2. Read or simulate reading file content
#     3. Extract the STATUS value
#     4. Return the status string
#     """
#     ti = context['ti']
#     file_path = ti.xcom_pull(task_ids='generate_data')
#
#     # For this exercise, we'll simulate based on the file path
#     # In reality, you'd read: open(file_path.strip()).read()
#
#     # Simulate random status (in reality, parse from file)
#     import random
#     status = random.choice(["VALID", "INVALID"])
#
#     print(f"Analyzed file: {file_path}")
#     print(f"Determined status: {status}")
#
#     return status


# TODO: Create decide_branch function
# def decide_branch_func(**context):
#     """
#     Decide which branch to take based on analysis result.
#
#     Returns:
#         str: Task ID of the branch to execute
#     """
#     ti = context['ti']
#     status = ti.xcom_pull(task_ids='analyze_data')
#
#     print(f"Branch decision based on status: {status}")
#
#     if status == "VALID":
#         return "process_valid"
#     else:
#         return "handle_invalid"


# =========================================================================
# DAG DEFINITION
# =========================================================================

# TODO: Create DAG
# with DAG(
#     dag_id="exercise_3_1_operator_exploration",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-03", "operators"],
# ) as dag:

    # =====================================================================
    # TASK 1: BashOperator - Generate Data
    # =====================================================================

    # TODO: Create BashOperator that generates random data
    # generate_data = BashOperator(
    #     task_id="generate_data",
    #     bash_command="""
    #         TEMP_FILE=$(mktemp /tmp/data_XXXXXX)
    #         if [ $((RANDOM % 2)) -eq 0 ]; then
    #             STATUS="VALID"
    #         else
    #             STATUS="INVALID"
    #         fi
    #         echo "timestamp=$(date)" > $TEMP_FILE
    #         echo "value=$RANDOM" >> $TEMP_FILE
    #         echo "STATUS=$STATUS" >> $TEMP_FILE
    #         echo "File created with STATUS=$STATUS" >&2
    #         echo $TEMP_FILE
    #     """,
    # )

    # =====================================================================
    # TASK 2: PythonOperator - Analyze Data
    # =====================================================================

    # TODO: Create PythonOperator
    # analyze_data = PythonOperator(
    #     task_id="analyze_data",
    #     python_callable=analyze_data_func,
    # )

    # =====================================================================
    # TASK 3: BranchPythonOperator - Decide Path
    # =====================================================================

    # TODO: Create BranchPythonOperator
    # decide_branch = BranchPythonOperator(
    #     task_id="decide_branch",
    #     python_callable=decide_branch_func,
    # )

    # =====================================================================
    # TASK 4-5: EmptyOperators - Branch Endpoints
    # =====================================================================

    # TODO: Create branch endpoint tasks
    # process_valid = EmptyOperator(task_id="process_valid")
    # handle_invalid = EmptyOperator(task_id="handle_invalid")

    # =====================================================================
    # TASK 6: EmptyOperator - Final Task
    # =====================================================================

    # TODO: Create final task with trigger_rule
    # Note: Use "none_failed_min_one_success" to run after either branch
    # final_task = EmptyOperator(
    #     task_id="final_task",
    #     trigger_rule="none_failed_min_one_success",
    # )

    # =====================================================================
    # DEPENDENCIES
    # =====================================================================

    # TODO: Set up the task flow
    # generate_data >> analyze_data >> decide_branch
    # decide_branch >> [process_valid, handle_invalid]
    # [process_valid, handle_invalid] >> final_task

    pass  # Remove when implementing
