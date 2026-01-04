"""
Exercise 2.3: Mixed Operators and TaskFlow
==========================================

Learn to combine traditional Airflow operators with TaskFlow tasks.

You'll practice:
- Using BashOperator with XCom
- Consuming operator output in TaskFlow tasks
- Using .output to bridge operators and TaskFlow
- Traditional PythonOperator with xcom_pull
"""

from datetime import datetime

# TODO: Import DAG and task from airflow.sdk
# from airflow.sdk import DAG, task

# TODO: Import traditional operators
# from airflow.providers.standard.operators.bash import BashOperator
# from airflow.providers.standard.operators.python import PythonOperator


# =========================================================================
# PYTHON CALLABLE FOR PYTHONOPERATOR
# =========================================================================

# TODO: Define cleanup function for PythonOperator
# def cleanup_function(**context):
#     """
#     Traditional Python callable that accesses XCom.
#
#     This shows the 'old way' of accessing data between tasks.
#     """
#     ti = context['ti']
#
#     # Pull XCom from create_temp_file task
#     # file_path = ti.xcom_pull(task_ids='create_temp_file')
#
#     # Pull XCom from process_file task
#     # process_result = ti.xcom_pull(task_ids='process_file')
#
#     # Simulate cleanup
#     # print(f"Cleaning up file: {file_path}")
#     # print(f"Process result was: {process_result}")
#
#     pass


# =========================================================================
# DAG DEFINITION
# =========================================================================

# TODO: Create DAG using context manager
# with DAG(
#     dag_id="exercise_2_3_mixed_operators",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-02", "mixed"],
# ) as dag:

    # =========================================================================
    # TRADITIONAL OPERATOR: BashOperator
    # =========================================================================

    # TODO: Create BashOperator that creates a temp file
    # Requirements:
    # - Create temp file with mktemp
    # - Write some content to it
    # - Echo the file path (becomes XCom return value)
    #
    # create_temp_file = BashOperator(
    #     task_id="create_temp_file",
    #     bash_command="""
    #         TEMP_FILE=$(mktemp /tmp/airflow_exercise_XXXXXX)
    #         echo "Exercise 2.3 data: $(date)" > $TEMP_FILE
    #         echo "More data line 2" >> $TEMP_FILE
    #         echo $TEMP_FILE
    #     """,
    # )

    # =========================================================================
    # TASKFLOW TASK: Read File Info
    # =========================================================================

    # TODO: Create TaskFlow task that receives BashOperator output
    # @task
    # def read_file_info(file_path: str) -> dict:
    #     """
    #     Receive file path from BashOperator and return metadata.
    #
    #     Note: file_path comes from create_temp_file.output
    #     """
    #     # Strip whitespace (bash echo adds newline)
    #     # file_path = file_path.strip()
    #
    #     # Return file metadata
    #     # return {
    #     #     "path": file_path,
    #     #     "filename": file_path.split('/')[-1],
    #     #     "received_at": datetime.now().isoformat(),
    #     # }
    #     pass

    # =========================================================================
    # TASKFLOW TASK: Process File
    # =========================================================================

    # TODO: Create TaskFlow task that processes the file
    # @task
    # def process_file(file_info: dict) -> dict:
    #     """
    #     Simulate processing the file.
    #     """
    #     # print(f"Processing file: {file_info['path']}")
    #
    #     # Simulate reading and processing
    #     # lines_processed = 2  # We wrote 2 lines
    #
    #     # return {
    #     #     "file_path": file_info["path"],
    #     #     "lines_processed": lines_processed,
    #     #     "status": "completed",
    #     #     "processed_at": datetime.now().isoformat(),
    #     # }
    #     pass

    # =========================================================================
    # TRADITIONAL OPERATOR: PythonOperator
    # =========================================================================

    # TODO: Create PythonOperator for cleanup
    # cleanup = PythonOperator(
    #     task_id="cleanup",
    #     python_callable=cleanup_function,
    # )

    # =========================================================================
    # WIRE UP DEPENDENCIES
    # =========================================================================

    # TODO: Connect the tasks
    # Step 1: TaskFlow task consumes BashOperator output
    # file_info = read_file_info(create_temp_file.output)

    # Step 2: Process file consumes file_info
    # process_result = process_file(file_info)

    # Step 3: Cleanup runs after process (explicit dependency)
    # process_result >> cleanup

    # Alternative dependency syntax if needed:
    # cleanup.set_upstream(process_result)

    pass  # Remove when implementing
