"""
Solution 2.3: Mixed Operators and TaskFlow
==========================================

Complete solution demonstrating:
- BashOperator creating files and returning paths via XCom
- TaskFlow tasks consuming operator output via .output
- PythonOperator accessing XCom with xcom_pull
- Mixed dependency patterns
"""

from datetime import datetime
from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


def cleanup_function(**context):
    """
    Traditional Python callable demonstrating xcom_pull.

    This shows how to access XCom values in traditional PythonOperator
    tasks, which is useful when you need the old pattern or when
    integrating with legacy code.

    Args:
        **context: Airflow context including ti (TaskInstance)
    """
    ti = context["ti"]

    # Pull XCom from BashOperator (returns the file path)
    file_path = ti.xcom_pull(task_ids="create_temp_file")

    # Pull XCom from TaskFlow task (returns the dict we returned)
    process_result = ti.xcom_pull(task_ids="process_file")

    print("=" * 60)
    print("CLEANUP TASK (PythonOperator)")
    print("=" * 60)
    print(f"File path from BashOperator: {file_path}")
    print(f"Process result from TaskFlow: {process_result}")
    print()

    # Simulate cleanup
    if file_path:
        file_path = file_path.strip()
        print(f"Simulating cleanup of: {file_path}")
        # In production: os.remove(file_path)
        print(f"File {file_path} would be deleted here")

    print("=" * 60)
    print("Cleanup completed!")
    print("=" * 60)


with DAG(
    dag_id="solution_2_3_mixed_operators",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-02", "mixed"],
    description="Demonstrates mixing traditional operators with TaskFlow",
) as dag:

    # =========================================================================
    # TRADITIONAL OPERATOR: BashOperator
    # =========================================================================

    create_temp_file = BashOperator(
        task_id="create_temp_file",
        bash_command="""
            # Create a temporary file
            TEMP_FILE=$(mktemp /tmp/airflow_exercise_XXXXXX)

            # Write some data to it
            echo "Exercise 2.3 - Mixed Operators Demo" > $TEMP_FILE
            echo "Created at: $(date)" >> $TEMP_FILE
            echo "This file demonstrates BashOperator to TaskFlow data passing" >> $TEMP_FILE

            # Log what we did
            echo "Created temp file with content:" >&2
            cat $TEMP_FILE >&2

            # The LAST echo to stdout becomes the XCom return value
            echo $TEMP_FILE
        """,
        doc_md="""
        ### Create Temp File

        This BashOperator:
        1. Creates a temporary file using mktemp
        2. Writes sample content to demonstrate file operations
        3. Echoes the file path (which becomes the XCom return value)

        **Note**: In Bash, the last line echoed to stdout becomes the XCom value.
        Use `>&2` to send logs to stderr without affecting the XCom.
        """,
    )

    # =========================================================================
    # TASKFLOW TASK: Read File Info
    # =========================================================================

    @task
    def read_file_info(file_path: str) -> dict:
        """
        Receive file path from BashOperator and extract metadata.

        This demonstrates the .output pattern for consuming traditional
        operator output in TaskFlow tasks.

        Args:
            file_path: The temp file path from create_temp_file.output

        Returns:
            Dictionary with file metadata
        """
        # Clean up the path (bash echo adds newline)
        file_path = file_path.strip()

        print("=" * 60)
        print("READ FILE INFO (TaskFlow)")
        print("=" * 60)
        print(f"Received file path: {file_path}")

        # Extract metadata
        filename = file_path.split("/")[-1]

        # In production, you might check file size, modification time, etc.
        metadata = {
            "path": file_path,
            "filename": filename,
            "directory": "/tmp",
            "received_at": datetime.now().isoformat(),
        }

        print(f"Extracted metadata: {metadata}")
        print("=" * 60)

        return metadata

    # =========================================================================
    # TASKFLOW TASK: Process File
    # =========================================================================

    @task
    def process_file(file_info: dict) -> dict:
        """
        Simulate processing the file content.

        This shows how TaskFlow tasks naturally chain together,
        with XCom passed automatically via function parameters.

        Args:
            file_info: Dictionary from read_file_info task

        Returns:
            Dictionary with processing results
        """
        print("=" * 60)
        print("PROCESS FILE (TaskFlow)")
        print("=" * 60)
        print(f"Processing file: {file_info['path']}")
        print(f"Filename: {file_info['filename']}")

        # Simulate file processing
        # In production: actually read and process the file
        lines_processed = 3  # We wrote 3 lines in the bash command

        result = {
            "file_path": file_info["path"],
            "lines_processed": lines_processed,
            "status": "completed",
            "processed_at": datetime.now().isoformat(),
        }

        print(f"Processing result: {result}")
        print("=" * 60)

        return result

    # =========================================================================
    # TRADITIONAL OPERATOR: PythonOperator
    # =========================================================================

    cleanup = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup_function,
        doc_md="""
        ### Cleanup Task

        This PythonOperator demonstrates the traditional way of accessing XCom:
        - Uses `ti.xcom_pull()` to get values from upstream tasks
        - Works with both traditional operators and TaskFlow tasks

        This pattern is useful for:
        - Migrating legacy code
        - Complex cleanup logic
        - Cases where you need explicit XCom key access
        """,
    )

    # =========================================================================
    # WIRE UP DEPENDENCIES
    # =========================================================================

    # TaskFlow creates dependencies through function calls:
    # create_temp_file.output feeds into read_file_info
    file_info = read_file_info(create_temp_file.output)

    # read_file_info output feeds into process_file
    process_result = process_file(file_info)

    # PythonOperator needs explicit dependency since it doesn't
    # receive TaskFlow output as a parameter
    process_result >> cleanup

    # Alternative syntax (equivalent):
    # cleanup.set_upstream(process_result)
