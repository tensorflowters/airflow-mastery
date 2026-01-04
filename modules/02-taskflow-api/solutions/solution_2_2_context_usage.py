"""
Solution 2.2: Context Usage in TaskFlow
=======================================

Complete solution demonstrating:
- Accessing Airflow context with **context parameter
- Using get_current_context() alternative
- Dynamic file naming based on execution dates
- Data interval awareness
"""

from datetime import datetime
from airflow.sdk import dag, task, get_current_context


@dag(
    dag_id="solution_2_2_context_usage",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-02", "context"],
    description="Demonstrates context access patterns in TaskFlow tasks",
)
def context_usage_dag():
    """
    Context Usage Pipeline.

    Shows how to access Airflow's runtime context including:
    - logical_date (the logical execution time)
    - data_interval_start/end (the data period this run covers)
    - dag_run, ti, params, and other context variables
    """

    @task
    def get_execution_info(**context) -> dict:
        """
        Extract execution information from Airflow context.

        The **context parameter receives all Airflow context variables
        including logical_date, data intervals, dag_run, ti, etc.

        Returns:
            Dictionary with formatted execution metadata
        """
        # Access key context variables
        logical_date = context["logical_date"]
        data_interval_start = context["data_interval_start"]
        data_interval_end = context["data_interval_end"]
        dag_run = context["dag_run"]
        ti = context["ti"]

        # Log context information
        print("=" * 60)
        print("EXECUTION CONTEXT")
        print("=" * 60)
        print(f"Logical Date: {logical_date}")
        print(f"Data Interval: {data_interval_start} to {data_interval_end}")
        print(f"Run ID: {dag_run.run_id}")
        print(f"Task ID: {ti.task_id}")
        print("=" * 60)

        return {
            "logical_date": logical_date.strftime("%Y-%m-%d"),
            "logical_date_full": logical_date.isoformat(),
            "interval_start": data_interval_start.isoformat() if data_interval_start else None,
            "interval_end": data_interval_end.isoformat() if data_interval_end else None,
            "run_id": dag_run.run_id,
            "task_id": ti.task_id,
        }

    @task
    def generate_filename(exec_info: dict) -> dict:
        """
        Generate a filename based on execution metadata.

        This demonstrates a common pattern: using logical_date
        to create date-partitioned filenames for idempotent processing.

        Args:
            exec_info: Dictionary from get_execution_info task

        Returns:
            Dictionary with filename and path information
        """
        # Alternative: access context directly in this task
        context = get_current_context()
        logical_date = context["logical_date"]

        # Create filename with date components
        filename = f"data_{logical_date.strftime('%Y-%m-%d_%H%M%S')}.csv"
        base_path = "/data/output"
        full_path = f"{base_path}/{logical_date.strftime('%Y/%m/%d')}/{filename}"

        print(f"Generated filename: {filename}")
        print(f"Full path: {full_path}")

        return {
            "filename": filename,
            "path": full_path,
            "base_path": base_path,
            "date_partition": logical_date.strftime("%Y/%m/%d"),
            "generated_from_run": exec_info["run_id"],
        }

    @task
    def simulate_file_write(file_info: dict) -> dict:
        """
        Simulate writing data to the generated file path.

        In production, this would actually write to storage.
        Here we just simulate and return metadata.

        Args:
            file_info: Dictionary with filename and path

        Returns:
            Dictionary with write operation metadata
        """
        import random

        print("=" * 60)
        print("SIMULATING FILE WRITE")
        print("=" * 60)
        print(f"Target path: {file_info['path']}")
        print(f"Filename: {file_info['filename']}")

        # Simulate file write (in production: actual I/O)
        simulated_rows = random.randint(100, 1000)
        simulated_bytes = simulated_rows * 150  # ~150 bytes per row

        print(f"Simulated writing {simulated_rows} rows ({simulated_bytes} bytes)")
        print("=" * 60)

        return {
            "path": file_info["path"],
            "filename": file_info["filename"],
            "bytes_written": simulated_bytes,
            "rows_written": simulated_rows,
            "status": "simulated",
            "timestamp": datetime.now().isoformat(),
        }

    @task
    def log_summary(write_result: dict) -> dict:
        """
        Log a final summary of the entire operation.

        Args:
            write_result: Dictionary with write operation results

        Returns:
            Dictionary with overall operation status
        """
        print()
        print("=" * 60)
        print("OPERATION SUMMARY")
        print("=" * 60)
        print(f"File: {write_result['filename']}")
        print(f"Path: {write_result['path']}")
        print(f"Rows Written: {write_result['rows_written']}")
        print(f"Bytes Written: {write_result['bytes_written']}")
        print(f"Status: {write_result['status']}")
        print(f"Completed At: {write_result['timestamp']}")
        print("=" * 60)

        return {
            "overall_status": "success",
            "file_path": write_result["path"],
            "total_rows": write_result["rows_written"],
            "total_bytes": write_result["bytes_written"],
            "completed_at": datetime.now().isoformat(),
        }

    # Wire up the pipeline
    exec_info = get_execution_info()
    file_info = generate_filename(exec_info)
    write_result = simulate_file_write(file_info)
    log_summary(write_result)


# Instantiate the DAG
context_usage_dag()
