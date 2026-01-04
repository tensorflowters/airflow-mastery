"""
Example DAG: Dynamic Task Mapping

Demonstrates Airflow 3's dynamic task mapping feature
with expand(), partial(), and reduce patterns.

Module: 06-dynamic-tasks
"""

from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="06_dynamic_task_mapping",
    description="Dynamic task mapping example with expand()",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-06", "dynamic"],
)
def dynamic_task_mapping():
    """Dynamic task mapping DAG demonstrating expand() and partial()."""

    @task
    def get_files_to_process() -> list[dict]:
        """Simulate discovering files to process.

        In reality, this might:
        - List files from S3/GCS
        - Query a database for pending items
        - Read from a queue
        """
        return [
            {"filename": "data_2024_01.csv", "size_mb": 100},
            {"filename": "data_2024_02.csv", "size_mb": 250},
            {"filename": "data_2024_03.csv", "size_mb": 75},
            {"filename": "data_2024_04.csv", "size_mb": 180},
        ]

    @task
    def process_file(file_info: dict, output_format: str) -> dict:
        """Process a single file.

        Args:
            file_info: Dictionary with filename and metadata
            output_format: Fixed parameter for all instances

        Returns:
            Processing result for this file
        """
        filename = file_info["filename"]
        size = file_info["size_mb"]

        print(f"Processing {filename} ({size}MB) to {output_format}")

        # Simulate processing time based on file size
        processed_rows = size * 1000

        return {
            "input_file": filename,
            "rows_processed": processed_rows,
            "output_format": output_format,
            "status": "success",
        }

    @task
    def generate_summary(results: list[dict]) -> dict:
        """Aggregate results from all processed files.

        This task automatically receives the list of all
        mapped task outputs (reduce pattern).
        """
        total_rows = sum(r["rows_processed"] for r in results)
        successful = sum(1 for r in results if r["status"] == "success")

        summary = {
            "total_files": len(results),
            "successful": successful,
            "failed": len(results) - successful,
            "total_rows_processed": total_rows,
        }

        print(f"Summary: {summary}")
        return summary

    # Step 1: Discover files (single task)
    files = get_files_to_process()

    # Step 2: Process each file dynamically
    # - partial() sets fixed parameters (output_format)
    # - expand_kwargs() creates N instances based on files list
    processed = process_file.partial(
        output_format="parquet"
    ).expand_kwargs(files)

    # Step 3: Aggregate all results (single task)
    # The 'processed' variable contains all mapped outputs
    generate_summary(processed)


# Instantiate the DAG
dynamic_task_mapping()
