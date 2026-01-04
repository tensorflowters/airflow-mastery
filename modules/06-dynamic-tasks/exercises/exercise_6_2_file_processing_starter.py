"""
Exercise 6.2: File Processing Simulation
========================================

Learn Dynamic Task Mapping for file processing scenarios.

You'll create:
- A task that lists files to process
- A mapped task that processes each file
- An aggregation task that generates a summary report
"""

from datetime import datetime

# TODO: Import dag and task from airflow.sdk
# from airflow.sdk import dag, task


# =========================================================================
# DAG DEFINITION
# =========================================================================

# TODO: Add @dag decorator
# @dag(
#     dag_id="exercise_6_2_file_processing",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-06", "dynamic-tasks", "files"],
#     description="File processing pipeline with dynamic task mapping",
# )
def file_processing_dag():
    """
    File Processing Pipeline.

    Simulates a common real-world scenario:
    1. List files from a directory/storage
    2. Process each file independently (in parallel)
    3. Generate a summary report

    This pattern is used for:
    - Data lake ingestion
    - Batch file processing
    - Log analysis
    - Report generation
    """

    # =====================================================================
    # TASK 1: List Files
    # =====================================================================

    # TODO: Create task that simulates listing files
    # @task
    # def list_files() -> list[str]:
    #     """
    #     Simulate listing files from a directory.
    #
    #     In production, this might:
    #     - List files from S3 bucket
    #     - Query database for pending files
    #     - Scan local directory
    #     """
    #     import random
    #
    #     print("=" * 60)
    #     print("LIST FILES")
    #     print("=" * 60)
    #     print()
    #
    #     # Generate random number of files (5-10)
    #     num_files = random.randint(5, 10)
    #     extensions = ["csv", "json", "parquet"]
    #
    #     files = []
    #     for i in range(num_files):
    #         ext = random.choice(extensions)
    #         files.append(f"data_{i:03d}.{ext}")
    #
    #     print(f"📁 Found {len(files)} files to process:")
    #     for f in files:
    #         print(f"   - {f}")
    #     print()
    #     print("Each file will be processed by a separate task instance.")
    #     print("=" * 60)
    #
    #     return files

    # =====================================================================
    # TASK 2: Process File (Mapped)
    # =====================================================================

    # TODO: Create task that processes a single file
    # @task
    # def process_file(filename: str, **context) -> dict:
    #     """
    #     Process a single file.
    #
    #     This task runs once per file via expand().
    #     Each instance processes its assigned file independently.
    #     """
    #     import random
    #     import time
    #
    #     # Access map index
    #     map_index = context.get("map_index", 0)
    #
    #     print("=" * 40)
    #     print(f"PROCESS FILE [{map_index}]: {filename}")
    #     print("=" * 40)
    #
    #     # Simulate processing time
    #     time.sleep(0.1)
    #
    #     # Extract file info
    #     extension = filename.split(".")[-1]
    #
    #     # Simulate metadata extraction
    #     result = {
    #         "filename": filename,
    #         "extension": extension,
    #         "size_mb": round(random.uniform(1, 50), 2),
    #         "row_count": random.randint(100, 10000),
    #         "columns": random.randint(5, 20),
    #         "status": "processed",
    #         "map_index": map_index,
    #     }
    #
    #     print(f"  Size: {result['size_mb']} MB")
    #     print(f"  Rows: {result['row_count']:,}")
    #     print(f"  Columns: {result['columns']}")
    #     print(f"  Status: {result['status']}")
    #     print("=" * 40)
    #
    #     return result

    # =====================================================================
    # TASK 3: Generate Report
    # =====================================================================

    # TODO: Create task that aggregates results
    # @task
    # def generate_report(file_results: list[dict]) -> dict:
    #     """
    #     Generate a summary report from all processed files.
    #
    #     Receives a list of all results from the mapped tasks.
    #     """
    #     print("=" * 60)
    #     print("GENERATE REPORT")
    #     print("=" * 60)
    #     print()
    #
    #     # Calculate totals
    #     total_files = len(file_results)
    #     total_size = sum(f["size_mb"] for f in file_results)
    #     total_rows = sum(f["row_count"] for f in file_results)
    #
    #     # Count by extension
    #     ext_counts = {}
    #     for f in file_results:
    #         ext = f["extension"]
    #         ext_counts[ext] = ext_counts.get(ext, 0) + 1
    #
    #     report = {
    #         "total_files": total_files,
    #         "total_size_mb": round(total_size, 2),
    #         "total_rows": total_rows,
    #         "avg_rows_per_file": round(total_rows / total_files, 0),
    #         "by_extension": ext_counts,
    #         "status": "complete",
    #     }
    #
    #     print("📊 Processing Summary:")
    #     print("-" * 40)
    #     print(f"  Total files: {report['total_files']}")
    #     print(f"  Total size: {report['total_size_mb']} MB")
    #     print(f"  Total rows: {report['total_rows']:,}")
    #     print(f"  Avg rows/file: {report['avg_rows_per_file']:,.0f}")
    #     print()
    #     print("  By extension:")
    #     for ext, count in report["by_extension"].items():
    #         print(f"    .{ext}: {count} files")
    #     print()
    #     print("=" * 60)
    #
    #     return report

    # =====================================================================
    # WIRE UP THE PIPELINE
    # =====================================================================

    # TODO: Connect the tasks
    # files = list_files()
    # processed = process_file.expand(filename=files)
    # generate_report(processed)

    pass  # Remove when implementing


# TODO: Instantiate the DAG
# file_processing_dag()
