"""
Solution 6.2: File Processing Simulation
========================================

Complete solution demonstrating:
- File listing and discovery
- Parallel file processing with expand()
- Result aggregation and reporting
- Accessing map_index in task context
"""

from datetime import datetime
import random
import time
from airflow.sdk import dag, task


@dag(
    dag_id="solution_6_2_file_processing",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-06", "dynamic-tasks", "files"],
    description="File processing pipeline with dynamic task mapping",
)
def file_processing_dag():
    """
    File Processing Pipeline.

    This DAG simulates a common data engineering pattern:
    1. Discover files to process (unknown count at DAG write time)
    2. Process each file independently in parallel
    3. Generate a consolidated summary report

    Real-world applications:
    - S3 file ingestion to data warehouse
    - Log file analysis and aggregation
    - Batch image/document processing
    - Data quality validation across files
    """

    @task
    def list_files() -> list[str]:
        """
        Simulate listing files from a directory or storage system.

        In production, this task might:
        - Use S3Hook.list_keys() to list S3 objects
        - Query a database for pending files
        - Scan a local/network directory
        - Get files from an API endpoint
        """
        print("=" * 60)
        print("LIST FILES - Discovery Phase")
        print("=" * 60)
        print()

        print("🔍 Scanning for files to process...")
        print("   Location: s3://data-lake/incoming/")
        print()

        # Simulate variable file count (5-10 files)
        num_files = random.randint(5, 10)

        # Simulate different file types
        extensions = ["csv", "json", "parquet"]
        weights = [0.5, 0.3, 0.2]  # csv most common

        files = []
        for i in range(num_files):
            # Weighted random extension
            ext = random.choices(extensions, weights=weights)[0]
            # Realistic file naming
            date_str = datetime.now().strftime("%Y%m%d")
            files.append(f"data_{date_str}_{i:03d}.{ext}")

        print(f"📁 Found {len(files)} files to process:")
        print("-" * 40)
        for f in files:
            print(f"   📄 {f}")
        print()

        print("ℹ️  Each file will be processed by a separate task instance")
        print("   created dynamically via expand().")
        print("=" * 60)

        return files

    @task
    def process_file(filename: str, **context) -> dict:
        """
        Process a single file and extract metadata.

        This task:
        - Runs once per file via expand()
        - Executes in parallel with other file instances
        - Has access to map_index in context
        - Returns structured metadata for aggregation
        """
        # Access the map index - useful for logging and debugging
        map_index = context.get("map_index", 0)

        print("=" * 50)
        print(f"PROCESS FILE - Instance [{map_index}]")
        print("=" * 50)
        print()

        print(f"📄 Processing: {filename}")

        # Simulate processing time (variable by file type)
        extension = filename.split(".")[-1]
        base_time = {"csv": 0.1, "json": 0.15, "parquet": 0.2}.get(extension, 0.1)
        time.sleep(base_time)

        # Simulate metadata extraction
        # In production: actually read file, count rows, check schema

        # Size varies by file type
        size_ranges = {
            "csv": (5, 100),
            "json": (1, 50),
            "parquet": (10, 200),
        }
        size_range = size_ranges.get(extension, (1, 50))
        size_mb = round(random.uniform(*size_range), 2)

        # Row count correlates with size
        rows_per_mb = {"csv": 5000, "json": 3000, "parquet": 10000}.get(extension, 5000)
        row_count = int(size_mb * rows_per_mb * random.uniform(0.8, 1.2))

        # Simulate column detection
        columns = random.randint(5, 25)

        result = {
            "filename": filename,
            "extension": extension,
            "size_mb": size_mb,
            "row_count": row_count,
            "columns": columns,
            "map_index": map_index,
            "status": "success",
            "validation": {
                "schema_valid": True,
                "no_nulls_in_pk": True,
                "row_count_expected": True,
            },
        }

        print()
        print("📊 File Metadata:")
        print(f"   Extension: .{extension}")
        print(f"   Size: {size_mb} MB")
        print(f"   Rows: {row_count:,}")
        print(f"   Columns: {columns}")
        print()
        print("✓ Validation passed")
        print("=" * 50)

        return result

    @task
    def generate_report(file_results: list[dict]) -> dict:
        """
        Generate a summary report from all processed files.

        This task receives the output of ALL mapped task instances
        as a single list, allowing comprehensive aggregation.
        """
        print("=" * 60)
        print("GENERATE REPORT - Aggregation Phase")
        print("=" * 60)
        print()

        # Calculate totals
        total_files = len(file_results)
        total_size = sum(f["size_mb"] for f in file_results)
        total_rows = sum(f["row_count"] for f in file_results)
        total_columns = sum(f["columns"] for f in file_results)

        # Count by extension
        ext_counts = {}
        ext_sizes = {}
        ext_rows = {}
        for f in file_results:
            ext = f["extension"]
            ext_counts[ext] = ext_counts.get(ext, 0) + 1
            ext_sizes[ext] = ext_sizes.get(ext, 0) + f["size_mb"]
            ext_rows[ext] = ext_rows.get(ext, 0) + f["row_count"]

        # Find largest and smallest files
        largest = max(file_results, key=lambda x: x["size_mb"])
        smallest = min(file_results, key=lambda x: x["size_mb"])

        report = {
            "summary": {
                "total_files": total_files,
                "total_size_mb": round(total_size, 2),
                "total_rows": total_rows,
                "avg_size_mb": round(total_size / total_files, 2),
                "avg_rows": round(total_rows / total_files),
            },
            "by_extension": {
                ext: {
                    "count": ext_counts[ext],
                    "total_size_mb": round(ext_sizes[ext], 2),
                    "total_rows": ext_rows[ext],
                }
                for ext in ext_counts
            },
            "extremes": {
                "largest_file": largest["filename"],
                "largest_size_mb": largest["size_mb"],
                "smallest_file": smallest["filename"],
                "smallest_size_mb": smallest["size_mb"],
            },
            "validation": {
                "all_passed": all(f["status"] == "success" for f in file_results),
                "failed_count": sum(1 for f in file_results if f["status"] != "success"),
            },
            "status": "complete",
        }

        print("📊 PROCESSING SUMMARY")
        print("=" * 40)
        print()

        print("📁 Overall Statistics:")
        print(f"   Total files processed: {total_files}")
        print(f"   Total data size: {total_size:.2f} MB")
        print(f"   Total rows: {total_rows:,}")
        print(f"   Average file size: {total_size/total_files:.2f} MB")
        print(f"   Average rows/file: {total_rows/total_files:,.0f}")
        print()

        print("📄 By File Type:")
        print("-" * 40)
        for ext, stats in report["by_extension"].items():
            print(f"   .{ext}:")
            print(f"      Files: {stats['count']}")
            print(f"      Size: {stats['total_size_mb']} MB")
            print(f"      Rows: {stats['total_rows']:,}")
        print()

        print("📈 Size Extremes:")
        print(f"   Largest: {largest['filename']} ({largest['size_mb']} MB)")
        print(f"   Smallest: {smallest['filename']} ({smallest['size_mb']} MB)")
        print()

        validation_status = "✅ All files passed" if report["validation"]["all_passed"] else "⚠️ Some files failed"
        print(f"🔍 Validation: {validation_status}")
        print()

        print("=" * 60)
        print("✅ FILE PROCESSING PIPELINE COMPLETE")
        print("=" * 60)

        return report

    # =====================================================================
    # WIRE UP THE PIPELINE
    # =====================================================================

    # Step 1: Discover files
    files = list_files()

    # Step 2: Process each file in parallel
    processed = process_file.expand(filename=files)

    # Step 3: Aggregate results
    generate_report(processed)


# Instantiate the DAG
file_processing_dag()
