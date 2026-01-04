"""
Solution 4.3: Parameterized DAG
===============================

Complete solution demonstrating:
- Typed parameters with Param class
- Required vs optional parameters
- Enum constraints for mode selection
- Nullable integer for optional limit
- Parameter validation and usage
"""

from datetime import datetime
from airflow.sdk import dag, task
from airflow.sdk.definitions.param import Param


@dag(
    dag_id="solution_4_3_parameterized_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-04", "params"],
    description="Demonstrates parameterized DAG configuration",
    params={
        # Required: source table name
        "source_table": Param(
            type="string",
            description="Source table to read data from (required)",
            minLength=1,
        ),
        # Required: target table name
        "target_table": Param(
            type="string",
            description="Target table to write data to (required)",
            minLength=1,
        ),
        # Optional: write mode with enum constraint
        "mode": Param(
            default="append",
            type="string",
            enum=["append", "overwrite"],
            description="Write mode: 'append' adds rows, 'overwrite' replaces table",
        ),
        # Optional: row limit (nullable integer)
        "limit": Param(
            default=None,
            type=["null", "integer"],
            minimum=1,
            description="Maximum rows to process (null = no limit)",
        ),
    },
)
def parameterized_dag():
    """
    Parameterized Data Processing Pipeline.

    This DAG demonstrates runtime parameterization, allowing users
    to customize the pipeline behavior without code changes.

    Parameters:
        source_table (str): Required. Source table to read from.
        target_table (str): Required. Target table to write to.
        mode (str): Optional. 'append' or 'overwrite'. Default: 'append'.
        limit (int|None): Optional. Row limit. Default: None (no limit).

    Example trigger configuration:
        {
            "source_table": "raw_events",
            "target_table": "processed_events",
            "mode": "overwrite",
            "limit": 1000
        }
    """

    @task
    def validate_params(**context) -> dict:
        """
        Validate all parameters and return configuration.

        This task:
        1. Extracts parameters from context
        2. Performs custom validation logic
        3. Returns a clean configuration dict
        """
        params = context["params"]

        source_table = params["source_table"]
        target_table = params["target_table"]
        mode = params["mode"]
        limit = params.get("limit")

        print("=" * 60)
        print("PARAMETER VALIDATION")
        print("=" * 60)
        print()
        print("Parameters received:")
        print(f"  📥 source_table: {source_table}")
        print(f"  📤 target_table: {target_table}")
        print(f"  📝 mode: {mode}")
        print(f"  🔢 limit: {limit if limit else 'No limit (all rows)'}")
        print()

        # Custom validation: source and target shouldn't be the same
        if source_table == target_table:
            raise ValueError(
                f"Source and target tables cannot be the same! "
                f"Both are set to '{source_table}'"
            )

        # Validation passed
        print("✅ All parameters validated successfully!")
        print()
        print("Configuration summary:")
        print(f"  Will read from: {source_table}")
        print(f"  Will write to: {target_table}")
        print(f"  Write mode: {mode}")
        if limit:
            print(f"  Row limit: {limit:,} rows maximum")
        else:
            print("  Row limit: No limit (process all rows)")
        print("=" * 60)

        return {
            "source_table": source_table,
            "target_table": target_table,
            "mode": mode,
            "limit": limit,
        }

    @task
    def extract_data(config: dict) -> dict:
        """
        Simulate extracting data from the source table.

        In production, this would:
        - Connect to database/data warehouse
        - Execute SELECT query with optional LIMIT
        - Return data or write to intermediate storage
        """
        source = config["source_table"]
        limit = config["limit"]

        print("=" * 60)
        print("EXTRACT PHASE")
        print("=" * 60)
        print()

        # Simulate source table statistics
        total_rows_in_source = 10000
        print(f"📊 Source table: {source}")
        print(f"   Total rows available: {total_rows_in_source:,}")
        print()

        # Determine rows to extract
        if limit:
            rows_to_extract = min(limit, total_rows_in_source)
            print(f"🔢 Limit specified: {limit:,}")
            print(f"   Rows to extract: {rows_to_extract:,}")
        else:
            rows_to_extract = total_rows_in_source
            print("🔢 No limit specified - extracting all rows")
            print(f"   Rows to extract: {rows_to_extract:,}")

        print()
        print("Simulated SQL:")
        if limit:
            print(f"  SELECT * FROM {source} LIMIT {limit}")
        else:
            print(f"  SELECT * FROM {source}")
        print()

        # Simulate extracted data structure
        data = {
            "source": source,
            "row_count": rows_to_extract,
            "columns": ["id", "name", "value", "created_at", "updated_at"],
            "schema": {
                "id": "INTEGER",
                "name": "VARCHAR(255)",
                "value": "DECIMAL(10,2)",
                "created_at": "TIMESTAMP",
                "updated_at": "TIMESTAMP",
            },
        }

        print(f"✅ Extracted {rows_to_extract:,} rows")
        print(f"   Columns: {', '.join(data['columns'])}")
        print("=" * 60)

        return data

    @task
    def transform_data(data: dict) -> dict:
        """
        Simulate transforming the extracted data.

        In production, this would:
        - Clean and validate data
        - Apply business logic transformations
        - Handle null values
        - Create derived fields
        """
        print("=" * 60)
        print("TRANSFORM PHASE")
        print("=" * 60)
        print()

        row_count = data["row_count"]
        source_columns = data["columns"]

        print(f"📊 Input: {row_count:,} rows, {len(source_columns)} columns")
        print()

        print("🔄 Transformations applied:")
        transformations = [
            ("clean_nulls", "Replaced NULL values with defaults"),
            ("normalize_timestamps", "Converted to UTC timezone"),
            ("validate_values", "Checked value ranges and constraints"),
            ("add_derived_fields", "Calculated processing_date and row_hash"),
        ]

        for transform_id, description in transformations:
            print(f"   • {transform_id}: {description}")

        print()

        # Add derived columns
        new_columns = source_columns + ["processing_date", "row_hash"]

        transformed = {
            **data,
            "columns": new_columns,
            "transformations_applied": [t[0] for t in transformations],
        }

        print(f"✅ Transformed {row_count:,} rows")
        print(f"   Output columns: {len(new_columns)}")
        print(f"   New columns: processing_date, row_hash")
        print("=" * 60)

        return transformed

    @task
    def load_data(data: dict, config: dict) -> dict:
        """
        Simulate loading data to the target table.

        In production, this would:
        - Connect to target database
        - Handle append vs overwrite logic
        - Execute INSERT/MERGE statements
        - Return load statistics
        """
        target = config["target_table"]
        mode = config["mode"]
        row_count = data["row_count"]
        columns = data["columns"]

        print("=" * 60)
        print("LOAD PHASE")
        print("=" * 60)
        print()

        print(f"📤 Target table: {target}")
        print(f"📝 Write mode: {mode}")
        print(f"📊 Rows to load: {row_count:,}")
        print(f"📋 Columns: {len(columns)}")
        print()

        if mode == "overwrite":
            print("🗑️  Overwrite mode selected:")
            print(f"   1. TRUNCATE TABLE {target}")
            print(f"   2. INSERT INTO {target} ({row_count:,} rows)")
            print()
            print("Simulated SQL:")
            print(f"   TRUNCATE TABLE {target};")
            print(f"   INSERT INTO {target} SELECT * FROM staging;")
        else:  # append
            print("➕ Append mode selected:")
            print(f"   INSERT INTO {target} ({row_count:,} rows)")
            print()
            print("Simulated SQL:")
            print(f"   INSERT INTO {target} SELECT * FROM staging;")

        print()
        print(f"✅ Successfully loaded {row_count:,} rows to {target}")
        print("=" * 60)

        return {
            "target": target,
            "mode": mode,
            "rows_loaded": row_count,
            "columns_loaded": len(columns),
        }

    @task
    def log_completion(load_result: dict, config: dict) -> dict:
        """
        Log completion details and return pipeline summary.

        This task serves as the final checkpoint, providing:
        - Complete pipeline summary
        - Success confirmation
        - Metadata for downstream systems
        """
        print("=" * 60)
        print("PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print()

        summary = {
            "pipeline": "parameterized_data_pipeline",
            "source_table": config["source_table"],
            "target_table": load_result["target"],
            "write_mode": load_result["mode"],
            "rows_processed": load_result["rows_loaded"],
            "columns_loaded": load_result["columns_loaded"],
            "limit_applied": config["limit"],
            "status": "SUCCESS",
        }

        print("📋 Pipeline Summary:")
        print("-" * 40)
        print(f"   Source: {summary['source_table']}")
        print(f"   Target: {summary['target_table']}")
        print(f"   Mode: {summary['write_mode']}")
        print(f"   Rows: {summary['rows_processed']:,}")
        print(f"   Columns: {summary['columns_loaded']}")
        if summary["limit_applied"]:
            print(f"   Limit: {summary['limit_applied']:,}")
        else:
            print("   Limit: None (all rows)")
        print(f"   Status: ✅ {summary['status']}")
        print()

        print("🎉 Data pipeline completed successfully!")
        print("=" * 60)

        return summary

    # Wire up the pipeline
    config = validate_params()
    extracted = extract_data(config)
    transformed = transform_data(extracted)
    loaded = load_data(transformed, config)
    log_completion(loaded, config)


# Instantiate the DAG
parameterized_dag()
