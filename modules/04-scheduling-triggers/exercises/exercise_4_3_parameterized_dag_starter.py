"""
Exercise 4.3: Parameterized DAG
===============================

Learn to create DAGs that accept runtime parameters.

You'll practice:
- Defining typed parameters with Param
- Setting defaults and validation
- Using enum constraints
- Accessing parameters in tasks
"""

from datetime import datetime

# TODO: Import dag and task from airflow.sdk
# from airflow.sdk import dag, task

# TODO: Import Param for parameter definitions
# from airflow.sdk.definitions.param import Param


# =========================================================================
# DAG DEFINITION
# =========================================================================

# TODO: Add @dag decorator with params
# @dag(
#     dag_id="exercise_4_3_parameterized_dag",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-04", "params"],
#     params={
#         # Required: source table name
#         "source_table": Param(
#             type="string",
#             description="Source table to read data from",
#             minLength=1,
#         ),
#         # Required: target table name
#         "target_table": Param(
#             type="string",
#             description="Target table to write data to",
#             minLength=1,
#         ),
#         # Optional: write mode with enum constraint
#         "mode": Param(
#             default="append",
#             type="string",
#             enum=["append", "overwrite"],
#             description="Write mode: 'append' or 'overwrite'",
#         ),
#         # Optional: row limit (nullable integer)
#         "limit": Param(
#             default=None,
#             type=["null", "integer"],
#             minimum=1,
#             description="Row limit (null = no limit)",
#         ),
#     },
# )
def parameterized_dag():
    """
    Data processing DAG with runtime parameters.

    Parameters:
        source_table (str): Source table to read from
        target_table (str): Target table to write to
        mode (str): Write mode - 'append' or 'overwrite'
        limit (int|None): Optional row limit
    """

    # =====================================================================
    # TASK 1: Validate Parameters
    # =====================================================================

    # TODO: Create validation task
    # @task
    # def validate_params(**context) -> dict:
    #     """Validate all parameters and return configuration."""
    #     params = context["params"]
    #
    #     source_table = params["source_table"]
    #     target_table = params["target_table"]
    #     mode = params["mode"]
    #     limit = params.get("limit")
    #
    #     print("=" * 60)
    #     print("PARAMETER VALIDATION")
    #     print("=" * 60)
    #     print(f"Source Table: {source_table}")
    #     print(f"Target Table: {target_table}")
    #     print(f"Mode: {mode}")
    #     print(f"Limit: {limit if limit else 'No limit'}")
    #     print("=" * 60)
    #
    #     # Validation: source and target shouldn't be same
    #     if source_table == target_table:
    #         raise ValueError("Source and target tables cannot be the same!")
    #
    #     return {
    #         "source_table": source_table,
    #         "target_table": target_table,
    #         "mode": mode,
    #         "limit": limit,
    #     }

    # =====================================================================
    # TASK 2: Extract Data
    # =====================================================================

    # TODO: Create extract task
    # @task
    # def extract_data(config: dict) -> dict:
    #     """Simulate extracting data from source table."""
    #     source = config["source_table"]
    #     limit = config["limit"]
    #
    #     print("=" * 60)
    #     print("EXTRACT PHASE")
    #     print("=" * 60)
    #
    #     # Simulate data extraction
    #     total_rows = 10000  # Pretend source has 10000 rows
    #     rows_to_extract = limit if limit else total_rows
    #
    #     print(f"Reading from: {source}")
    #     print(f"Total rows in source: {total_rows}")
    #     print(f"Rows to extract: {rows_to_extract}")
    #
    #     # Simulate extracted data
    #     data = {
    #         "source": source,
    #         "row_count": rows_to_extract,
    #         "columns": ["id", "name", "value", "timestamp"],
    #     }
    #
    #     print(f"Extracted {rows_to_extract} rows with {len(data['columns'])} columns")
    #     print("=" * 60)
    #
    #     return data

    # =====================================================================
    # TASK 3: Transform Data
    # =====================================================================

    # TODO: Create transform task
    # @task
    # def transform_data(data: dict) -> dict:
    #     """Simulate transforming the extracted data."""
    #     print("=" * 60)
    #     print("TRANSFORM PHASE")
    #     print("=" * 60)
    #
    #     row_count = data["row_count"]
    #
    #     print(f"Processing {row_count} rows...")
    #     print("Transformations applied:")
    #     print("  - Cleaned null values")
    #     print("  - Normalized timestamps")
    #     print("  - Calculated derived fields")
    #
    #     transformed = {
    #         **data,
    #         "columns": data["columns"] + ["derived_field"],
    #         "transformations_applied": [
    #             "clean_nulls",
    #             "normalize_timestamps",
    #             "add_derived_fields",
    #         ],
    #     }
    #
    #     print(f"Output: {row_count} rows with {len(transformed['columns'])} columns")
    #     print("=" * 60)
    #
    #     return transformed

    # =====================================================================
    # TASK 4: Load Data
    # =====================================================================

    # TODO: Create load task
    # @task
    # def load_data(data: dict, config: dict) -> dict:
    #     """Simulate loading data to target table."""
    #     target = config["target_table"]
    #     mode = config["mode"]
    #     row_count = data["row_count"]
    #
    #     print("=" * 60)
    #     print("LOAD PHASE")
    #     print("=" * 60)
    #
    #     print(f"Target table: {target}")
    #     print(f"Write mode: {mode}")
    #     print(f"Rows to load: {row_count}")
    #
    #     if mode == "overwrite":
    #         print(f"  → Truncating {target}...")
    #         print(f"  → Inserting {row_count} rows...")
    #     else:  # append
    #         print(f"  → Appending {row_count} rows to {target}...")
    #
    #     print(f"Successfully loaded {row_count} rows to {target}")
    #     print("=" * 60)
    #
    #     return {
    #         "target": target,
    #         "mode": mode,
    #         "rows_loaded": row_count,
    #     }

    # =====================================================================
    # TASK 5: Log Completion
    # =====================================================================

    # TODO: Create completion logging task
    # @task
    # def log_completion(load_result: dict, config: dict) -> dict:
    #     """Log completion details."""
    #     print("=" * 60)
    #     print("PIPELINE COMPLETED")
    #     print("=" * 60)
    #
    #     summary = {
    #         "source": config["source_table"],
    #         "target": load_result["target"],
    #         "mode": load_result["mode"],
    #         "rows_processed": load_result["rows_loaded"],
    #         "status": "SUCCESS",
    #     }
    #
    #     print("Summary:")
    #     for key, value in summary.items():
    #         print(f"  {key}: {value}")
    #
    #     print("=" * 60)
    #
    #     return summary

    # =====================================================================
    # WIRE UP THE TASKS
    # =====================================================================

    # TODO: Chain the tasks
    # config = validate_params()
    # extracted = extract_data(config)
    # transformed = transform_data(extracted)
    # loaded = load_data(transformed, config)
    # log_completion(loaded, config)

    pass  # Remove when implementing


# TODO: Instantiate the DAG
# parameterized_dag()
