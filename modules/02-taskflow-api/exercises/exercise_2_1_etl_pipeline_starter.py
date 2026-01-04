"""
Exercise 2.1: ETL Pipeline with TaskFlow API
============================================

Build a multi-source ETL pipeline using TaskFlow patterns.

Requirements:
- Extract from 2 sources (users and orders)
- Transform each source separately
- Combine transformed data
- Load to destination (print)
- Use retries on extract tasks
- Use multiple_outputs where appropriate
- Include type hints
"""

from datetime import datetime, timedelta

# TODO: Import dag and task from airflow.sdk
# from airflow.sdk import ...


# TODO: Create the DAG using @dag decorator
# @dag(
#     dag_id="exercise_2_1_etl_pipeline",
#     start_date=...,
#     schedule=...,
#     catchup=...,
#     tags=[...],
# )
def etl_pipeline():
    """ETL Pipeline demonstrating TaskFlow patterns."""

    # =========================================================================
    # EXTRACT LAYER
    # =========================================================================

    # TODO: Create extract_users task
    # Requirements:
    # - retries=2, retry_delay=timedelta(seconds=30)
    # - Return type hint: dict
    # - Returns: {"users": [{"id": 1, "name": "Alice", "email": "alice@example.com"},
    #                       {"id": 2, "name": "Bob", "email": "bob@example.com"}]}

    # @task(...)
    # def extract_users() -> dict:
    #     pass

    # TODO: Create extract_orders task
    # Requirements:
    # - retries=2, retry_delay=timedelta(seconds=30)
    # - Return type hint: dict
    # - Returns: {"orders": [{"id": 101, "user_id": 1, "amount": 99.99},
    #                        {"id": 102, "user_id": 2, "amount": 149.99},
    #                        {"id": 103, "user_id": 1, "amount": 49.99}]}

    # @task(...)
    # def extract_orders() -> dict:
    #     pass

    # =========================================================================
    # TRANSFORM LAYER
    # =========================================================================

    # TODO: Create transform_users task
    # Requirements:
    # - Takes dict parameter with type hint
    # - Adds "processed": True to each user
    # - Returns dict with transformed users

    # @task
    # def transform_users(data: dict) -> dict:
    #     pass

    # TODO: Create transform_orders task
    # Requirements:
    # - Takes dict parameter with type hint
    # - Use multiple_outputs=True
    # - Calculate total amount across all orders
    # - Return {"orders": [...], "total": <sum>}

    # @task(multiple_outputs=True)
    # def transform_orders(data: dict) -> dict:
    #     pass

    # =========================================================================
    # COMBINE LAYER
    # =========================================================================

    # TODO: Create combine_data task
    # Requirements:
    # - Takes transformed users (dict) and orders info (dict with "orders" and "total")
    # - Creates summary: user_count, order_count, total_revenue
    # - Returns combined summary dict

    # @task
    # def combine_data(users: dict, orders_data: dict) -> dict:
    #     pass

    # =========================================================================
    # LOAD LAYER
    # =========================================================================

    # TODO: Create load_to_destination task
    # Requirements:
    # - Takes the combined summary
    # - Prints it in a formatted way
    # - Returns metadata: {"status": "success", "loaded_at": <timestamp>}

    # @task
    # def load_to_destination(summary: dict) -> dict:
    #     pass

    # =========================================================================
    # TASK DEPENDENCIES (Wire it up!)
    # =========================================================================

    # TODO: Call the tasks and create the pipeline
    # Step 1: Extract (parallel)
    # users_raw = extract_users()
    # orders_raw = extract_orders()

    # Step 2: Transform (parallel, waiting on respective extracts)
    # users_transformed = transform_users(users_raw)
    # orders_transformed = transform_orders(orders_raw)

    # Step 3: Combine (waits for both transforms)
    # For multiple_outputs, access like: orders_transformed["orders"]
    # combined = combine_data(users_transformed, orders_transformed)

    # Step 4: Load
    # load_to_destination(combined)

    pass  # Remove this when you add your implementation


# TODO: Instantiate the DAG
# etl_pipeline()
