"""
Exercise 9.1: Production-Ready DAG (Starter)
=============================================

Transform this basic DAG into a production-ready pipeline.

TODO List:
1. Add retry configuration with exponential backoff
2. Add execution timeouts to tasks
3. Add DAG-level timeout
4. Implement failure callback with Slack notification
5. Implement success callback
6. Replace print statements with proper logging
7. Add comprehensive error handling
"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta
import requests
import logging

# TODO: Set up proper logging
# logger = logging.getLogger(__name__)


# =============================================================================
# CALLBACK FUNCTIONS
# =============================================================================

# TODO: Implement failure callback
# def on_task_failure(context):
#     """
#     Called when a task fails.
#
#     Should send a Slack notification with:
#     - DAG ID
#     - Task ID
#     - Execution date
#     - Error message
#     - Link to logs
#     """
#     pass


# TODO: Implement success callback
# def on_dag_success(context):
#     """
#     Called when the entire DAG succeeds.
#
#     Should report:
#     - DAG ID
#     - Execution duration
#     - Summary of processed data
#     """
#     pass


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="exercise_9_1_production_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["exercise", "production"],
    # TODO: Add dagrun_timeout
    # dagrun_timeout=timedelta(hours=2),
    # TODO: Add default_args with on_failure_callback
    # default_args={
    #     "owner": "data-team",
    #     "retries": 2,
    #     "retry_delay": timedelta(minutes=5),
    #     "on_failure_callback": on_task_failure,
    # },
    # TODO: Add on_success_callback for DAG
    # on_success_callback=on_dag_success,
)
def production_etl():
    """
    Production-ready ETL pipeline with proper error handling,
    retries, timeouts, and alerting.
    """

    # TODO: Add retry configuration and timeout
    # @task(
    #     retries=3,
    #     retry_delay=timedelta(minutes=1),
    #     retry_exponential_backoff=True,
    #     max_retry_delay=timedelta(minutes=30),
    #     execution_timeout=timedelta(minutes=10),
    # )
    @task
    def extract():
        """
        Extract data from external API.

        TODO:
        - Add proper logging
        - Add error handling for different error types
        - Add timing information
        """
        # TODO: Use logger instead of print
        print("Starting extraction...")

        # TODO: Add error handling
        response = requests.get(
            "https://jsonplaceholder.typicode.com/posts",
            timeout=30
        )
        response.raise_for_status()
        data = response.json()

        print(f"Extracted {len(data)} records")
        return data

    # TODO: Add timeout
    # @task(execution_timeout=timedelta(minutes=5))
    @task
    def transform(data: list) -> dict:
        """
        Transform extracted data.

        TODO:
        - Add validation
        - Add logging
        - Handle edge cases
        """
        print(f"Transforming {len(data)} records...")

        # TODO: Add validation
        if not data:
            print("No data to transform")
            return {"records": [], "total": 0}

        # Simple transformation
        transformed = []
        total_ids = 0

        for item in data:
            transformed.append({
                "id": item["id"],
                "title_length": len(item.get("title", "")),
                "body_length": len(item.get("body", "")),
            })
            total_ids += item["id"]

        result = {
            "records": transformed,
            "total": total_ids,
            "count": len(transformed),
        }

        print(f"Transformation complete: {len(transformed)} records")
        return result

    # TODO: Add timeout
    # @task(execution_timeout=timedelta(minutes=5))
    @task
    def validate(transformed_data: dict) -> dict:
        """
        Validate transformed data before loading.

        TODO:
        - Add comprehensive validation
        - Log validation results
        - Raise appropriate exceptions for failures
        """
        print("Validating data...")

        records = transformed_data.get("records", [])

        # TODO: Add more validation
        if not records:
            print("Warning: No records to validate")

        # Simple validation
        valid_count = sum(1 for r in records if r["id"] > 0)

        print(f"Validation complete: {valid_count}/{len(records)} valid")

        return {
            **transformed_data,
            "valid_count": valid_count,
            "is_valid": valid_count == len(records),
        }

    # TODO: Add timeout
    # @task(execution_timeout=timedelta(minutes=10))
    @task
    def load(validated_data: dict) -> dict:
        """
        Load validated data to destination.

        TODO:
        - Add proper logging
        - Add error handling for load failures
        - Report load statistics
        """
        print("Loading data...")

        if not validated_data.get("is_valid", False):
            # TODO: Handle invalid data appropriately
            print("Warning: Loading potentially invalid data")

        records = validated_data.get("records", [])

        # Simulate database load
        # In production, this would be actual database operations
        print(f"Simulating load of {len(records)} records...")

        result = {
            "status": "success",
            "records_loaded": len(records),
            "total_processed": validated_data.get("total", 0),
        }

        print(f"Load complete: {result}")
        return result

    # Pipeline execution
    raw_data = extract()
    transformed = transform(raw_data)
    validated = validate(transformed)
    load(validated)


# Instantiate the DAG
production_etl()


# =============================================================================
# LOCAL TESTING
# =============================================================================

if __name__ == "__main__":
    # Test callback context (mock)
    mock_context = {
        "ti": type("TaskInstance", (), {
            "task_id": "test_task",
            "dag_id": "test_dag",
            "log_url": "http://localhost:8080/log",
        })(),
        "dag": type("DAG", (), {"dag_id": "test_dag"})(),
        "logical_date": datetime.now(),
        "exception": Exception("Test error"),
    }

    # TODO: Test your callback functions
    # on_task_failure(mock_context)
    # on_dag_success(mock_context)

    print("Starter code loaded. Implement the TODOs!")
