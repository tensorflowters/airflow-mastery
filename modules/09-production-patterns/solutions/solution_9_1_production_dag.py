"""
Solution 9.1: Production-Ready DAG
===================================

A production-hardened ETL pipeline demonstrating:
1. Retry configuration with exponential backoff
2. Execution timeouts at task and DAG level
3. Failure callbacks with Slack notifications
4. Success callbacks with summary reporting
5. Structured logging with context
6. Comprehensive error handling

Key Production Patterns:
- Idempotent operations
- Graceful degradation
- Observability through logging
- Alerting on failures
"""

from airflow.sdk import dag, task
from airflow.exceptions import AirflowException, AirflowSkipException
from datetime import datetime, timedelta
import requests
import logging
import time

# Set up structured logging
logger = logging.getLogger(__name__)


# =============================================================================
# CALLBACK FUNCTIONS
# =============================================================================


def on_task_failure(context):
    """
    Called when any task in the DAG fails.

    Sends a Slack notification with full context about the failure.
    """
    ti = context.get("ti")
    dag_id = context.get("dag").dag_id
    task_id = ti.task_id
    execution_date = context.get("logical_date")
    exception = context.get("exception")
    log_url = ti.log_url
    try_number = ti.try_number

    # Log the failure
    logger.error(
        "Task failed",
        extra={
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": str(execution_date),
            "try_number": try_number,
            "exception": str(exception),
        }
    )

    # Build Slack message
    slack_message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "❌ Airflow Task Failed",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Task:*\n{task_id}"},
                    {"type": "mrkdwn", "text": f"*Execution Date:*\n{execution_date}"},
                    {"type": "mrkdwn", "text": f"*Attempt:*\n{try_number}"},
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{str(exception)[:500]}```"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "📋 View Logs"},
                        "url": log_url,
                        "style": "primary"
                    }
                ]
            }
        ]
    }

    # Send to Slack (commented out - requires connection setup)
    # try:
    #     from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    #     hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")
    #     hook.send_dict(slack_message)
    # except Exception as e:
    #     logger.error(f"Failed to send Slack alert: {e}")

    # For demo purposes, log the message
    logger.warning(f"Slack alert would be sent: {slack_message['blocks'][0]['text']['text']}")


def on_dag_success(context):
    """
    Called when the entire DAG run completes successfully.

    Sends a summary notification with pipeline statistics.
    """
    dag_id = context.get("dag").dag_id
    dag_run = context.get("dag_run")
    execution_date = context.get("logical_date")

    # Calculate duration
    start_date = dag_run.start_date
    end_date = dag_run.end_date or datetime.now()
    duration = end_date - start_date

    logger.info(
        "DAG completed successfully",
        extra={
            "dag_id": dag_id,
            "execution_date": str(execution_date),
            "duration_seconds": duration.total_seconds(),
        }
    )

    # Build success message
    slack_message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "✅ Pipeline Completed Successfully",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n{duration}"},
                    {"type": "mrkdwn", "text": f"*Execution Date:*\n{execution_date}"},
                ]
            }
        ]
    }

    # For demo purposes
    logger.info(f"Success notification: {dag_id} completed in {duration}")


def on_retry(context):
    """
    Called when a task is about to be retried.

    Useful for logging retry attempts and potentially notifying on multiple retries.
    """
    ti = context.get("ti")
    try_number = ti.try_number
    max_tries = ti.max_tries

    logger.warning(
        "Task retry",
        extra={
            "task_id": ti.task_id,
            "try_number": try_number,
            "max_tries": max_tries,
        }
    )

    # Alert if on last retry
    if try_number >= max_tries - 1:
        logger.error(f"Task {ti.task_id} on final retry attempt!")


# =============================================================================
# CUSTOM EXCEPTIONS
# =============================================================================


class RetryableError(Exception):
    """Errors that should trigger a retry."""
    pass


class NonRetryableError(AirflowException):
    """Errors that should fail immediately without retry."""
    pass


class DataValidationError(AirflowException):
    """Errors related to data validation failures."""
    pass


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="solution_9_1_production_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["solution", "production"],
    description="Production-ready ETL with error handling and alerting",
    # DAG-level timeout
    dagrun_timeout=timedelta(hours=2),
    # DAG-level success callback
    on_success_callback=on_dag_success,
    # Default arguments for all tasks
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": on_task_failure,
        "on_retry_callback": on_retry,
    },
)
def production_etl():
    """
    Production-ready ETL pipeline demonstrating best practices.

    Features:
    - Retry configuration with exponential backoff
    - Task and DAG-level timeouts
    - Failure and success callbacks
    - Structured logging
    - Error categorization
    """

    @task(
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        execution_timeout=timedelta(minutes=10),
    )
    def extract() -> list:
        """
        Extract data from external API with proper error handling.

        Returns:
            list: Raw data from the API

        Raises:
            RetryableError: For transient errors (connection, timeout)
            NonRetryableError: For permanent errors (auth, not found)
        """
        start_time = time.time()
        logger.info("Starting data extraction")

        try:
            response = requests.get(
                "https://jsonplaceholder.typicode.com/posts",
                timeout=30,
                headers={"Accept": "application/json"}
            )

            # Handle HTTP errors
            if response.status_code == 401:
                raise NonRetryableError("Authentication failed - check credentials")
            elif response.status_code == 404:
                raise NonRetryableError("API endpoint not found")
            elif response.status_code == 429:
                raise RetryableError("Rate limited - will retry")
            elif response.status_code >= 500:
                raise RetryableError(f"Server error: {response.status_code}")

            response.raise_for_status()
            data = response.json()

            # Log success metrics
            duration = time.time() - start_time
            logger.info(
                "Extraction completed",
                extra={
                    "record_count": len(data),
                    "duration_seconds": round(duration, 2),
                    "status_code": response.status_code,
                }
            )

            return data

        except requests.exceptions.Timeout:
            logger.warning("Request timed out, will retry")
            raise RetryableError("API request timed out")
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error: {e}")
            raise RetryableError(f"Connection error: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Unexpected request error: {e}")
            raise

    @task(execution_timeout=timedelta(minutes=5))
    def transform(data: list) -> dict:
        """
        Transform extracted data with validation.

        Args:
            data: Raw data from extraction

        Returns:
            dict: Transformed data with metadata
        """
        start_time = time.time()
        logger.info(f"Starting transformation of {len(data)} records")

        if not data:
            logger.warning("No data received for transformation")
            return {
                "records": [],
                "count": 0,
                "total_id_sum": 0,
                "avg_title_length": 0,
            }

        transformed = []
        total_id_sum = 0
        total_title_length = 0
        errors = []

        for idx, item in enumerate(data):
            try:
                # Validate required fields
                if "id" not in item:
                    errors.append(f"Record {idx}: missing 'id' field")
                    continue

                record = {
                    "id": item["id"],
                    "title_length": len(item.get("title", "")),
                    "body_length": len(item.get("body", "")),
                    "user_id": item.get("userId"),
                }

                transformed.append(record)
                total_id_sum += item["id"]
                total_title_length += record["title_length"]

            except Exception as e:
                errors.append(f"Record {idx}: {str(e)}")
                continue

        # Log transformation results
        duration = time.time() - start_time
        logger.info(
            "Transformation completed",
            extra={
                "input_count": len(data),
                "output_count": len(transformed),
                "error_count": len(errors),
                "duration_seconds": round(duration, 2),
            }
        )

        if errors:
            logger.warning(f"Transformation had {len(errors)} errors: {errors[:5]}")

        return {
            "records": transformed,
            "count": len(transformed),
            "total_id_sum": total_id_sum,
            "avg_title_length": round(total_title_length / len(transformed), 2) if transformed else 0,
            "error_count": len(errors),
        }

    @task(execution_timeout=timedelta(minutes=5))
    def validate(transformed_data: dict) -> dict:
        """
        Validate transformed data before loading.

        Raises:
            DataValidationError: If validation fails critically
            AirflowSkipException: If no data to process
        """
        logger.info("Starting data validation")

        records = transformed_data.get("records", [])

        # Check for empty data
        if not records:
            logger.warning("No records to validate - skipping load")
            raise AirflowSkipException("No data to process")

        # Validation checks
        validation_results = {
            "total_records": len(records),
            "valid_records": 0,
            "invalid_records": 0,
            "issues": [],
        }

        for record in records:
            is_valid = True

            # Check ID is positive
            if record.get("id", 0) <= 0:
                is_valid = False
                validation_results["issues"].append(f"Invalid ID: {record.get('id')}")

            # Check title length is reasonable
            if record.get("title_length", 0) > 1000:
                is_valid = False
                validation_results["issues"].append(f"Title too long for ID {record.get('id')}")

            if is_valid:
                validation_results["valid_records"] += 1
            else:
                validation_results["invalid_records"] += 1

        # Calculate validation score
        validation_score = validation_results["valid_records"] / validation_results["total_records"]
        validation_results["validation_score"] = round(validation_score, 4)

        logger.info(
            "Validation completed",
            extra={
                "validation_score": validation_score,
                "valid_records": validation_results["valid_records"],
                "invalid_records": validation_results["invalid_records"],
            }
        )

        # Fail if too many invalid records
        if validation_score < 0.9:
            raise DataValidationError(
                f"Validation failed: only {validation_score:.1%} valid records"
            )

        return {
            **transformed_data,
            "validation": validation_results,
        }

    @task(execution_timeout=timedelta(minutes=10))
    def load(validated_data: dict) -> dict:
        """
        Load validated data to destination (simulated).

        In production, this would write to a database or data warehouse.
        """
        start_time = time.time()
        records = validated_data.get("records", [])

        logger.info(f"Starting load of {len(records)} records")

        # Simulate database load with progress logging
        batch_size = 100
        loaded_count = 0

        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]

            # Simulate batch insert
            time.sleep(0.01)  # Simulate I/O

            loaded_count += len(batch)

            if loaded_count % 500 == 0:
                logger.info(f"Loaded {loaded_count}/{len(records)} records")

        # Compute final statistics
        duration = time.time() - start_time
        records_per_second = len(records) / duration if duration > 0 else 0

        result = {
            "status": "success",
            "records_loaded": loaded_count,
            "duration_seconds": round(duration, 2),
            "records_per_second": round(records_per_second, 2),
            "validation_score": validated_data.get("validation", {}).get("validation_score"),
        }

        logger.info(
            "Load completed successfully",
            extra=result
        )

        return result

    # Pipeline execution flow
    raw_data = extract()
    transformed = transform(raw_data)
    validated = validate(transformed)
    load(validated)


# Instantiate the DAG
production_etl()


# =============================================================================
# TESTING UTILITIES
# =============================================================================

if __name__ == "__main__":
    # Test callback functions with mock context
    from unittest.mock import MagicMock

    mock_ti = MagicMock()
    mock_ti.task_id = "test_task"
    mock_ti.dag_id = "test_dag"
    mock_ti.log_url = "http://localhost:8080/log/test"
    mock_ti.try_number = 2
    mock_ti.max_tries = 3

    mock_dag = MagicMock()
    mock_dag.dag_id = "test_dag"

    mock_dag_run = MagicMock()
    mock_dag_run.start_date = datetime.now() - timedelta(minutes=5)
    mock_dag_run.end_date = datetime.now()

    mock_context = {
        "ti": mock_ti,
        "dag": mock_dag,
        "dag_run": mock_dag_run,
        "logical_date": datetime.now(),
        "exception": Exception("Test error for demonstration"),
    }

    print("Testing callbacks...")
    print("\n=== Testing on_task_failure ===")
    on_task_failure(mock_context)

    print("\n=== Testing on_dag_success ===")
    on_dag_success(mock_context)

    print("\n=== Testing on_retry ===")
    on_retry(mock_context)

    print("\nCallback tests complete!")
