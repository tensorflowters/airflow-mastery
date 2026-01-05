# Exercise 16.2: Ingestion Pipeline

Build the document ingestion DAG with deferrable sensors, dynamic task mapping, and robust error handling patterns.

## Learning Goals

- Implement deferrable sensors for resource-efficient file monitoring
- Use dynamic task mapping for parallel file processing
- Create Asset outlets to trigger downstream DAGs
- Apply dead-letter queue patterns for failed document handling
- Configure exponential backoff retry strategies

## Scenario

Your document processing platform needs to continuously monitor multiple S3 buckets for new documents. When files arrive, the ingestion pipeline should:

1. **Detect new files** using deferrable sensors (releasing workers during wait)
2. **Process files in parallel** using dynamic task mapping
3. **Emit Asset events** to trigger the processing pipeline
4. **Handle failures gracefully** with dead-letter queues for retry

This is the first stage of a multi-DAG pipeline where data-aware scheduling connects ingestion to downstream processing.

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION PIPELINE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌──────────────────┐         ┌──────────────────┐                         │
│   │  Deferrable S3   │────────▶│  List New Files  │                         │
│   │     Sensor       │         │    (discover)    │                         │
│   │  (releases       │         └────────┬─────────┘                         │
│   │   worker)        │                  │                                   │
│   └──────────────────┘                  ▼                                   │
│                              ┌──────────────────┐                           │
│                              │   Dynamic Task   │                           │
│                              │     Mapping      │                           │
│                              │  .expand(file=   │                           │
│                              │    file_list)    │                           │
│                              └────────┬─────────┘                           │
│                                       │                                     │
│              ┌────────────────────────┼────────────────────────┐            │
│              ▼                        ▼                        ▼            │
│   ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐   │
│   │  Process File 1  │     │  Process File 2  │     │  Process File N  │   │
│   │   (validate,     │     │   (validate,     │     │   (validate,     │   │
│   │    parse, emit)  │     │    parse, emit)  │     │    parse, emit)  │   │
│   └────────┬─────────┘     └────────┬─────────┘     └────────┬─────────┘   │
│            │                        │                        │              │
│            │        ┌───────────────┴───────────────┐       │              │
│            │        ▼                               ▼       │              │
│            │    ┌────────┐                     ┌────────┐   │              │
│            │    │Success │                     │ Failed │   │              │
│            │    └───┬────┘                     └───┬────┘   │              │
│            │        │                              │        │              │
│            ▼        ▼                              ▼        ▼              │
│   ┌──────────────────────┐              ┌──────────────────────┐           │
│   │   Asset: documents.  │              │   Dead Letter Queue  │           │
│   │   raw (outlet)       │              │   (retry later)      │           │
│   └──────────────────────┘              └──────────────────────┘           │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Requirements

### Task 1: Create Deferrable FileSensor for S3 Monitoring

Create a deferrable sensor that monitors an S3 bucket prefix for new files:

```python
# sensors/deferrable_s3_sensor.py
from datetime import timedelta
from typing import Any

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.triggers.s3 import S3KeyTrigger


class DeferrableS3FileSensor(S3KeySensor):
    """
    Deferrable sensor that waits for files in S3 bucket.

    Releases the worker slot while waiting, resuming only when
    files are detected or timeout is reached.
    """

    def __init__(
        self,
        bucket_name: str,
        prefix: str,
        aws_conn_id: str = "s3_default",
        wildcard_match: bool = True,
        poke_interval: int = 60,
        timeout: int = 3600,
        deferrable: bool = True,
        **kwargs,
    ):
        super().__init__(
            bucket_key=f"{prefix}*" if wildcard_match else prefix,
            bucket_name=bucket_name,
            aws_conn_id=aws_conn_id,
            poke_interval=poke_interval,
            timeout=timeout,
            deferrable=deferrable,
            **kwargs,
        )
        self.prefix = prefix

    def execute(self, context: dict) -> Any:
        """Defer immediately to release worker."""
        if not self.deferrable:
            return super().execute(context)

        self.defer(
            trigger=S3KeyTrigger(
                bucket_name=self.bucket_name,
                bucket_key=self.bucket_key,
                aws_conn_id=self.aws_conn_id,
                poke_interval=self.poke_interval,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.timeout),
        )

    def execute_complete(self, context: dict, event: dict) -> str:
        """Handle trigger completion."""
        if event.get("status") == "success":
            return event.get("message", "Files detected")
        raise Exception(f"Sensor failed: {event}")
```

**Acceptance Criteria:**

- Sensor defers immediately upon execution
- Worker slot is released during the wait period
- Sensor resumes when files are detected
- Proper timeout handling with descriptive error messages

### Task 2: Implement Dynamic Task Mapping for Parallel Processing

Create a dynamic task group that processes discovered files in parallel:

```python
# dags/ingestion/document_ingestion.py
from datetime import datetime, timedelta

from airflow.sdk.definitions.decorators import dag, task


@dag(
    dag_id="document_ingestion",
    schedule="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "ingestion-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["capstone", "ingestion"],
)
def document_ingestion():
    """
    Ingest documents from S3 bucket with parallel processing.

    This DAG:
    1. Waits for new files using a deferrable sensor
    2. Lists all new files in the bucket prefix
    3. Processes each file in parallel using dynamic task mapping
    4. Emits Asset events for successfully processed documents
    5. Routes failed documents to dead-letter queue
    """

    @task
    def list_new_files(bucket: str, prefix: str) -> list[dict]:
        """
        List new files that haven't been processed yet.

        Returns a list of file metadata dictionaries for dynamic mapping.
        """
        from airflow.models import Variable
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="s3_default")

        # Get list of already processed files
        processed_key = f"ingestion_processed_{prefix.replace('/', '_')}"
        processed_files = set(Variable.get(processed_key, default_var=[], deserialize_json=True))

        # List current files in bucket
        keys = hook.list_keys(bucket_name=bucket, prefix=prefix)

        # Filter to new files only
        new_files = []
        for key in keys or []:
            if key not in processed_files and not key.endswith("/"):
                metadata = hook.head_object(key=key, bucket_name=bucket)
                new_files.append(
                    {
                        "bucket": bucket,
                        "key": key,
                        "size": metadata.get("ContentLength", 0),
                        "last_modified": str(metadata.get("LastModified", "")),
                        "content_type": metadata.get("ContentType", "unknown"),
                    }
                )

        return new_files

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
    )
    def process_file(file_info: dict) -> dict:
        """
        Process a single file: validate, parse, and prepare for downstream.

        Returns processed document metadata or raises exception for DLQ routing.
        """
        import hashlib

        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="s3_default")

        # Download file content
        content = hook.read_key(key=file_info["key"], bucket_name=file_info["bucket"])

        # Validate content
        if not content or len(content) == 0:
            raise ValueError(f"Empty file: {file_info['key']}")

        # Generate content hash for deduplication
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        # Parse based on content type
        parsed_content = parse_document(content, file_info["content_type"])

        return {
            "source_bucket": file_info["bucket"],
            "source_key": file_info["key"],
            "content_hash": content_hash,
            "document_type": file_info["content_type"],
            "size_bytes": file_info["size"],
            "chunk_count": len(parsed_content.get("chunks", [])),
            "metadata": parsed_content.get("metadata", {}),
            "processed_at": datetime.utcnow().isoformat(),
            "status": "success",
        }

    # Dynamic task mapping - process all files in parallel
    files = list_new_files(bucket="capstone-documents", prefix="incoming/")

    # Use expand() for parallel processing
    processed = process_file.expand(file_info=files)

    # Continue with downstream tasks...
```

**Acceptance Criteria:**

- Dynamic mapping handles 0 to N files gracefully
- Each file is processed independently in parallel
- Failed file processing doesn't affect other files
- Processed files are tracked to avoid reprocessing

### Task 3: Create Asset Outlet for Processed Documents

Add Asset emission to signal successful document ingestion:

```python
@task(outlets=[DOCUMENTS_RAW])
def emit_asset_event(processed_documents: list[dict]) -> dict:
    """
    Emit Asset event to trigger downstream processing DAG.

    Filters successful documents and creates aggregated event metadata.
    """
    successful = [doc for doc in processed_documents if doc.get("status") == "success"]
    failed = [doc for doc in processed_documents if doc.get("status") != "success"]

    event_metadata = {
        "batch_id": f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        "document_count": len(successful),
        "failed_count": len(failed),
        "total_chunks": sum(doc.get("chunk_count", 0) for doc in successful),
        "source_keys": [doc["source_key"] for doc in successful],
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Asset event is emitted automatically when task succeeds
    # Downstream DAGs scheduled on Asset("documents.raw") will trigger

    return event_metadata
```

Configure the downstream DAG to consume this Asset:

```python
# dags/processing/document_processing.py
from airflow.sdk.definitions.decorators import dag
from assets import DOCUMENTS_RAW


@dag(
    dag_id="document_processing",
    schedule=DOCUMENTS_RAW,  # Triggered by Asset
    catchup=False,
    tags=["capstone", "processing"],
)
def document_processing():
    """
    Process documents triggered by ingestion Asset events.

    Automatically runs when documents.raw Asset is updated.
    """
    # Processing implementation in Exercise 16.3
    ...
```

**Acceptance Criteria:**

- Asset event includes batch metadata for downstream tracking
- Downstream DAG triggers automatically on Asset update
- Zero successful documents results in no Asset event (empty batch handling)

### Task 4: Add Dead-Letter Queue Pattern for Failed Documents

Implement error routing for documents that fail processing:

```python
@task
def route_to_dlq(file_info: dict, error: str) -> dict:
    """
    Route failed document to dead-letter queue for later retry.

    Stores failure metadata in S3 DLQ bucket with error details.
    """
    import json

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id="s3_default")

    dlq_record = {
        "original_file": file_info,
        "error_message": str(error),
        "error_type": type(error).__name__,
        "failed_at": datetime.utcnow().isoformat(),
        "retry_count": file_info.get("retry_count", 0) + 1,
        "max_retries": 3,
    }

    dlq_key = f"dlq/ingestion/{datetime.utcnow().strftime('%Y/%m/%d')}/{file_info['key'].split('/')[-1]}.json"

    hook.load_string(
        string_data=json.dumps(dlq_record, indent=2),
        key=dlq_key,
        bucket_name="capstone-documents",
        replace=True,
    )

    return {"dlq_key": dlq_key, "status": "queued_for_retry"}


@task_group
def process_with_dlq(file_info: dict) -> dict:
    """
    Process file with automatic DLQ routing on failure.

    Uses branching to route failures without stopping the pipeline.
    """

    @task
    def attempt_processing(file_info: dict) -> dict:
        """Attempt to process, capturing any errors."""
        try:
            return process_file(file_info)
        except Exception as e:
            # Return error info instead of raising
            return {
                **file_info,
                "status": "failed",
                "error": str(e),
            }

    @task.branch
    def check_result(result: dict) -> str:
        """Branch based on processing result."""
        if result.get("status") == "success":
            return "process_with_dlq.mark_success"
        return "process_with_dlq.handle_failure"

    @task
    def mark_success(result: dict) -> dict:
        """Mark file as successfully processed."""
        return result

    @task
    def handle_failure(result: dict) -> dict:
        """Route to DLQ."""
        return route_to_dlq(result, result.get("error", "Unknown error"))

    result = attempt_processing(file_info)
    branch = check_result(result)
    success = mark_success(result)
    failure = handle_failure(result)

    branch >> [success, failure]

    return success  # Return successful result for downstream
```

**Acceptance Criteria:**

- Failed documents are stored in DLQ with full error context
- DLQ records include retry count and metadata for reprocessing
- Pipeline continues processing other files after individual failures
- DLQ is organized by date for easy management

### Task 5: Implement Exponential Backoff Retry Strategy

Configure comprehensive retry behavior for the ingestion pipeline:

```python
# config/retry_config.py
from datetime import timedelta

# Retry configuration for different failure scenarios
RETRY_CONFIGS = {
    # Transient failures (network, rate limits)
    "transient": {
        "retries": 5,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
    },
    # Validation failures (might succeed with different parsing)
    "validation": {
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": False,
    },
    # External service failures (S3, etc.)
    "external_service": {
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
}


def get_retry_config(failure_type: str) -> dict:
    """Get retry configuration for a failure type."""
    return RETRY_CONFIGS.get(failure_type, RETRY_CONFIGS["transient"])
```

Apply retry strategies at the task level:

```python
from airflow.sdk.definitions.decorators import task
from retry_config import RETRY_CONFIGS


@task(**RETRY_CONFIGS["external_service"])
def download_from_s3(file_info: dict) -> bytes:
    """
    Download file content with external service retry strategy.

    Retries: 3 times with exponential backoff up to 10 minutes.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id="s3_default")
    return hook.read_key(key=file_info["key"], bucket_name=file_info["bucket"])


@task(**RETRY_CONFIGS["validation"])
def validate_content(content: bytes, expected_type: str) -> dict:
    """
    Validate document content with validation retry strategy.

    Retries: 2 times with fixed 30-second delay.
    """
    if not content:
        raise ValueError("Empty content")

    # Content validation logic
    ...
```

Implement a custom retry callback for logging:

```python
def on_retry_callback(context: dict) -> None:
    """
    Log retry attempts with context for debugging.

    Called each time a task is retried.
    """
    ti = context["task_instance"]
    exception = context.get("exception")

    log_data = {
        "task_id": ti.task_id,
        "try_number": ti.try_number,
        "max_tries": ti.max_tries,
        "exception_type": type(exception).__name__ if exception else None,
        "exception_message": str(exception) if exception else None,
        "next_retry_at": str(ti.next_retry_datetime),
    }

    # Log to structured logging system
    ti.log.warning(f"Task retry: {log_data}")


# Apply callback to tasks
@task(
    retries=3,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
    on_retry_callback=on_retry_callback,
)
def process_with_retry_logging(file_info: dict) -> dict:
    """Process with retry logging enabled."""
    ...
```

**Acceptance Criteria:**

- Different retry strategies for different failure types
- Exponential backoff prevents overwhelming external services
- Retry attempts are logged with full context
- Maximum retry delays prevent infinite waits

## Complete DAG Implementation

Combine all components into the final ingestion DAG:

```python
# dags/ingestion/document_ingestion.py
from datetime import datetime, timedelta

from airflow.sdk.definitions.decorators import dag, task
from assets import DOCUMENTS_RAW
from sensors.deferrable_s3_sensor import DeferrableS3FileSensor


@dag(
    dag_id="document_ingestion",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "ingestion-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
    },
    tags=["capstone", "ingestion"],
    doc_md=__doc__,
)
def document_ingestion():
    """
    Document ingestion pipeline with deferrable sensors and dynamic mapping.

    ## Overview
    Monitors S3 bucket for new documents and processes them in parallel,
    emitting Asset events to trigger downstream processing.

    ## Features
    - Deferrable sensors for efficient resource usage
    - Dynamic task mapping for parallel file processing
    - Dead-letter queue for failed document handling
    - Exponential backoff retry strategies
    """
    # Step 1: Wait for files (deferrable)
    wait_for_files = DeferrableS3FileSensor(
        task_id="wait_for_files",
        bucket_name="capstone-documents",
        prefix="incoming/",
        poke_interval=60,
        timeout=3600,
        mode="reschedule",  # Alternative to deferrable for compatibility
    )

    # Step 2: List new files
    @task
    def list_new_files() -> list[dict]:
        """List files ready for processing."""
        # Implementation from Task 2
        ...

    # Step 3: Process files in parallel with DLQ
    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
    )
    def process_file(file_info: dict) -> dict:
        """Process single file with retries."""
        # Implementation from Task 2
        ...

    # Step 4: Aggregate results
    @task
    def aggregate_results(results: list[dict]) -> dict:
        """Aggregate processing results."""
        successful = [r for r in results if r.get("status") == "success"]
        failed = [r for r in results if r.get("status") != "success"]

        return {
            "total": len(results),
            "successful": len(successful),
            "failed": len(failed),
            "successful_docs": successful,
            "failed_docs": failed,
        }

    # Step 5: Route failures to DLQ
    @task
    def route_failures_to_dlq(aggregated: dict) -> None:
        """Route all failed documents to DLQ."""
        for doc in aggregated.get("failed_docs", []):
            route_to_dlq(doc, doc.get("error", "Unknown"))

    # Step 6: Emit Asset event for successful documents
    @task(outlets=[DOCUMENTS_RAW])
    def emit_asset_event(aggregated: dict) -> dict:
        """Emit Asset event for downstream DAGs."""
        if aggregated["successful"] == 0:
            return {"status": "no_documents", "triggered": False}

        return {
            "batch_id": f"batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "document_count": aggregated["successful"],
            "status": "success",
            "triggered": True,
        }

    # DAG structure
    files = list_new_files()
    wait_for_files >> files

    # Dynamic mapping for parallel processing
    processed = process_file.expand(file_info=files)

    # Aggregate and route
    aggregated = aggregate_results(processed)
    route_failures_to_dlq(aggregated)
    emit_asset_event(aggregated)


document_ingestion()
```

## Success Criteria

- [ ] Deferrable sensor releases worker slot while waiting
- [ ] Dynamic task mapping handles 0, 1, and many files correctly
- [ ] Asset event triggers downstream processing DAG
- [ ] Failed documents are routed to DLQ with full context
- [ ] Exponential backoff prevents API/service overload
- [ ] Pipeline handles partial failures gracefully
- [ ] Processed files are tracked to avoid reprocessing

## Hints

<details>
<summary>Hint 1: Empty File List Handling</summary>

Handle the case when no new files are found:

```python
@task
def list_new_files() -> list[dict]:
    files = discover_files()
    if not files:
        # Return empty list - expand() handles gracefully
        return []
    return files


# Dynamic mapping with empty list creates 0 task instances
processed = process_file.expand(file_info=files)


# Aggregate receives empty list
@task
def aggregate_results(results: list[dict]) -> dict:
    # Handle empty results gracefully
    if not results:
        return {"total": 0, "successful": 0, "failed": 0}
    ...
```

</details>

<details>
<summary>Hint 2: Deferrable vs Reschedule Mode</summary>

Understand the difference for sensor configuration:

```python
# Deferrable mode (Airflow 2.6+)
# - Uses Triggers for async waiting
# - Most efficient for long waits
DeferrableSensor(deferrable=True)

# Reschedule mode (all versions)
# - Releases slot between pokes
# - Good fallback if triggers unavailable
BaseSensor(mode="reschedule", poke_interval=60)

# Poke mode (default)
# - Holds worker slot during wait
# - Only for very short waits
BaseSensor(mode="poke", poke_interval=5)
```

</details>

<details>
<summary>Hint 3: Processing State Tracking</summary>

Track processed files to avoid reprocessing:

```python
from airflow.models import Variable


def mark_as_processed(keys: list[str], prefix: str) -> None:
    """Mark files as processed to avoid reprocessing."""
    var_key = f"processed_{prefix.replace('/', '_')}"

    current = set(Variable.get(var_key, default_var=[], deserialize_json=True))
    current.update(keys)

    # Keep only last 1000 files to limit variable size
    if len(current) > 1000:
        current = set(list(current)[-1000:])

    Variable.set(var_key, list(current), serialize_json=True)
```

</details>

<details>
<summary>Hint 4: Retry Exponential Backoff Formula</summary>

Understand the retry timing:

```python
# Exponential backoff formula:
# delay = min(retry_delay * (2 ** (try_number - 1)), max_retry_delay)

# Example with retry_delay=30s, max_retry_delay=300s:
# Try 1: 30s
# Try 2: 60s
# Try 3: 120s
# Try 4: 240s
# Try 5: 300s (capped at max)


@task(
    retries=5,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=5),
)
def task_with_backoff(): ...
```

</details>

## Files

- **Sensor**: `plugins/sensors/deferrable_s3_sensor.py`
- **DAG**: `dags/ingestion/document_ingestion.py`
- **Config**: `config/retry_config.py`
- **Solution**: `../solutions/dags/ingestion_pipeline.py`

## Estimated Time

4 hours

## Related Modules

- [Module 03: Operators and Hooks](../../03-operators-hooks/README.md) - S3 Hook usage
- [Module 05: Assets and Data-Aware Scheduling](../../05-assets-data-aware/README.md) - Asset outlets
- [Module 06: Dynamic Tasks](../../06-dynamic-tasks/README.md) - Task mapping with expand()
- [Module 11: Sensors and Deferrable Operators](../../11-sensors-deferrable/README.md) - Deferrable patterns

## Next Steps

After completing this exercise:

1. Test the sensor with real S3 files (or LocalStack for local development)
2. Verify dynamic mapping works with varying file counts
3. Confirm Asset events appear in the Airflow UI
4. Proceed to [Exercise 16.3: LLM Processing](exercise_16_3_llm_processing.md)

---

[Previous: Exercise 16.1 - Foundation Setup](exercise_16_1_setup.md) | [Back to Module 16](../README.md) | [Next: Exercise 16.3 - LLM Processing](exercise_16_3_llm_processing.md)
