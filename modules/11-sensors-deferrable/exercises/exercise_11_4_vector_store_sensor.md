# Exercise 11.4: Vector Store Sensor

## Objective

Build a deferrable sensor that waits for vector store indexing operations to complete, demonstrating async patterns for AI/ML pipelines.

## Background

Vector store operations in AI/ML pipelines often involve:

| Operation           | Duration         | Pattern           |
| ------------------- | ---------------- | ----------------- |
| Embedding ingestion | Minutes to hours | Async polling     |
| Index building      | Minutes to hours | Deferrable sensor |
| Collection creation | Seconds          | Quick check       |
| Health checks       | Milliseconds     | Sync validation   |

Traditional sensors holding worker slots for hours is wasteful. Deferrable sensors release the worker and use the triggerer service for efficient async waiting.

## Requirements

### Task 1: Vector Store Status Trigger

Create a custom trigger that:

- Polls vector store indexing status asynchronously
- Supports multiple vector store backends (simulated)
- Handles various status states (indexing, ready, failed)
- Times out gracefully with informative errors

### Task 2: Deferrable Vector Store Sensor

Implement a sensor that:

- Defers to custom trigger for async waiting
- Validates collection exists before waiting
- Provides progress information when available
- Handles different failure modes

### Task 3: Index Health Check

Add validation that:

- Verifies index is queryable after creation
- Checks document count matches expected
- Reports index statistics (dimensions, document count)

### Task 4: Pipeline Integration

Create a DAG that:

- Ingests documents to vector store
- Uses sensor to wait for indexing
- Validates index health
- Runs sample queries to verify

## Starter Code

See `exercise_11_4_vector_store_sensor_starter.py`

## Hints

<details>
<summary>Hint 1: Custom Trigger Structure</summary>

```python
from airflow.triggers.base import BaseTrigger, TriggerEvent


class VectorStoreIndexTrigger(BaseTrigger):
    """Trigger that waits for vector store indexing to complete."""

    def __init__(
        self,
        collection_name: str,
        expected_count: int,
        poll_interval: float = 30.0,
        timeout: float = 3600.0,
    ):
        super().__init__()
        self.collection_name = collection_name
        self.expected_count = expected_count
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict]:
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "collection_name": self.collection_name,
                "expected_count": self.expected_count,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        # Poll for indexing completion
        ...
```

</details>

<details>
<summary>Hint 2: Deferrable Sensor Pattern</summary>

```python
from airflow.sensors.base import BaseSensorOperator


class VectorStoreSensor(BaseSensorOperator):
    """Sensor that waits for vector store to be ready."""

    def __init__(
        self,
        collection_name: str,
        expected_count: int = 0,
        deferrable: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.expected_count = expected_count
        self.deferrable = deferrable

    def execute(self, context: Context) -> None:
        # Quick check first
        if self._check_ready():
            return

        # Defer to trigger for async waiting
        if self.deferrable:
            self.defer(
                trigger=VectorStoreIndexTrigger(
                    collection_name=self.collection_name,
                    expected_count=self.expected_count,
                ),
                method_name="execute_complete",
            )
        else:
            # Fallback to poke mode
            super().execute(context)

    def execute_complete(self, context: Context, event: dict) -> None:
        """Called when trigger fires."""
        if event.get("status") == "success":
            self.log.info(f"Vector store ready: {event}")
        else:
            raise AirflowException(f"Vector store error: {event}")
```

</details>

<details>
<summary>Hint 3: Async Status Polling</summary>

```python
import asyncio
from datetime import datetime, timedelta


async def run(self) -> AsyncIterator[TriggerEvent]:
    start_time = datetime.utcnow()
    timeout_at = start_time + timedelta(seconds=self.timeout)

    while datetime.utcnow() < timeout_at:
        # Check status asynchronously
        status = await self._check_status()

        if status["state"] == "ready":
            yield TriggerEvent(
                {
                    "status": "success",
                    "collection": self.collection_name,
                    "document_count": status["count"],
                }
            )
            return

        if status["state"] == "failed":
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": status.get("error", "Indexing failed"),
                }
            )
            return

        # Log progress
        self.log.info(f"Indexing in progress: {status.get('progress', 'unknown')}")

        # Wait before next poll
        await asyncio.sleep(self.poll_interval)

    # Timeout
    yield TriggerEvent(
        {
            "status": "timeout",
            "message": f"Indexing did not complete within {self.timeout}s",
        }
    )
```

</details>

<details>
<summary>Hint 4: Health Check Validation</summary>

```python
@task
def validate_index_health(collection_name: str, expected_count: int) -> dict:
    """Validate vector store index is healthy and queryable."""
    # Get collection stats
    stats = get_collection_stats(collection_name)

    # Validate document count
    if stats["document_count"] < expected_count:
        raise AirflowException(f"Document count {stats['document_count']} < expected {expected_count}")

    # Run test query
    test_embedding = generate_test_embedding()
    results = query_collection(collection_name, test_embedding, k=3)

    if not results:
        raise AirflowException("Test query returned no results")

    return {
        "collection": collection_name,
        "document_count": stats["document_count"],
        "dimensions": stats["dimensions"],
        "test_query_results": len(results),
        "healthy": True,
    }
```

</details>

## Success Criteria

- [ ] Custom trigger polls asynchronously without blocking workers
- [ ] Sensor defers properly to triggerer service
- [ ] Timeout and error handling work correctly
- [ ] Health check validates index is queryable
- [ ] Progress logging provides visibility during long waits
- [ ] Integration test runs end-to-end successfully

## Files

- **Starter**: `exercise_11_4_vector_store_sensor_starter.py`
- **Solution**: `../solutions/solution_11_4_vector_store_sensor.py`

## Estimated Time

60-90 minutes

## Prerequisites

- Understanding of async/await patterns
- Familiarity with Airflow triggers (Exercise 11.3)
- Basic knowledge of vector databases

---

[← Exercise 11.3](exercise_11_3_custom_trigger.md) | [Module 15: AI/ML Orchestration →](../../15-ai-ml-orchestration/)
