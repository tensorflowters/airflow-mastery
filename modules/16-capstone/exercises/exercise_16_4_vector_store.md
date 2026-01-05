# Exercise 16.4: Vector Store Pipeline

Build the embedding generation and vector store indexing DAG with parallel processing, custom deferrable triggers, and asset-driven orchestration.

## Learning Goals

- Implement parallel embedding generation using dynamic task mapping
- Create batch upsert operations for vector store efficiency
- Build custom deferrable triggers for async indexing operations
- Chain Asset dependencies for multi-DAG orchestration
- Validate index health and search quality

## Background

### Vector Store Patterns and Embeddings

Vector stores enable semantic search by storing document embeddings - dense numerical representations that capture meaning. The indexing pipeline must handle:

1. **Embedding Generation**: Convert text chunks to vectors (parallelizable, GPU-accelerated)
2. **Batch Upserts**: Efficiently insert/update vectors in the store
3. **Index Building**: Background process that optimizes search performance
4. **Health Validation**: Verify index completeness and query accuracy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         VECTOR STORE PIPELINE                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Asset("documents.enriched")                                                 │
│            │                                                                 │
│            ▼                                                                 │
│  ┌──────────────────┐                                                        │
│  │  Load Documents  │                                                        │
│  └────────┬─────────┘                                                        │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────────────────────────────────────────────┐               │
│  │              Parallel Embedding Generation                │               │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐     │               │
│  │  │ Chunk 1 │  │ Chunk 2 │  │ Chunk 3 │  │ Chunk N │     │               │
│  │  │  ───▶   │  │  ───▶   │  │  ───▶   │  │  ───▶   │     │               │
│  │  │ Vector  │  │ Vector  │  │ Vector  │  │ Vector  │     │               │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘     │               │
│  └──────────────────────────────────────────────────────────┘               │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐                                                        │
│  │   Batch Upsert   │                                                        │
│  │  to Vector Store │                                                        │
│  └────────┬─────────┘                                                        │
│           │                                                                  │
│           ▼                                                                  │
│  ┌──────────────────┐     ┌──────────────────────────────┐                  │
│  │   Defer to       │────▶│  VectorStoreIndexingTrigger  │                  │
│  │   Async Index    │     │  (Runs in Triggerer)         │                  │
│  └──────────────────┘     └────────────┬─────────────────┘                  │
│                                        │                                     │
│                                        ▼                                     │
│                           ┌──────────────────┐                              │
│                           │ Validate Index   │                              │
│                           │     Health       │                              │
│                           └────────┬─────────┘                              │
│                                    │                                         │
│                                    ▼                                         │
│                           Asset("documents.indexed")                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Patterns Applied

| Pattern              | Module Reference | Application in This Exercise                  |
| -------------------- | ---------------- | --------------------------------------------- |
| Dynamic task mapping | Module 06        | Parallel embedding generation with `expand()` |
| Deferrable operators | Module 11        | Async indexing with custom trigger            |
| Custom triggers      | Module 11        | `VectorStoreIndexingTrigger` implementation   |
| Asset dependencies   | Module 05        | Chain from enriched to indexed documents      |
| Batch processing     | Module 06        | Efficient vector store upserts                |

## Prerequisites

- Completed Exercise 16.3 (LLM Processing Pipeline)
- Asset `documents.enriched` being produced by processing pipeline
- Vector store connection configured (Pinecone, Weaviate, Qdrant, or mock)
- Embedding model access (OpenAI, Sentence Transformers, or mock)

## Requirements

### Task 1: Parallel Embedding Generation with Dynamic Mapping

Create a pipeline that processes document chunks in parallel:

```python
from airflow.sdk import task


@task
def load_enriched_documents() -> list[dict]:
    """
    Load documents from the enriched asset.

    Returns list of documents, each with 'id', 'content', and 'chunks'.
    """
    pass


@task
def prepare_chunks(documents: list[dict]) -> list[dict]:
    """
    Flatten documents into individual chunks for parallel processing.

    Each chunk includes document_id for reassembly.
    """
    chunks = []
    for doc in documents:
        for i, chunk_text in enumerate(doc["chunks"]):
            chunks.append(
                {
                    "chunk_id": f"{doc['id']}_chunk_{i}",
                    "document_id": doc["id"],
                    "text": chunk_text,
                    "metadata": doc.get("metadata", {}),
                }
            )
    return chunks


@task(
    pool="embedding_api",
    retries=3,
    retry_exponential_backoff=True,
)
def generate_embedding(chunk: dict) -> dict:
    """
    Generate embedding for a single chunk.

    Uses expand() to run in parallel across all chunks.
    """
    embedding = call_embedding_model(chunk["text"])

    return {
        "chunk_id": chunk["chunk_id"],
        "document_id": chunk["document_id"],
        "embedding": embedding,
        "metadata": chunk["metadata"],
    }


# In the DAG:
chunks = prepare_chunks(load_enriched_documents())
embeddings = generate_embedding.expand(chunk=chunks)  # Parallel execution
```

Requirements:

- Use `expand()` for parallel chunk processing
- Configure pool for embedding API rate limiting
- Handle variable-length document chunks
- Preserve chunk-to-document mapping for reassembly

### Task 2: Batch Upsert to Vector Store

Implement efficient batch operations for vector store insertion:

```python
@task
def batch_upsert_vectors(embeddings: list[dict], batch_size: int = 100) -> dict:
    """
    Upsert embeddings to vector store in batches.

    Batching improves throughput and handles rate limits.
    """
    from itertools import islice

    def chunked(iterable, size):
        it = iter(iterable)
        while chunk := list(islice(it, size)):
            yield chunk

    results = {
        "total": len(embeddings),
        "batches": 0,
        "upserted": 0,
        "failed": 0,
    }

    for batch in chunked(embeddings, batch_size):
        try:
            # Upsert batch to vector store
            response = vector_store_client.upsert(
                vectors=[
                    {
                        "id": e["chunk_id"],
                        "values": e["embedding"],
                        "metadata": e["metadata"],
                    }
                    for e in batch
                ]
            )
            results["upserted"] += response["upserted_count"]
            results["batches"] += 1
        except Exception as e:
            logger.error(f"Batch upsert failed: {e}")
            results["failed"] += len(batch)

    return results
```

Requirements:

- Process embeddings in configurable batch sizes
- Track success/failure counts per batch
- Handle partial batch failures gracefully
- Log batch progress for monitoring

### Task 3: Custom VectorStoreIndexingTrigger

Create a deferrable sensor with custom trigger for async indexing:

```python
from collections.abc import AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent


class VectorStoreIndexingTrigger(BaseTrigger):
    """
    Async trigger that waits for vector store indexing to complete.

    Runs in the Triggerer process, freeing up worker slots.
    """

    def __init__(
        self,
        collection_name: str,
        expected_count: int,
        connection_id: str = "vector_store_default",
        poll_interval: float = 30.0,
        timeout: float = 3600.0,
    ):
        super().__init__()
        self.collection_name = collection_name
        self.expected_count = expected_count
        self.connection_id = connection_id
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict]:
        """Serialize trigger for database storage."""
        return (
            "capstone.triggers.VectorStoreIndexingTrigger",
            {
                "collection_name": self.collection_name,
                "expected_count": self.expected_count,
                "connection_id": self.connection_id,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Async generator that polls until indexing complete."""
        import asyncio
        from datetime import datetime, timedelta

        start_time = datetime.utcnow()
        timeout_at = start_time + timedelta(seconds=self.timeout)

        while datetime.utcnow() < timeout_at:
            try:
                status = await self._check_index_status()

                if status["indexed_count"] >= self.expected_count:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "indexed_count": status["indexed_count"],
                            "index_freshness": status.get("freshness_ms", 0),
                        }
                    )
                    return

                self.log.info(f"Indexing progress: {status['indexed_count']}/{self.expected_count}")

            except Exception as e:
                self.log.warning(f"Index check failed: {e}")

            await asyncio.sleep(self.poll_interval)

        yield TriggerEvent(
            {
                "status": "timeout",
                "message": f"Indexing did not complete within {self.timeout}s",
            }
        )

    async def _check_index_status(self) -> dict:
        """Check current index status asynchronously."""
        # Implementation depends on vector store
        # Example for Pinecone-style API
        import aiohttp

        async with aiohttp.ClientSession() as session, session.get(
            f"{self.base_url}/describe_index_stats",
            headers=self.headers,
        ) as response:
            data = await response.json()
            return {
                "indexed_count": data.get("total_vector_count", 0),
                "freshness_ms": data.get("freshness_ms", 0),
            }
```

Requirements:

- Implement `serialize()` for database storage
- Create async `run()` generator that polls index status
- Handle timeout gracefully with clear error messages
- Log progress during polling for visibility

### Task 4: Asset Outlet Triggered by Enriched Asset

Create the DAG with proper asset dependencies:

```python
from airflow.sdk import Asset, dag

ENRICHED_DOCS_ASSET = Asset("documents.enriched")
INDEXED_DOCS_ASSET = Asset("documents.indexed")


@dag(
    dag_id="vector_store_pipeline",
    schedule=ENRICHED_DOCS_ASSET,  # Triggered when enriched docs available
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "vector-store", "embeddings"],
)
def vector_store_pipeline():
    # Load and process documents
    documents = load_enriched_documents()
    chunks = prepare_chunks(documents)

    # Parallel embedding generation
    embeddings = generate_embedding.expand(chunk=chunks)

    # Batch upsert to vector store
    upsert_result = batch_upsert_vectors(embeddings)

    # Wait for indexing (deferrable)
    indexing_complete = wait_for_indexing(upsert_result)

    # Validate and publish
    validate_result = validate_index_health(indexing_complete)

    # Final task with outlet
    publish_indexed = publish_indexed_asset(validate_result)


@task(outlets=[INDEXED_DOCS_ASSET])
def publish_indexed_asset(validation_result: dict) -> dict:
    """
    Publish the indexed asset to signal downstream consumers.
    """
    return {
        "indexed_at": datetime.utcnow().isoformat(),
        "document_count": validation_result["document_count"],
        "vector_count": validation_result["vector_count"],
        "validation_passed": validation_result["passed"],
    }
```

Requirements:

- Schedule DAG on `Asset("documents.enriched")`
- Create `Asset("documents.indexed")` outlet on final task
- Ensure proper task dependencies flow through the pipeline
- Handle the case when upstream asset has no new data

### Task 5: Index Health Validation

Implement comprehensive health checks for the vector index:

```python
@task
def validate_index_health(indexing_result: dict) -> dict:
    """
    Validate vector index health and search quality.

    Checks:
    1. Vector count matches expected
    2. Sample queries return relevant results
    3. Index freshness is acceptable
    """
    validation = {
        "passed": True,
        "checks": [],
        "document_count": 0,
        "vector_count": 0,
    }

    # Check 1: Vector count
    stats = vector_store_client.describe_index_stats()
    validation["vector_count"] = stats["total_vector_count"]
    validation["document_count"] = stats.get("namespaces", {}).get("default", {}).get("vector_count", 0)

    expected_count = indexing_result.get("upserted", 0)
    count_match = validation["vector_count"] >= expected_count
    validation["checks"].append(
        {
            "name": "vector_count",
            "expected": expected_count,
            "actual": validation["vector_count"],
            "passed": count_match,
        }
    )

    if not count_match:
        validation["passed"] = False

    # Check 2: Sample query quality
    sample_queries = get_sample_queries()  # From test data or config
    for query in sample_queries:
        results = vector_store_client.query(
            vector=generate_embedding_sync(query["text"]),
            top_k=5,
            include_metadata=True,
        )

        # Check if expected document is in top results
        expected_id = query.get("expected_doc_id")
        if expected_id:
            found = any(r["id"].startswith(expected_id) for r in results["matches"])
            validation["checks"].append(
                {
                    "name": f"query_relevance_{query['id']}",
                    "query": query["text"][:50],
                    "expected_in_top_5": expected_id,
                    "found": found,
                    "passed": found,
                }
            )
            if not found:
                validation["passed"] = False

    # Check 3: Index freshness
    freshness_ms = stats.get("freshness_ms", 0)
    max_freshness_ms = 60000  # 1 minute
    freshness_ok = freshness_ms <= max_freshness_ms
    validation["checks"].append(
        {
            "name": "index_freshness",
            "freshness_ms": freshness_ms,
            "max_allowed_ms": max_freshness_ms,
            "passed": freshness_ok,
        }
    )

    if not freshness_ok:
        logger.warning(f"Index freshness {freshness_ms}ms exceeds threshold")
        # Don't fail on freshness, just warn

    return validation
```

Requirements:

- Verify vector count matches upserted count
- Run sample queries to validate search quality
- Check index freshness for latency issues
- Return detailed validation report for monitoring

## Deferrable Operator Integration

Use the custom trigger in a deferrable operator:

```python
from airflow.sensors.base import BaseSensorOperator


class VectorStoreIndexingSensor(BaseSensorOperator):
    """
    Deferrable sensor that waits for vector store indexing.

    Releases worker slot while waiting by deferring to trigger.
    """

    def __init__(
        self,
        collection_name: str,
        expected_count: int,
        connection_id: str = "vector_store_default",
        poll_interval: float = 30.0,
        timeout: float = 3600.0,
        deferrable: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.expected_count = expected_count
        self.connection_id = connection_id
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.deferrable = deferrable

    def execute(self, context):
        """Check immediately, then defer if not ready."""
        # Quick check before deferring
        if self._check_complete():
            return {"status": "immediate_success"}

        if not self.deferrable:
            # Fallback to poke mode
            raise AirflowSkipException("Not ready, will retry")

        # Defer to async trigger
        self.defer(
            trigger=VectorStoreIndexingTrigger(
                collection_name=self.collection_name,
                expected_count=self.expected_count,
                connection_id=self.connection_id,
                poll_interval=self.poll_interval,
                timeout=self.timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event):
        """Called when trigger fires."""
        if event["status"] == "timeout":
            raise AirflowException(f"Vector store indexing timed out: {event['message']}")

        self.log.info(f"Indexing complete: {event['indexed_count']} vectors")
        return event

    def _check_complete(self) -> bool:
        """Synchronous check for immediate completion."""
        try:
            stats = self._get_index_stats()
            return stats.get("total_vector_count", 0) >= self.expected_count
        except Exception:
            return False
```

## Complete DAG Structure

```python
from datetime import datetime, timedelta

from airflow.sdk import Asset, dag, task
from capstone.sensors import VectorStoreIndexingSensor

ENRICHED_DOCS_ASSET = Asset("documents.enriched")
INDEXED_DOCS_ASSET = Asset("documents.indexed")


@dag(
    dag_id="vector_store_pipeline",
    schedule=ENRICHED_DOCS_ASSET,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "vector-store", "embeddings"],
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
)
def vector_store_pipeline():
    # Task 1: Load enriched documents
    documents = load_enriched_documents()

    # Task 2: Prepare chunks for parallel processing
    chunks = prepare_chunks(documents)

    # Task 3: Parallel embedding generation
    embeddings = generate_embedding.expand(chunk=chunks)

    # Task 4: Aggregate embeddings (reduce step)
    @task
    def aggregate_embeddings(embedding_list: list[dict]) -> list[dict]:
        """Collect all embeddings from mapped tasks."""
        # Filter out any failed embeddings
        valid = [e for e in embedding_list if e.get("embedding") is not None]
        logger.info(f"Aggregated {len(valid)}/{len(embedding_list)} embeddings")
        return valid

    all_embeddings = aggregate_embeddings(embeddings)

    # Task 5: Batch upsert to vector store
    upsert_result = batch_upsert_vectors(all_embeddings)

    # Task 6: Wait for indexing (deferrable)
    wait_for_index = VectorStoreIndexingSensor(
        task_id="wait_for_indexing",
        collection_name="documents",
        expected_count="{{ ti.xcom_pull(task_ids='batch_upsert_vectors')['upserted'] }}",
        deferrable=True,
        poke_interval=30,
        timeout=3600,
    )

    # Task 7: Validate index health
    validation = validate_index_health(wait_for_index.output)

    # Task 8: Publish indexed asset
    publish = publish_indexed_asset(validation)

    # Set dependencies
    upsert_result >> wait_for_index >> validation >> publish


vector_store_pipeline()
```

## Success Criteria

- [ ] Dynamic mapping creates parallel embedding tasks for each chunk
- [ ] Embeddings are batched efficiently for vector store upsert
- [ ] Custom trigger serializes and runs correctly in triggerer
- [ ] Deferrable sensor releases worker slot during wait
- [ ] DAG is triggered by `documents.enriched` asset
- [ ] `documents.indexed` asset is published on completion
- [ ] Index validation checks vector count and query quality
- [ ] All errors are handled gracefully with clear logging

## Hints

<details>
<summary>Hint 1: Aggregating Mapped Task Results</summary>

```python
@task
def aggregate_embeddings(embeddings: list[dict]) -> list[dict]:
    """
    When using .expand(), the next task receives a list of all outputs.

    Filter out failures and log statistics.
    """
    successful = []
    failed = []

    for e in embeddings:
        if e.get("embedding") is not None:
            successful.append(e)
        else:
            failed.append(e)

    logger.info(f"Embedding stats: {len(successful)} success, {len(failed)} failed")

    if failed:
        logger.warning(f"Failed chunk IDs: {[f['chunk_id'] for f in failed]}")

    return successful


# In DAG:
embeddings = generate_embedding.expand(chunk=chunks)
aggregated = aggregate_embeddings(embeddings)  # Receives list of all outputs
```

</details>

<details>
<summary>Hint 2: Mock Embedding Model</summary>

```python
def mock_embedding(text: str, dimension: int = 1536) -> list[float]:
    """
    Generate deterministic mock embedding for testing.

    Uses hash of text to create reproducible vectors.
    """
    import hashlib

    # Create deterministic seed from text
    text_hash = hashlib.sha256(text.encode()).digest()

    # Generate vector from hash bytes
    import struct

    values = []
    for i in range(0, min(len(text_hash), dimension * 4), 4):
        if len(values) >= dimension:
            break
        val = struct.unpack("f", text_hash[i : i + 4])[0]
        # Normalize to reasonable range
        values.append((val % 2) - 1)

    # Pad if needed
    while len(values) < dimension:
        values.append(0.0)

    return values[:dimension]
```

</details>

<details>
<summary>Hint 3: Async HTTP Client in Trigger</summary>

```python
import aiohttp


async def _check_index_status(self) -> dict:
    """Async HTTP request to vector store API."""
    # Get connection details
    conn = await self._get_connection_async()

    async with aiohttp.ClientSession() as session, session.get(
        f"{conn.host}/describe_index_stats",
        headers={"Authorization": f"Bearer {conn.password}"},
        timeout=aiohttp.ClientTimeout(total=30),
    ) as response:
        if response.status != 200:
            raise Exception(f"API error: {response.status}")

        data = await response.json()
        return {
            "indexed_count": data.get("total_vector_count", 0),
            "freshness_ms": data.get("freshness_ms", 0),
            "index_fullness": data.get("index_fullness", 0),
        }
```

</details>

<details>
<summary>Hint 4: Testing Deferrable Operators</summary>

```python
from unittest.mock import AsyncMock, patch

import pytest


@pytest.mark.asyncio
async def test_vector_store_trigger():
    """Test custom trigger behavior."""
    trigger = VectorStoreIndexingTrigger(
        collection_name="test",
        expected_count=100,
        poll_interval=0.1,  # Fast polling for test
        timeout=1.0,
    )

    # Mock the check method
    with patch.object(trigger, "_check_index_status", new_callable=AsyncMock) as mock:
        mock.return_value = {"indexed_count": 100, "freshness_ms": 50}

        events = []
        async for event in trigger.run():
            events.append(event)

        assert len(events) == 1
        assert events[0].payload["status"] == "success"
        assert events[0].payload["indexed_count"] == 100
```

</details>

## Files

- **Starter**: `../starter/exercise_16_4_vector_store_starter.py`
- **Solution**: `../solutions/dags/vector_store_pipeline.py`
- **Trigger**: `../solutions/triggers/indexing_trigger.py`
- **Sensor**: `../solutions/sensors/vector_store_sensor.py`

## Estimated Time

3 hours

## References

- [Module 06: Dynamic Tasks](../../06-dynamic-tasks/README.md) - Task mapping patterns
- [Module 11: Sensors & Deferrable](../../11-sensors-deferrable/README.md) - Custom triggers
- [Module 15: AI/ML Orchestration](../../15-ai-ml-orchestration/README.md) - Embedding patterns

---

[Previous: Exercise 16.3 - LLM Processing](exercise_16_3_llm_processing.md) | [Next: Exercise 16.5 - Deployment](exercise_16_5_deployment.md)
