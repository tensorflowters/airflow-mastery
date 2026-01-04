"""
Vector Store Sensor Starter Code.

Build a deferrable sensor for waiting on vector store indexing operations,
demonstrating async patterns for AI/ML pipelines.

TODO List:
1. Implement VectorStoreIndexTrigger for async polling
2. Create VectorStoreSensor that defers to trigger
3. Add index health check validation
4. Build integration pipeline DAG
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = logging.getLogger(__name__)


# =============================================================================
# Mock Vector Store Backend
# =============================================================================


@dataclass
class MockVectorCollection:
    """Simulated vector store collection."""

    name: str
    dimensions: int = 384
    documents: list[dict[str, Any]] | None = None
    status: str = "indexing"
    progress: float = 0.0
    created_at: datetime | None = None

    def __post_init__(self):
        """Initialize collection with defaults."""
        if self.documents is None:
            self.documents = []
        if self.created_at is None:
            self.created_at = datetime.now()


# Global mock storage (simulates vector store state)
MOCK_COLLECTIONS: dict[str, MockVectorCollection] = {}


def create_collection(name: str, dimensions: int = 384) -> MockVectorCollection:
    """Create a new vector collection."""
    collection = MockVectorCollection(name=name, dimensions=dimensions)
    MOCK_COLLECTIONS[name] = collection
    logger.info(f"Created collection: {name}")
    return collection


def get_collection(name: str) -> MockVectorCollection | None:
    """Get collection by name."""
    return MOCK_COLLECTIONS.get(name)


def upsert_embeddings(
    collection_name: str,
    documents: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Upsert embeddings to collection.

    Simulates async indexing by setting status to 'indexing'.
    """
    collection = MOCK_COLLECTIONS.get(collection_name)
    if not collection:
        collection = create_collection(collection_name)

    # Add documents
    for doc in documents:
        collection.documents.append(doc)

    # Set to indexing status
    collection.status = "indexing"
    collection.progress = 0.0

    logger.info(f"Upserting {len(documents)} documents to {collection_name}")

    return {
        "collection": collection_name,
        "upserted": len(documents),
        "status": "indexing",
    }


def get_collection_status(name: str) -> dict[str, Any]:
    """
    Get collection indexing status.

    Simulates indexing progress over time.
    """
    collection = MOCK_COLLECTIONS.get(name)
    if not collection:
        return {"status": "not_found", "error": f"Collection {name} not found"}

    # Simulate indexing progress
    if collection.status == "indexing":
        # Progress increases over time
        elapsed = (datetime.now() - collection.created_at).total_seconds()
        collection.progress = min(100.0, elapsed * 10)  # 10% per second

        # Complete after 10 seconds (simulated)
        if collection.progress >= 100:
            collection.status = "ready"
            collection.progress = 100.0

        # Random chance of failure for testing
        if random.random() < 0.05:  # 5% failure rate
            collection.status = "failed"
            return {
                "status": "failed",
                "error": "Simulated indexing failure",
                "collection": name,
            }

    return {
        "status": collection.status,
        "progress": collection.progress,
        "document_count": len(collection.documents),
        "dimensions": collection.dimensions,
        "collection": name,
    }


def query_collection(
    name: str,
    query_embedding: list[float],
    k: int = 5,
) -> list[dict[str, Any]]:
    """Query collection with embedding vector."""
    collection = MOCK_COLLECTIONS.get(name)
    if not collection or collection.status != "ready":
        return []

    # Return mock results
    results = []
    for i, doc in enumerate(collection.documents[:k]):
        results.append(
            {
                "id": doc.get("id", f"doc-{i}"),
                "score": random.uniform(0.7, 0.99),
                "metadata": doc.get("metadata", {}),
            }
        )

    return sorted(results, key=lambda x: x["score"], reverse=True)


def mock_embedding(text: str, dimensions: int = 384) -> list[float]:
    """Generate deterministic mock embedding."""
    hash_bytes = hashlib.sha256(text.encode()).digest()
    extended = (hash_bytes * (dimensions // len(hash_bytes) + 1))[:dimensions]
    return [b / 255.0 for b in extended]


# =============================================================================
# Vector Store Index Trigger
# =============================================================================


class VectorStoreIndexTrigger(BaseTrigger):
    """
    Trigger that waits for vector store indexing to complete.

    Polls the vector store status asynchronously without holding
    a worker slot, using the triggerer service.

    TODO: Implement the trigger methods
    """

    def __init__(
        self,
        collection_name: str,
        expected_count: int = 0,
        poll_interval: float = 2.0,
        timeout: float = 60.0,
    ):
        """
        Initialize the vector store index trigger.

        Args:
            collection_name: Name of the collection to monitor
            expected_count: Minimum expected document count
            poll_interval: Seconds between status checks
            timeout: Maximum seconds to wait
        """
        super().__init__()
        self.collection_name = collection_name
        self.expected_count = expected_count
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize trigger for database storage.

        Returns:
            Tuple of (classpath, constructor_kwargs)
        """
        # TODO: Implement serialization
        # The triggerer needs this to reconstruct the trigger

        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "collection_name": self.collection_name,
                "expected_count": self.expected_count,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def _check_status(self) -> dict[str, Any]:
        """
        Check collection indexing status.

        Runs synchronous status check in executor to avoid blocking.
        """
        # TODO: Implement async status check
        # Use run_in_executor for sync operations

        loop = asyncio.get_event_loop()
        status = await loop.run_in_executor(
            None,
            lambda: get_collection_status(self.collection_name),
        )
        return status

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Async generator that polls for indexing completion.

        TODO: Implement the polling loop
        1. Calculate timeout deadline
        2. Poll status at poll_interval
        3. Yield TriggerEvent on success, failure, or timeout
        """
        # TODO: Use these variables in the polling loop implementation
        # start_time = datetime.utcnow()
        # timeout_at = start_time + timedelta(seconds=self.timeout)
        # check_count = 0

        self.log.info(
            f"VectorStoreIndexTrigger started: collection={self.collection_name}, "
            f"expected_count={self.expected_count}, timeout={self.timeout}s"
        )

        # TODO: Implement polling loop
        # while datetime.utcnow() < timeout_at:
        #     check_count += 1
        #     status = await self._check_status()
        #
        #     # Handle ready status
        #     if status.get("status") == "ready":
        #         # Check document count if expected
        #         ...
        #
        #     # Handle failed status
        #     if status.get("status") == "failed":
        #         ...
        #
        #     # Log progress
        #     ...
        #
        #     # Wait before next poll
        #     await asyncio.sleep(self.poll_interval)

        # Placeholder - yield timeout immediately
        yield TriggerEvent(
            {
                "status": "timeout",
                "message": "Trigger not implemented",
                "collection": self.collection_name,
            }
        )


# =============================================================================
# Vector Store Sensor
# =============================================================================


class VectorStoreSensor(BaseSensorOperator):
    """
    Sensor that waits for vector store indexing to complete.

    Supports both poke mode and deferrable mode. In deferrable mode,
    the sensor releases the worker slot and uses the triggerer service.

    TODO: Implement the sensor methods
    """

    template_fields = ("collection_name",)

    def __init__(
        self,
        collection_name: str,
        expected_count: int = 0,
        poll_interval: float = 2.0,
        deferrable: bool = True,
        **kwargs,
    ):
        """
        Initialize vector store sensor.

        Args:
            collection_name: Collection to wait for
            expected_count: Minimum expected document count
            poll_interval: Seconds between checks
            deferrable: Use deferrable mode (recommended)
            **kwargs: Passed to BaseSensorOperator
        """
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.expected_count = expected_count
        self.poll_interval = poll_interval
        self._deferrable = deferrable

    def poke(self, context: Context) -> bool:
        """
        Check if vector store is ready (poke mode).

        Returns:
            True if ready, False to continue waiting
        """
        # TODO: Implement poke method
        # 1. Get collection status
        # 2. Check if status is "ready"
        # 3. Validate document count if expected_count > 0

        status = get_collection_status(self.collection_name)
        self.log.info(f"Poke: {self.collection_name} status={status}")

        if status.get("status") == "ready":
            if self.expected_count > 0:
                doc_count = status.get("document_count", 0)
                return doc_count >= self.expected_count
            return True

        return False

    def execute(self, context: Context) -> None:
        """
        Execute sensor - defer to trigger if deferrable.

        TODO: Implement deferrable execution
        1. Quick poke check first
        2. If not ready and deferrable, defer to trigger
        3. If not deferrable, call super().execute()
        """
        # Quick check first
        if self.poke(context):
            self.log.info(f"Collection {self.collection_name} already ready")
            return

        # TODO: Implement deferrable mode
        # if self._deferrable:
        #     self.defer(
        #         trigger=VectorStoreIndexTrigger(
        #             collection_name=self.collection_name,
        #             expected_count=self.expected_count,
        #             poll_interval=self.poll_interval,
        #             timeout=self.timeout,
        #         ),
        #         method_name="execute_complete",
        #     )
        # else:
        #     super().execute(context)

        # Placeholder - just complete
        self.log.warning("Deferrable mode not implemented")

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Called when trigger fires.

        Args:
            context: Airflow context
            event: TriggerEvent payload
        """
        # TODO: Implement completion handler
        # 1. Check event status
        # 2. Log results
        # 3. Raise exception on failure/timeout

        status = event.get("status")
        if status == "success":
            self.log.info(
                f"Vector store ready: collection={event.get('collection')}, documents={event.get('document_count')}"
            )
        elif status == "timeout":
            raise AirflowException(f"Timeout waiting for {self.collection_name}")
        else:
            raise AirflowException(f"Vector store error: {event}")


# =============================================================================
# DAG Definition
# =============================================================================


@dag(
    dag_id="vector_store_sensor_exercise",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ai-ml", "sensors", "module-11"],
    doc_md=__doc__,
)
def vector_store_sensor_exercise():
    """
    Vector Store Sensor Pipeline.

    Demonstrates deferrable sensors for AI/ML indexing operations.
    """

    @task
    def prepare_documents() -> list[dict[str, Any]]:
        """Prepare sample documents for indexing."""
        documents = []
        topics = [
            "Machine learning fundamentals",
            "Deep neural networks",
            "Natural language processing",
            "Computer vision techniques",
            "Reinforcement learning",
        ]

        for i, topic in enumerate(topics):
            doc_id = f"doc-{i:03d}"
            content = f"{topic}: This is sample content for testing vector search."
            embedding = mock_embedding(content)

            documents.append(
                {
                    "id": doc_id,
                    "content": content,
                    "embedding": embedding,
                    "metadata": {
                        "topic": topic,
                        "source": "exercise",
                    },
                }
            )

        logger.info(f"Prepared {len(documents)} documents")
        return documents

    @task
    def ingest_to_vector_store(
        documents: list[dict[str, Any]],
        collection_name: str = "exercise_collection",
    ) -> dict[str, Any]:
        """Ingest documents to vector store."""
        # Create collection if needed
        if not get_collection(collection_name):
            create_collection(collection_name)

        # Upsert documents
        result = upsert_embeddings(collection_name, documents)

        return {
            "collection": collection_name,
            "documents_ingested": len(documents),
            "status": result.get("status"),
        }

    # TODO: Add VectorStoreSensor to wait for indexing
    # wait_for_indexing = VectorStoreSensor(
    #     task_id="wait_for_indexing",
    #     collection_name="exercise_collection",
    #     expected_count=5,
    #     poll_interval=1.0,
    #     timeout=30,
    #     deferrable=True,
    # )

    @task
    def validate_index(
        ingest_result: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Validate vector store index is healthy.

        TODO: Implement validation
        1. Get collection status
        2. Verify document count
        3. Run test query
        4. Return health report
        """
        collection_name = ingest_result.get("collection", "exercise_collection")

        # Simulate waiting for indexing (remove when sensor is implemented)
        time.sleep(2)

        # Get status
        status = get_collection_status(collection_name)

        if status.get("status") != "ready":
            raise AirflowException(f"Collection not ready: {status}")

        # Run test query
        test_query = mock_embedding("machine learning")
        results = query_collection(collection_name, test_query, k=3)

        return {
            "collection": collection_name,
            "status": "healthy",
            "document_count": status.get("document_count", 0),
            "dimensions": status.get("dimensions", 0),
            "test_query_results": len(results),
        }

    @task
    def run_sample_queries(
        validation_result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Run sample queries to verify index."""
        collection_name = validation_result.get("collection", "exercise_collection")

        queries = [
            "What are neural networks?",
            "How does NLP work?",
            "Explain computer vision",
        ]

        results = []
        for query_text in queries:
            query_embedding = mock_embedding(query_text)
            search_results = query_collection(collection_name, query_embedding, k=2)

            results.append(
                {
                    "query": query_text,
                    "results_count": len(search_results),
                    "top_score": search_results[0]["score"] if search_results else 0,
                }
            )

        logger.info(f"Ran {len(results)} sample queries")
        return results

    @task
    def summarize_pipeline(
        validation: dict[str, Any],
        query_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Summarize pipeline execution."""
        return {
            "collection": validation.get("collection"),
            "documents_indexed": validation.get("document_count"),
            "index_healthy": validation.get("status") == "healthy",
            "queries_executed": len(query_results),
            "avg_top_score": sum(q["top_score"] for q in query_results) / len(query_results) if query_results else 0,
            "pipeline_complete": True,
        }

    # DAG Flow
    docs = prepare_documents()
    ingest_result = ingest_to_vector_store(docs)

    # TODO: Add sensor between ingest and validate
    # ingest_result >> wait_for_indexing >> validate

    validation = validate_index(ingest_result)
    queries = run_sample_queries(validation)
    summarize_pipeline(validation, queries)


# Instantiate the DAG
vector_store_sensor_exercise()
