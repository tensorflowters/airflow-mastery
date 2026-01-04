"""
Vector Store Sensor Solution.

Complete implementation of a deferrable sensor for vector store indexing,
demonstrating async patterns for AI/ML pipelines.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
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
    """Upsert embeddings to collection, simulating async indexing."""
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
    """Get collection indexing status with simulated progress."""
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

        # Random chance of failure for testing (reduced for solution)
        if random.random() < 0.02:  # 2% failure rate
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
# Vector Store Index Trigger (SOLUTION)
# =============================================================================


class VectorStoreIndexTrigger(BaseTrigger):
    """
    Trigger that waits for vector store indexing to complete.

    Polls the vector store status asynchronously without holding
    a worker slot, using the triggerer service. This is ideal for
    long-running indexing operations common in AI/ML pipelines.
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
            expected_count: Minimum expected document count (0 = any count)
            poll_interval: Seconds between status checks
            timeout: Maximum seconds to wait before timing out
        """
        super().__init__()
        self.collection_name = collection_name
        self.expected_count = expected_count
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize trigger for database storage.

        The triggerer service needs this to reconstruct the trigger
        after it's been persisted. All constructor args must be
        JSON-serializable.

        Returns:
            Tuple of (classpath, constructor_kwargs)
        """
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
        Check collection indexing status asynchronously.

        Runs synchronous status check in executor to avoid blocking
        the async event loop.

        Returns:
            Status dictionary with indexing state information
        """
        loop = asyncio.get_event_loop()
        status = await loop.run_in_executor(
            None,
            lambda: get_collection_status(self.collection_name),
        )
        return status

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Async generator that polls for indexing completion.

        This method runs in the triggerer service and polls the
        vector store status at regular intervals until:
        - Status becomes "ready" (success)
        - Status becomes "failed" (error)
        - Timeout is exceeded (timeout)

        Yields:
            TriggerEvent with status and collection information
        """
        start_time = datetime.utcnow()
        timeout_at = start_time + timedelta(seconds=self.timeout)
        check_count = 0

        self.log.info(
            f"VectorStoreIndexTrigger started: collection={self.collection_name}, "
            f"expected_count={self.expected_count}, timeout={self.timeout}s"
        )

        while datetime.utcnow() < timeout_at:
            check_count += 1
            status = await self._check_status()

            current_status = status.get("status")
            progress = status.get("progress", 0)
            doc_count = status.get("document_count", 0)

            # Handle ready status
            if current_status == "ready":
                # Validate document count if expected
                if self.expected_count > 0 and doc_count < self.expected_count:
                    self.log.warning(
                        f"Collection ready but document count {doc_count} < expected {self.expected_count}"
                    )
                    yield TriggerEvent(
                        {
                            "status": "error",
                            "message": (f"Document count {doc_count} below expected {self.expected_count}"),
                            "collection": self.collection_name,
                            "document_count": doc_count,
                        }
                    )
                    return

                elapsed = (datetime.utcnow() - start_time).total_seconds()
                self.log.info(f"Collection {self.collection_name} ready after {elapsed:.1f}s ({check_count} checks)")
                yield TriggerEvent(
                    {
                        "status": "success",
                        "collection": self.collection_name,
                        "document_count": doc_count,
                        "dimensions": status.get("dimensions"),
                        "elapsed_seconds": elapsed,
                        "check_count": check_count,
                    }
                )
                return

            # Handle failed status
            if current_status == "failed":
                error_msg = status.get("error", "Unknown indexing failure")
                self.log.error(f"Collection {self.collection_name} failed: {error_msg}")
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": error_msg,
                        "collection": self.collection_name,
                    }
                )
                return

            # Handle not found status
            if current_status == "not_found":
                self.log.error(f"Collection {self.collection_name} not found")
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": f"Collection {self.collection_name} not found",
                        "collection": self.collection_name,
                    }
                )
                return

            # Log progress for indexing status
            if current_status == "indexing":
                self.log.info(f"Check {check_count}: {self.collection_name} indexing {progress:.1f}% complete")

            # Wait before next poll
            await asyncio.sleep(self.poll_interval)

        # Timeout reached
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        self.log.warning(f"Timeout after {elapsed:.1f}s waiting for {self.collection_name}")
        yield TriggerEvent(
            {
                "status": "timeout",
                "message": f"Indexing did not complete within {self.timeout}s",
                "collection": self.collection_name,
                "elapsed_seconds": elapsed,
                "check_count": check_count,
            }
        )


# =============================================================================
# Vector Store Sensor (SOLUTION)
# =============================================================================


class VectorStoreSensor(BaseSensorOperator):
    """
    Sensor that waits for vector store indexing to complete.

    Supports both poke mode and deferrable mode. In deferrable mode,
    the sensor releases the worker slot and uses the triggerer service,
    making it ideal for long-running AI/ML indexing operations.

    Example:
        wait_for_index = VectorStoreSensor(
            task_id="wait_for_index",
            collection_name="my_collection",
            expected_count=1000,
            poll_interval=5.0,
            timeout=3600,
            deferrable=True,
        )
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
            expected_count: Minimum expected document count (0 = any)
            poll_interval: Seconds between checks
            deferrable: Use deferrable mode (recommended for long waits)
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

        Called repeatedly in poke mode. For deferrable mode,
        this is only called once as an initial quick check.

        Args:
            context: Airflow context

        Returns:
            True if ready, False to continue waiting
        """
        status = get_collection_status(self.collection_name)
        current_status = status.get("status")

        self.log.info(
            f"Poke: {self.collection_name} status={current_status}, progress={status.get('progress', 0):.1f}%"
        )

        # Not found is an error condition
        if current_status == "not_found":
            raise AirflowException(f"Collection {self.collection_name} not found")

        # Failed is an error condition
        if current_status == "failed":
            raise AirflowException(
                f"Collection {self.collection_name} indexing failed: {status.get('error', 'Unknown error')}"
            )

        # Check if ready
        if current_status == "ready":
            doc_count = status.get("document_count", 0)
            if self.expected_count > 0 and doc_count < self.expected_count:
                self.log.warning(f"Collection ready but document count {doc_count} < expected {self.expected_count}")
                return False
            return True

        return False

    def execute(self, context: Context) -> None:
        """
        Execute sensor - defer to trigger if deferrable.

        In deferrable mode, this method does a quick initial check,
        then defers to the VectorStoreIndexTrigger for async polling.
        This releases the worker slot during the wait.

        Args:
            context: Airflow context
        """
        # Quick check first - avoid deferring if already ready
        if self.poke(context):
            self.log.info(f"Collection {self.collection_name} already ready")
            return

        # Defer to trigger for async waiting
        if self._deferrable:
            self.log.info(f"Deferring to trigger for {self.collection_name} (timeout={self.timeout}s)")
            self.defer(
                trigger=VectorStoreIndexTrigger(
                    collection_name=self.collection_name,
                    expected_count=self.expected_count,
                    poll_interval=self.poll_interval,
                    timeout=self.timeout,
                ),
                method_name="execute_complete",
            )
        else:
            # Fallback to poke mode (holds worker slot)
            self.log.info("Using poke mode (not recommended for long waits)")
            super().execute(context)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Called when trigger fires with the result.

        Processes the TriggerEvent from VectorStoreIndexTrigger and
        either completes successfully or raises an exception.

        Args:
            context: Airflow context
            event: TriggerEvent payload with status and details
        """
        status = event.get("status")
        collection = event.get("collection", self.collection_name)

        if status == "success":
            doc_count = event.get("document_count", 0)
            elapsed = event.get("elapsed_seconds", 0)
            self.log.info(f"Vector store ready: collection={collection}, documents={doc_count}, elapsed={elapsed:.1f}s")
            return

        if status == "timeout":
            elapsed = event.get("elapsed_seconds", self.timeout)
            raise AirflowException(f"Timeout after {elapsed:.1f}s waiting for {collection} to be ready")

        # Handle error status
        message = event.get("message", "Unknown error")
        raise AirflowException(f"Vector store error for {collection}: {message}")


# =============================================================================
# Index Health Validator (SOLUTION)
# =============================================================================


def validate_index_health(
    collection_name: str,
    expected_count: int = 0,
    run_test_query: bool = True,
) -> dict[str, Any]:
    """
    Validate vector store index is healthy and queryable.

    Performs comprehensive health checks on a vector store collection:
    1. Verifies collection exists and is ready
    2. Validates document count meets expectations
    3. Optionally runs a test query to verify queryability

    Args:
        collection_name: Name of the collection to validate
        expected_count: Minimum expected document count
        run_test_query: Whether to run a test query

    Returns:
        Health report dictionary

    Raises:
        AirflowException: If any health check fails
    """
    # Get collection status
    status = get_collection_status(collection_name)

    if status.get("status") == "not_found":
        raise AirflowException(f"Collection {collection_name} not found")

    if status.get("status") != "ready":
        raise AirflowException(f"Collection {collection_name} not ready: {status.get('status')}")

    doc_count = status.get("document_count", 0)
    dimensions = status.get("dimensions", 0)

    # Validate document count
    if expected_count > 0 and doc_count < expected_count:
        raise AirflowException(f"Document count {doc_count} below expected {expected_count}")

    health_report = {
        "collection": collection_name,
        "status": "healthy",
        "document_count": doc_count,
        "dimensions": dimensions,
        "expected_count": expected_count,
        "test_query_passed": False,
    }

    # Run test query
    if run_test_query:
        test_embedding = mock_embedding("test query for health check")
        results = query_collection(collection_name, test_embedding, k=3)

        if not results:
            raise AirflowException(f"Test query returned no results for {collection_name}")

        health_report["test_query_passed"] = True
        health_report["test_query_results"] = len(results)
        health_report["top_result_score"] = results[0].get("score", 0)

    logger.info(f"Index health check passed: {health_report}")
    return health_report


# =============================================================================
# DAG Definition (SOLUTION)
# =============================================================================


@dag(
    dag_id="vector_store_sensor_solution",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ai-ml", "sensors", "module-11", "solution"],
    doc_md=__doc__,
)
def vector_store_sensor_solution():
    """
    Vector Store Sensor Pipeline (Solution).

    Demonstrates complete deferrable sensor pattern for AI/ML
    vector store indexing operations.

    Flow:
    1. Prepare sample documents with embeddings
    2. Ingest documents to vector store
    3. Wait for indexing (deferrable sensor)
    4. Validate index health
    5. Run sample queries
    6. Summarize results
    """

    @task
    def prepare_documents() -> list[dict[str, Any]]:
        """Prepare sample documents with embeddings for indexing."""
        documents = []
        topics = [
            "Machine learning fundamentals and algorithms",
            "Deep neural networks and backpropagation",
            "Natural language processing with transformers",
            "Computer vision and convolutional networks",
            "Reinforcement learning and policy gradients",
            "Generative AI and large language models",
            "Vector databases and similarity search",
            "MLOps and model deployment pipelines",
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
                        "source": "solution",
                        "index": i,
                    },
                }
            )

        logger.info(f"Prepared {len(documents)} documents with embeddings")
        return documents

    @task
    def ingest_to_vector_store(
        documents: list[dict[str, Any]],
        collection_name: str = "solution_collection",
    ) -> dict[str, Any]:
        """Ingest documents to vector store and start indexing."""
        # Create collection if needed
        if not get_collection(collection_name):
            create_collection(collection_name)

        # Upsert documents (starts async indexing)
        result = upsert_embeddings(collection_name, documents)

        logger.info(f"Ingested {len(documents)} documents to {collection_name}, status={result.get('status')}")

        return {
            "collection": collection_name,
            "documents_ingested": len(documents),
            "status": result.get("status"),
        }

    # Deferrable sensor waits for indexing without holding worker
    wait_for_indexing = VectorStoreSensor(
        task_id="wait_for_indexing",
        collection_name="solution_collection",
        expected_count=8,  # Match document count
        poll_interval=1.0,  # Check every second (fast for demo)
        timeout=30,  # 30 second timeout
        deferrable=True,  # Use triggerer service
        poke_interval=1,  # Fallback poke interval
        mode="reschedule",  # Release slot between pokes
    )

    @task
    def validate_index(
        ingest_result: dict[str, Any],
    ) -> dict[str, Any]:
        """Validate vector store index health after sensor completes."""
        collection_name = ingest_result.get("collection", "solution_collection")
        expected_count = ingest_result.get("documents_ingested", 0)

        return validate_index_health(
            collection_name=collection_name,
            expected_count=expected_count,
            run_test_query=True,
        )

    @task
    def run_sample_queries(
        validation_result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Run sample queries to demonstrate index functionality."""
        collection_name = validation_result.get("collection", "solution_collection")

        queries = [
            "What is machine learning?",
            "How do neural networks work?",
            "Explain natural language processing",
            "What are vector databases?",
            "Describe MLOps practices",
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
                    "top_doc_id": search_results[0]["id"] if search_results else None,
                }
            )
            logger.info(f"Query: '{query_text[:30]}...' -> {len(search_results)} results")

        return results

    @task
    def summarize_pipeline(
        validation: dict[str, Any],
        query_results: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Summarize complete pipeline execution."""
        avg_score = sum(q["top_score"] for q in query_results) / len(query_results) if query_results else 0

        summary = {
            "collection": validation.get("collection"),
            "documents_indexed": validation.get("document_count"),
            "dimensions": validation.get("dimensions"),
            "index_healthy": validation.get("status") == "healthy",
            "test_query_passed": validation.get("test_query_passed", False),
            "sample_queries_executed": len(query_results),
            "average_similarity_score": round(avg_score, 4),
            "pipeline_complete": True,
        }

        logger.info(f"Pipeline summary: {summary}")
        return summary

    # DAG Flow with deferrable sensor
    docs = prepare_documents()
    ingest_result = ingest_to_vector_store(docs)

    # Sensor waits for indexing (deferred execution)
    ingest_result >> wait_for_indexing

    # Continue after sensor completes
    validation = validate_index(ingest_result)
    wait_for_indexing >> validation

    queries = run_sample_queries(validation)
    summarize_pipeline(validation, queries)


# Instantiate the DAG
vector_store_sensor_solution()
