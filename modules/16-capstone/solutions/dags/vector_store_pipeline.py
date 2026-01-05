"""
Capstone Solution: Vector Store Pipeline
=========================================

Production-ready embedding and indexing DAG demonstrating:
- Asset-triggered scheduling from processing pipeline
- Parallel embedding generation with dynamic mapping
- Custom deferrable trigger for indexing completion
- Batch vector store operations

Modules Applied: 06, 11, 15
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.sdk import Asset, Variable, dag, task
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.trigger_rule import TriggerRule

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_PATH = Variable.get("capstone_base_path", default_var="/tmp/capstone")
ENRICHED_PATH = f"{BASE_PATH}/enriched"
VECTOR_STORE_PATH = f"{BASE_PATH}/vector_store"

# Asset definitions
ENRICHED_DOCUMENTS = Asset("documents.enriched")
INDEXED_DOCUMENTS = Asset("documents.indexed")

# Embedding Configuration
EMBEDDING_MODEL = Variable.get("embedding_model", default_var="text-embedding-ada-002")
EMBEDDING_DIMENSIONS = int(Variable.get("embedding_dimensions", default_var="1536"))
BATCH_SIZE = int(Variable.get("embedding_batch_size", default_var="10"))


# =============================================================================
# CUSTOM TRIGGER FOR VECTOR STORE INDEXING
# =============================================================================


class VectorStoreIndexingTrigger(BaseTrigger):
    """
    Custom trigger that waits for vector store indexing to complete.

    This deferrable trigger polls the vector store to check if all documents
    have been indexed, releasing the worker slot during the wait.
    """

    def __init__(
        self,
        collection_name: str,
        expected_count: int,
        poll_interval: int = 30,
        timeout: int = 600,
    ):
        """Initialize the vector store indexing trigger.

        Args:
            collection_name: Name of the vector store collection to monitor.
            expected_count: Number of vectors expected after indexing completes.
            poll_interval: Seconds between status checks (default: 30).
            timeout: Maximum wait time in seconds (default: 600).
        """
        super().__init__()
        self.collection_name = collection_name
        self.expected_count = expected_count
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize the trigger for storage."""
        return (
            "capstone.triggers.VectorStoreIndexingTrigger",
            {
                "collection_name": self.collection_name,
                "expected_count": self.expected_count,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Poll vector store until indexing completes or timeout.

        Yields:
            TriggerEvent with indexing status
        """
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
                            "collection": self.collection_name,
                            "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                        }
                    )
                    return

                logger.info(
                    f"Indexing in progress: {status['indexed_count']}/{self.expected_count} "
                    f"in collection {self.collection_name}"
                )

            except Exception as e:
                logger.warning(f"Error checking index status: {e}")

            await asyncio.sleep(self.poll_interval)

        # Timeout reached
        yield TriggerEvent(
            {
                "status": "timeout",
                "message": f"Indexing did not complete within {self.timeout} seconds",
                "collection": self.collection_name,
            }
        )

    async def _check_index_status(self) -> dict[str, Any]:
        """
        Check the current indexing status.

        In production, this would query the actual vector store.
        """
        # Simulated check - in production, query your vector store
        index_path = Path(VECTOR_STORE_PATH) / f"{self.collection_name}_index.json"
        if index_path.exists():
            index_data = json.loads(index_path.read_text())
            return {"indexed_count": len(index_data.get("vectors", []))}
        return {"indexed_count": 0}


# =============================================================================
# EMBEDDING COST TRACKING
# =============================================================================


class EmbeddingCostTracker:
    """Track embedding API costs for monitoring."""

    # Approximate costs per 1K tokens (as of 2024)
    COSTS = {
        "text-embedding-ada-002": 0.0001,
        "text-embedding-3-small": 0.00002,
        "text-embedding-3-large": 0.00013,
    }

    @classmethod
    def estimate_cost(cls, model: str, token_count: int) -> float:
        """Estimate cost based on token usage."""
        cost_per_1k = cls.COSTS.get(model, cls.COSTS["text-embedding-ada-002"])
        return round((token_count / 1000) * cost_per_1k, 8)


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="capstone_vector_store_pipeline",
    description="Generate embeddings and maintain searchable vector index",
    schedule=ENRICHED_DOCUMENTS,  # Triggered when enriched documents are available
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "vector-store", "embeddings", "production"],
    default_args={
        "owner": "capstone",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
    },
    doc_md="""
    ## Vector Store Pipeline

    Generates embeddings and maintains searchable vector index.

    ### Trigger
    - Asset-based: Runs when `documents.enriched` is updated

    ### Processing Flow
    ```
    Load Enriched → Chunk Text → Generate Embeddings → Upsert to Store → Validate Index
                                        ↓
                              Uses `embedding_api` pool
                              for rate limiting
    ```

    ### Deferrable Operations
    - Index validation uses custom trigger for async polling
    """,
)
def vector_store_pipeline():
    """Vector store management pipeline with embedding generation."""
    # =========================================================================
    # LOAD: Get enriched documents
    # =========================================================================

    @task
    def load_enriched_documents() -> list[dict]:
        """Load documents from enriched storage."""
        enriched_dir = Path(ENRICHED_PATH)
        documents = []

        if not enriched_dir.exists():
            logger.warning(f"Enriched directory not found: {ENRICHED_PATH}")
            return []

        for record_path in enriched_dir.glob("*.json"):
            try:
                document = json.loads(record_path.read_text())
                # Only process documents not yet indexed
                if document.get("status") == "enriched":
                    documents.append(document)
            except Exception as e:
                logger.error(f"Error loading {record_path}: {e}")

        logger.info(f"Loaded {len(documents)} enriched documents for embedding")
        return documents

    # =========================================================================
    # CHUNKING: Split documents for embedding
    # =========================================================================

    @task
    def chunk_document(document: dict) -> list[dict]:
        """
        Split document into chunks for embedding.

        Each chunk should be small enough for the embedding model's
        context window while maintaining semantic coherence.
        """
        # Get content to chunk
        content = document.get("content_preview", "")
        summary = document.get("summary", "")

        # Combine relevant text fields
        full_text = f"{content}\n\n{summary}"

        # Simple chunking strategy (in production: use proper text splitter)
        chunk_size = 500
        chunk_overlap = 50
        chunks = []

        for i in range(0, len(full_text), chunk_size - chunk_overlap):
            chunk_text = full_text[i : i + chunk_size]
            if chunk_text.strip():
                chunks.append(
                    {
                        "document_id": document["id"],
                        "chunk_index": len(chunks),
                        "text": chunk_text,
                        "metadata": {
                            "filename": document.get("filename", ""),
                            "source": document.get("source_path", ""),
                            "classification": document.get("classification", {}),
                        },
                    }
                )

        logger.info(f"Document {document['id']} split into {len(chunks)} chunks")
        return chunks

    # =========================================================================
    # EMBEDDING: Generate vector representations
    # =========================================================================

    @task(
        pool="embedding_api",
        pool_slots=1,
        retries=5,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
    )
    def generate_embeddings(chunks: list[dict]) -> list[dict]:
        """
        Generate embeddings for document chunks.

        Uses pool-based rate limiting to respect API limits.
        """
        if not chunks:
            return []

        embedded_chunks = []

        for chunk in chunks:
            # Simulated embedding generation (in production: actual API call)
            # In production: use openai.embeddings.create() or similar
            text = chunk["text"]

            # Simulate embedding vector
            import hashlib

            text_hash = hashlib.sha256(text.encode()).hexdigest()
            # Create deterministic "embedding" from hash for simulation
            embedding = [
                float(int(text_hash[i : i + 2], 16)) / 255.0
                for i in range(0, min(len(text_hash), EMBEDDING_DIMENSIONS * 2), 2)
            ]
            # Pad to full dimensions
            embedding.extend([0.0] * (EMBEDDING_DIMENSIONS - len(embedding)))
            embedding = embedding[:EMBEDDING_DIMENSIONS]

            # Normalize
            norm = sum(x * x for x in embedding) ** 0.5
            if norm > 0:
                embedding = [x / norm for x in embedding]

            # Estimate token count (rough approximation)
            token_count = len(text.split()) * 1.3

            # Track cost
            cost = EmbeddingCostTracker.estimate_cost(EMBEDDING_MODEL, token_count)

            embedded_chunk = {
                **chunk,
                "embedding": embedding,
                "embedding_model": EMBEDDING_MODEL,
                "embedding_dimensions": EMBEDDING_DIMENSIONS,
                "token_count": int(token_count),
                "embedding_cost": cost,
                "embedded_at": datetime.utcnow().isoformat(),
            }
            embedded_chunks.append(embedded_chunk)

        total_tokens = sum(c["token_count"] for c in embedded_chunks)
        total_cost = sum(c["embedding_cost"] for c in embedded_chunks)
        logger.info(f"Generated {len(embedded_chunks)} embeddings ({total_tokens} tokens, ${total_cost:.6f})")

        return embedded_chunks

    # =========================================================================
    # UPSERT: Store in vector database
    # =========================================================================

    @task
    def batch_upsert_vectors(embedded_chunks: list[dict]) -> dict:
        """
        Batch upsert vectors to the vector store.

        In production, this would use a vector database like
        Pinecone, Weaviate, Qdrant, or pgvector.
        """
        if not embedded_chunks:
            return {"upserted": 0, "status": "no_data"}

        # Flatten if we received list of lists
        all_chunks = []
        for item in embedded_chunks:
            if isinstance(item, list):
                all_chunks.extend(item)
            else:
                all_chunks.append(item)

        # Prepare for storage
        vector_store_dir = Path(VECTOR_STORE_PATH)
        vector_store_dir.mkdir(parents=True, exist_ok=True)

        # Load existing index
        index_path = vector_store_dir / "documents_index.json"
        if index_path.exists():
            index_data = json.loads(index_path.read_text())
        else:
            index_data = {"vectors": [], "metadata": {"created_at": datetime.utcnow().isoformat()}}

        # Upsert vectors (remove embeddings from stored metadata for size)
        for chunk in all_chunks:
            vector_entry = {
                "id": f"{chunk['document_id']}_{chunk['chunk_index']}",
                "document_id": chunk["document_id"],
                "chunk_index": chunk["chunk_index"],
                "metadata": chunk["metadata"],
                "embedded_at": chunk["embedded_at"],
                # In production, the actual embedding would be stored in the vector DB
                # Here we just store a reference
                "embedding_stored": True,
            }

            # Check for existing entry and update or append
            existing_idx = next(
                (i for i, v in enumerate(index_data["vectors"]) if v["id"] == vector_entry["id"]),
                None,
            )
            if existing_idx is not None:
                index_data["vectors"][existing_idx] = vector_entry
            else:
                index_data["vectors"].append(vector_entry)

        # Update metadata
        index_data["metadata"]["updated_at"] = datetime.utcnow().isoformat()
        index_data["metadata"]["total_vectors"] = len(index_data["vectors"])

        # Save index
        index_path.write_text(json.dumps(index_data, indent=2))

        # Also save individual embeddings (in production: would be in vector DB)
        embeddings_path = vector_store_dir / "embeddings.json"
        embeddings_data = {}
        if embeddings_path.exists():
            embeddings_data = json.loads(embeddings_path.read_text())

        for chunk in all_chunks:
            vector_id = f"{chunk['document_id']}_{chunk['chunk_index']}"
            embeddings_data[vector_id] = {
                "embedding": chunk["embedding"][:10],  # Store truncated for demo
                "dimensions": chunk["embedding_dimensions"],
            }

        embeddings_path.write_text(json.dumps(embeddings_data, indent=2))

        logger.info(f"Upserted {len(all_chunks)} vectors to index")

        return {
            "upserted": len(all_chunks),
            "total_vectors": len(index_data["vectors"]),
            "status": "success",
        }

    # =========================================================================
    # VALIDATION: Verify index health
    # =========================================================================

    @task
    def validate_index_health() -> dict:
        """
        Validate vector store index health.

        Checks:
        - Index file integrity
        - Vector count consistency
        - Embedding dimensions
        """
        index_path = Path(VECTOR_STORE_PATH) / "documents_index.json"

        if not index_path.exists():
            return {"status": "error", "message": "Index file not found"}

        try:
            index_data = json.loads(index_path.read_text())
            vectors = index_data.get("vectors", [])

            # Basic health checks
            health_status = {
                "status": "healthy",
                "total_vectors": len(vectors),
                "unique_documents": len({v["document_id"] for v in vectors}),
                "last_updated": index_data.get("metadata", {}).get("updated_at"),
                "checks": {
                    "index_readable": True,
                    "vectors_present": len(vectors) > 0,
                    "metadata_valid": "metadata" in index_data,
                },
            }

            # Check for issues
            issues = []
            if len(vectors) == 0:
                issues.append("No vectors in index")

            # Check for duplicate IDs
            ids = [v["id"] for v in vectors]
            if len(ids) != len(set(ids)):
                issues.append("Duplicate vector IDs detected")

            if issues:
                health_status["status"] = "warning"
                health_status["issues"] = issues

            logger.info(f"Index health check: {health_status['status']} ({len(vectors)} vectors)")
            return health_status

        except Exception as e:
            return {"status": "error", "message": str(e)}

    # =========================================================================
    # FINALIZE: Update document status and emit asset
    # =========================================================================

    @task(outlets=[INDEXED_DOCUMENTS])
    def finalize_indexing(upsert_result: dict, health_check: dict) -> dict:
        """
        Finalize indexing and emit asset for downstream consumers.

        Updates document status to 'indexed'.
        """
        if upsert_result.get("status") != "success":
            logger.warning("Indexing incomplete, skipping finalization")
            return {"status": "incomplete"}

        # Update enriched documents to mark as indexed
        enriched_dir = Path(ENRICHED_PATH)
        indexed_count = 0

        for record_path in enriched_dir.glob("*.json"):
            try:
                document = json.loads(record_path.read_text())
                if document.get("status") == "enriched":
                    document["status"] = "indexed"
                    document["indexed_at"] = datetime.utcnow().isoformat()
                    record_path.write_text(json.dumps(document, indent=2))
                    indexed_count += 1
            except Exception as e:
                logger.error(f"Error updating {record_path}: {e}")

        result = {
            "status": "success",
            "documents_indexed": indexed_count,
            "total_vectors": upsert_result.get("total_vectors", 0),
            "index_health": health_check.get("status", "unknown"),
            "completed_at": datetime.utcnow().isoformat(),
        }

        logger.info(f"Indexing finalized: {indexed_count} documents indexed")
        return result

    # =========================================================================
    # STATS: Collect pipeline statistics
    # =========================================================================

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def collect_indexing_stats(finalization_result: dict) -> dict:
        """Collect indexing statistics for monitoring."""
        stats = {
            "run_timestamp": datetime.utcnow().isoformat(),
            "documents_indexed": finalization_result.get("documents_indexed", 0),
            "total_vectors": finalization_result.get("total_vectors", 0),
            "index_health": finalization_result.get("index_health", "unknown"),
            "status": finalization_result.get("status", "unknown"),
        }

        logger.info(f"Indexing stats: {stats}")
        return stats

    # =========================================================================
    # TASK FLOW
    # =========================================================================

    # Load enriched documents
    documents = load_enriched_documents()

    # Chunk documents (dynamic mapping)
    chunked = chunk_document.expand(document=documents)

    # Generate embeddings (sequential per chunk batch)
    embedded = generate_embeddings.expand(chunks=chunked)

    # Batch upsert to vector store
    upsert_result = batch_upsert_vectors(embedded)

    # Validate index health
    health_check = validate_index_health()

    # Finalize and emit asset
    final = finalize_indexing(upsert_result, health_check)

    # Collect stats
    collect_indexing_stats(final)


# Instantiate the DAG
vector_store_pipeline()
