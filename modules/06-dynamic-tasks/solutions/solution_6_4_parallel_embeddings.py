"""
Parallel Embedding Generation - Complete Solution.

This solution demonstrates dynamic task mapping for parallel embedding
generation, showing how to process document chunks efficiently using
Airflow's expand() functionality.

Key patterns demonstrated:
- Dynamic task mapping with expand()
- Parallel chunk processing
- Failure handling in mapped tasks
- Result aggregation from mapped outputs
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

# Configuration
EMBEDDING_DIMENSION = 384
BATCH_SIZE = 5
SIMULATE_FAILURES = True  # Enable to test failure handling


# =============================================================================
# Helper Functions
# =============================================================================


def mock_embedding(text: str, dimension: int = EMBEDDING_DIMENSION) -> list[float]:
    """Generate deterministic mock embedding."""
    hash_bytes = hashlib.sha256(text.encode()).digest()
    extended = (hash_bytes * (dimension // len(hash_bytes) + 1))[:dimension]
    return [b / 255.0 for b in extended]


def chunk_text(text: str, chunk_size: int = 500) -> list[str]:
    """Split text into chunks of approximate size."""
    words = text.split()
    chunks = []
    current_chunk: list[str] = []
    current_size = 0

    for word in words:
        current_chunk.append(word)
        current_size += len(word) + 1

        if current_size >= chunk_size:
            chunks.append(" ".join(current_chunk))
            current_chunk = []
            current_size = 0

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    return chunks


def should_simulate_failure(chunk_id: str) -> bool:
    """Deterministically decide if a chunk should fail (for testing)."""
    if not SIMULATE_FAILURES:
        return False
    # Fail chunks where hash ends with specific values
    chunk_hash = hashlib.md5(chunk_id.encode()).hexdigest()
    return chunk_hash[-1] in ["a", "f"]  # ~12.5% failure rate


# =============================================================================
# DAG Definition
# =============================================================================


@dag(
    dag_id="parallel_embeddings",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    tags=["ai-ml", "dynamic-tasks", "module-06"],
    doc_md=__doc__,
)
def parallel_embeddings():
    """
    Parallel Embedding Generation Pipeline.

    Demonstrates dynamic task mapping for processing
    document chunks in parallel.
    """

    @task
    def get_documents() -> list[dict[str, Any]]:
        """
        Get sample documents for processing.

        Returns:
            List of document dicts with id and content.
        """
        documents = [
            {
                "id": "doc-001",
                "title": "Introduction to AI",
                "content": """
                Artificial intelligence is the simulation of human intelligence
                by machines. It includes learning, reasoning, and self-correction.
                AI systems can be trained on large datasets to recognize patterns
                and make decisions. Machine learning is a subset of AI that focuses
                on algorithms that improve through experience.
                """,
            },
            {
                "id": "doc-002",
                "title": "Deep Learning Fundamentals",
                "content": """
                Deep learning uses neural networks with multiple layers.
                Each layer learns increasingly abstract features from the data.
                Convolutional networks excel at image processing tasks.
                Recurrent networks handle sequential data like text and time series.
                Transformers have revolutionized natural language processing.
                """,
            },
            {
                "id": "doc-003",
                "title": "Vector Databases",
                "content": """
                Vector databases store and search high-dimensional embeddings.
                They enable semantic search across large document collections.
                Popular options include Pinecone, Weaviate, and Chroma.
                Efficient similarity search uses approximate nearest neighbor algorithms.
                """,
            },
        ]

        logger.info(f"Loaded {len(documents)} documents for processing")
        return documents

    @task
    def create_chunks(documents: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Split documents into chunks for embedding.

        Args:
            documents: List of documents to chunk.

        Returns:
            Flattened list of chunk dicts.
        """
        all_chunks = []

        for doc in documents:
            content = doc.get("content", "")
            chunks = chunk_text(content.strip(), chunk_size=200)

            for idx, chunk_content in enumerate(chunks):
                chunk_id = f"{doc['id']}-chunk-{idx:03d}"
                all_chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "document_id": doc["id"],
                        "document_title": doc.get("title", "Unknown"),
                        "chunk_index": idx,
                        "total_chunks": len(chunks),
                        "text": chunk_content,
                        "char_count": len(chunk_content),
                    }
                )

        logger.info(f"Created {len(all_chunks)} chunks from {len(documents)} documents")
        return all_chunks

    @task
    def generate_embedding(chunk: dict[str, Any]) -> dict[str, Any]:
        """
        Generate embedding for a single chunk.

        This task is mapped across all chunks - Airflow creates
        one task instance per chunk, running them in parallel.

        Args:
            chunk: Single chunk dict.

        Returns:
            Chunk with embedding added.
        """
        chunk_id = chunk.get("chunk_id", "unknown")
        text = chunk.get("text", "")

        # Simulate processing time (API call latency)
        time.sleep(0.05)

        # Simulate occasional failures for testing resilience
        if should_simulate_failure(chunk_id):
            logger.warning(f"Simulated failure for chunk: {chunk_id}")
            return {
                "chunk_id": chunk_id,
                "document_id": chunk.get("document_id"),
                "embedding": [],
                "success": False,
                "error": "Simulated API failure",
            }

        # Generate embedding
        embedding = mock_embedding(text)

        logger.debug(f"Generated embedding for {chunk_id}: dim={len(embedding)}")

        return {
            "chunk_id": chunk_id,
            "document_id": chunk.get("document_id"),
            "chunk_index": chunk.get("chunk_index"),
            "embedding": embedding,
            "embedding_dim": len(embedding),
            "char_count": chunk.get("char_count"),
            "success": True,
            "error": None,
        }

    @task
    def aggregate_embeddings(
        embeddings: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Aggregate embeddings from all mapped tasks.

        Args:
            embeddings: List of results from mapped tasks.
                       This is automatically collected from all
                       task instances created by expand().

        Returns:
            Aggregated summary with statistics.
        """
        successful = [e for e in embeddings if e.get("success", False)]
        failed = [e for e in embeddings if not e.get("success", False)]

        # Group by document
        by_document: dict[str, list[dict[str, Any]]] = {}
        for emb in successful:
            doc_id = emb.get("document_id", "unknown")
            if doc_id not in by_document:
                by_document[doc_id] = []
            by_document[doc_id].append(emb)

        # Calculate stats
        success_rate = len(successful) / len(embeddings) if embeddings else 0

        summary = {
            "total_chunks": len(embeddings),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": round(success_rate, 3),
            "documents_processed": len(by_document),
            "chunks_per_document": {doc_id: len(chunks) for doc_id, chunks in by_document.items()},
            "failed_chunk_ids": [e["chunk_id"] for e in failed],
            "failure_reasons": [e.get("error", "Unknown") for e in failed],
            "embeddings": successful,
        }

        logger.info(
            f"Aggregation complete: {len(successful)}/{len(embeddings)} successful "
            f"({success_rate:.1%}), {len(by_document)} documents"
        )

        if failed:
            logger.warning(f"Failed chunks: {summary['failed_chunk_ids']}")

        return summary

    @task
    def store_embeddings(summary: dict[str, Any]) -> dict[str, Any]:
        """
        Store successful embeddings to vector store.

        Args:
            summary: Aggregated embeddings and statistics.

        Returns:
            Storage confirmation.
        """
        embeddings = summary.get("embeddings", [])

        # Simulate batch storage
        stored_count = 0
        for emb in embeddings:
            # Simulate upsert
            time.sleep(0.01)
            stored_count += 1

        logger.info(f"Stored {stored_count} embeddings to vector store ({summary.get('failed', 0)} failed)")

        return {
            "stored_count": stored_count,
            "failed_count": summary.get("failed", 0),
            "documents_indexed": summary.get("documents_processed", 0),
            "timestamp": datetime.now().isoformat(),
        }

    # =========================================================================
    # DAG Flow
    # =========================================================================

    # Get documents
    documents = get_documents()

    # Create chunks from documents
    chunks = create_chunks(documents)

    # Generate embeddings in parallel using expand()
    # This creates one task instance per chunk, all running in parallel
    embeddings = generate_embedding.expand(chunk=chunks)

    # Aggregate results from all mapped tasks
    # The `embeddings` list contains outputs from all parallel tasks
    summary = aggregate_embeddings(embeddings)

    # Store to vector store
    store_embeddings(summary)


# Instantiate the DAG
parallel_embeddings()
