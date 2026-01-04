"""
Parallel Embedding Generation Starter Code.

Build a dynamic task mapping pipeline for parallel embedding generation,
demonstrating expand() patterns for AI/ML workloads.

Requirements:
1. Use expand() to process chunks in parallel
2. Implement batching for API efficiency
3. Aggregate results from mapped tasks
4. Handle failures in individual tasks

Instructions:
- Complete all TODO sections
- Run with: airflow dags test parallel_embeddings <date>
- Experiment with different batch sizes
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
SIMULATE_FAILURES = True  # Set to True to test failure handling


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
        return [
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
                        "chunk_index": idx,
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

        This task will be mapped across all chunks in parallel.

        Args:
            chunk: Single chunk dict.

        Returns:
            Chunk with embedding added.
        """
        # TODO: Implement embedding generation
        # 1. Extract text from chunk
        # 2. Optionally simulate random failures for testing
        # 3. Generate embedding using mock_embedding()
        # 4. Return result with embedding

        chunk_id = chunk.get("chunk_id", "unknown")
        text = chunk.get("text", "")

        # Simulate processing time
        time.sleep(0.05)

        # TODO: Simulate occasional failures if SIMULATE_FAILURES is True
        # Hint: Use chunk_id hash to deterministically fail some chunks

        # TODO: Generate embedding
        _ = text  # Use in your implementation

        return {
            "chunk_id": chunk_id,
            "document_id": chunk.get("document_id"),
            "embedding": [],  # TODO: Replace with actual embedding
            "success": False,
            "error": "Not implemented",
        }

    @task
    def aggregate_embeddings(
        embeddings: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Aggregate embeddings from all mapped tasks.

        Args:
            embeddings: List of results from mapped tasks.

        Returns:
            Aggregated summary with statistics.
        """
        # TODO: Implement aggregation
        # 1. Separate successful and failed embeddings
        # 2. Group embeddings by document
        # 3. Calculate statistics
        # 4. Return summary

        successful = [e for e in embeddings if e.get("success", False)]
        failed = [e for e in embeddings if not e.get("success", False)]

        summary = {
            "total_chunks": len(embeddings),
            "successful": len(successful),
            "failed": len(failed),
            "success_rate": len(successful) / len(embeddings) if embeddings else 0,
            "failed_chunk_ids": [e["chunk_id"] for e in failed],
            "embeddings": successful,
        }

        logger.info(f"Aggregation complete: {len(successful)}/{len(embeddings)} successful")

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

        # Simulate storage
        time.sleep(0.1)

        logger.info(f"Stored {len(embeddings)} embeddings to vector store")

        return {
            "stored_count": len(embeddings),
            "failed_count": summary.get("failed", 0),
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
    # TODO: Use .expand() to create one task per chunk
    embeddings = generate_embedding.expand(chunk=chunks)

    # Aggregate results from all mapped tasks
    summary = aggregate_embeddings(embeddings)

    # Store to vector store
    store_embeddings(summary)


# Instantiate the DAG
parallel_embeddings()
