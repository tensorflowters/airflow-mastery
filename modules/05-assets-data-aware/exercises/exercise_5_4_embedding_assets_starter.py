"""
Asset-Driven Embedding Regeneration Starter Code.

Build an asset-driven pipeline that automatically regenerates embeddings
when source documents change, demonstrating data-aware scheduling for AI/ML.

Requirements:
1. Define Assets for documents, embeddings, and vector store
2. Create a producer DAG that emits document asset
3. Create a consumer DAG that generates embeddings
4. Create a final consumer that updates vector store

Instructions:
- Complete all TODO sections
- Deploy all three DAGs to see the chain in action
- Test by triggering the document producer
"""

from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from typing import Any

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# =============================================================================
# Asset Definitions
# =============================================================================

# TODO: Define assets for the pipeline
# Each asset should have a unique URI and optional metadata

# Document source asset
documents_asset = Asset(
    uri="file:///data/documents/",
    extra={"format": "mixed", "source": "upload_system"},
)

# Embeddings asset (produced after document processing)
embeddings_asset = Asset(
    uri="file:///data/embeddings/",
    extra={"model": "text-embedding-3-small", "dimension": 384},
)

# Vector store asset (final destination)
vector_store_asset = Asset(
    uri="file:///data/vectorstore/",
    extra={"store_type": "simple", "index": "documents"},
)


# =============================================================================
# Helper Functions
# =============================================================================


def mock_embedding(text: str, dimension: int = 384) -> list[float]:
    """Generate deterministic mock embedding for testing."""
    hash_bytes = hashlib.sha256(text.encode()).digest()
    extended = (hash_bytes * (dimension // len(hash_bytes) + 1))[:dimension]
    return [b / 255.0 for b in extended]


# =============================================================================
# DAG 1: Document Producer
# =============================================================================


@dag(
    dag_id="embedding_doc_producer",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    tags=["ai-ml", "assets", "module-05"],
)
def embedding_doc_producer():
    """
    Document Producer DAG.

    Simulates document upload and emits the documents asset.
    """

    @task(outlets=[documents_asset])
    def upload_documents() -> dict[str, Any]:
        """
        Simulate document upload and emit asset.

        Returns:
            Metadata about uploaded documents (becomes asset extra).
        """
        # TODO: Implement document upload simulation
        # 1. Simulate fetching/uploading documents
        # 2. Return metadata including:
        #    - documents_added: count of new documents
        #    - batch_id: unique batch identifier
        #    - timestamp: upload time

        # Simulated documents
        documents = [
            {"id": "doc-001", "title": "AI Overview", "content": "Introduction to AI..."},
            {"id": "doc-002", "title": "ML Basics", "content": "Machine learning is..."},
            {"id": "doc-003", "title": "Deep Learning", "content": "Neural networks..."},
        ]

        logger.info(f"Uploaded {len(documents)} documents")

        # TODO: Return proper metadata
        return {
            "documents_added": 0,
            "batch_id": "unknown",
            "timestamp": datetime.now().isoformat(),
        }

    upload_documents()


# =============================================================================
# DAG 2: Embedding Generator (Consumer -> Producer)
# =============================================================================


@dag(
    dag_id="embedding_generator",
    start_date=datetime(2024, 1, 1),
    schedule=documents_asset,  # Triggered by document asset
    catchup=False,
    tags=["ai-ml", "assets", "module-05"],
)
def embedding_generator():
    """
    Embedding Generator DAG.

    Consumes documents asset, generates embeddings, produces embeddings asset.
    """

    @task
    def get_triggering_info(**context: Any) -> dict[str, Any]:
        """
        Extract information from triggering asset event.

        Returns:
            Info about what triggered this DAG.
        """
        # TODO: Implement triggering info extraction
        # 1. Access context["triggering_asset_events"]
        # 2. Extract metadata from the event
        # 3. Return batch_id and document count

        # Hint: triggering_asset_events is a dict of {asset_uri: [events]}

        return {
            "batch_id": "unknown",
            "documents_to_process": 0,
            "triggered_by": "unknown",
        }

    @task
    def load_documents(trigger_info: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Load documents that need embedding.

        Args:
            trigger_info: Information about triggering event.

        Returns:
            List of documents to embed.
        """
        # TODO: Load documents based on trigger info
        # In production, this would query the document store

        batch_id = trigger_info.get("batch_id", "unknown")
        logger.info(f"Loading documents for batch: {batch_id}")

        # Mock document loading
        return [
            {"id": "doc-001", "content": "Introduction to artificial intelligence..."},
            {"id": "doc-002", "content": "Machine learning fundamentals..."},
            {"id": "doc-003", "content": "Deep neural networks explained..."},
        ]

    @task(outlets=[embeddings_asset])
    def generate_embeddings(documents: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Generate embeddings for documents.

        Args:
            documents: Documents to embed.

        Returns:
            Metadata about generated embeddings (becomes asset extra).
        """
        # TODO: Implement embedding generation
        # 1. For each document, generate embedding using mock_embedding
        # 2. Store embeddings (simulated)
        # 3. Return metadata for the asset event

        embeddings_generated = 0

        for doc in documents:
            content = doc.get("content", "")
            # TODO: Generate embedding using mock_embedding(content)
            _ = content  # Use in your implementation
            embeddings_generated += 1

        logger.info(f"Generated {embeddings_generated} embeddings")

        return {
            "embeddings_generated": embeddings_generated,
            "model": "text-embedding-3-small",
            "dimension": 384,
            "timestamp": datetime.now().isoformat(),
        }

    # DAG flow
    trigger_info = get_triggering_info()
    docs = load_documents(trigger_info)
    generate_embeddings(docs)


# =============================================================================
# DAG 3: Vector Store Updater (Final Consumer)
# =============================================================================


@dag(
    dag_id="embedding_vectorstore_updater",
    start_date=datetime(2024, 1, 1),
    schedule=embeddings_asset,  # Triggered by embeddings asset
    catchup=False,
    tags=["ai-ml", "assets", "module-05"],
)
def embedding_vectorstore_updater():
    """
    Vector Store Updater DAG.

    Final consumer that updates the vector store with new embeddings.
    """

    @task
    def get_embedding_info(**context: Any) -> dict[str, Any]:
        """
        Extract information from triggering embeddings event.

        Returns:
            Info about embeddings to upsert.
        """
        # TODO: Extract triggering asset event info
        # Similar to get_triggering_info in the generator DAG

        return {
            "embeddings_count": 0,
            "model": "unknown",
            "triggered_at": datetime.now().isoformat(),
        }

    @task
    def update_vector_store(embedding_info: dict[str, Any]) -> dict[str, Any]:
        """
        Update vector store with new embeddings.

        Args:
            embedding_info: Info about embeddings to add.

        Returns:
            Update statistics.
        """
        # TODO: Implement vector store update
        # 1. Load embeddings from storage
        # 2. Upsert to vector store
        # 3. Return statistics

        embeddings_count = embedding_info.get("embeddings_count", 0)

        # Simulate vector store update
        time.sleep(0.1)

        logger.info(f"Updated vector store with {embeddings_count} embeddings")

        return {
            "vectors_upserted": embeddings_count,
            "vectors_updated": 0,
            "vectors_skipped": 0,
            "update_timestamp": datetime.now().isoformat(),
        }

    @task
    def log_pipeline_complete(update_stats: dict[str, Any]) -> None:
        """Log pipeline completion."""
        logger.info(
            f"Pipeline complete: upserted={update_stats['vectors_upserted']}, updated={update_stats['vectors_updated']}"
        )

    # DAG flow
    embedding_info = get_embedding_info()
    update_stats = update_vector_store(embedding_info)
    log_pipeline_complete(update_stats)


# Instantiate DAGs
embedding_doc_producer()
embedding_generator()
embedding_vectorstore_updater()
