"""
Asset-Driven Embedding Regeneration - Complete Solution.

This solution demonstrates asset-driven scheduling for AI/ML pipelines,
showing how document changes automatically trigger embedding regeneration
and vector store updates.

Key patterns demonstrated:
- Asset definitions with metadata
- Producer DAG emitting assets with context
- Consumer DAG triggered by asset updates
- Consumer-producer chain for pipeline stages
- Accessing triggering asset event metadata
"""

from __future__ import annotations

import hashlib
import logging
import time
import uuid
from datetime import datetime
from typing import Any

from airflow.sdk import Asset, dag, task

logger = logging.getLogger(__name__)

# =============================================================================
# Asset Definitions
# =============================================================================

# Document source asset - updated when new documents arrive
documents_asset = Asset(
    uri="file:///data/documents/",
    extra={"format": "mixed", "source": "upload_system"},
)

# Embeddings asset - updated when embeddings are generated
embeddings_asset = Asset(
    uri="file:///data/embeddings/",
    extra={"model": "text-embedding-3-small", "dimension": 384},
)

# Vector store asset - updated when store is refreshed
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


def generate_batch_id() -> str:
    """Generate a unique batch identifier."""
    return f"batch-{uuid.uuid4().hex[:8]}"


# =============================================================================
# DAG 1: Document Producer
# =============================================================================


@dag(
    dag_id="embedding_doc_producer",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    tags=["ai-ml", "assets", "module-05"],
    doc_md="""
    ## Document Producer

    Simulates document upload and emits the documents asset.
    In production, this would be triggered by a file sensor or webhook.
    """,
)
def embedding_doc_producer():
    """Document Producer DAG - emits documents asset when new docs arrive."""

    @task(outlets=[documents_asset])
    def upload_documents() -> dict[str, Any]:
        """
        Simulate document upload and emit asset.

        Returns:
            Metadata about uploaded documents (becomes asset extra).
        """
        batch_id = generate_batch_id()

        # Simulated documents (in production, fetch from upload system)
        documents = [
            {
                "id": "doc-001",
                "title": "AI Overview",
                "content": "Introduction to artificial intelligence and its applications...",
                "size_bytes": 1024,
            },
            {
                "id": "doc-002",
                "title": "ML Basics",
                "content": "Machine learning is a subset of AI focusing on data-driven learning...",
                "size_bytes": 2048,
            },
            {
                "id": "doc-003",
                "title": "Deep Learning",
                "content": "Neural networks with multiple layers enable deep learning...",
                "size_bytes": 3072,
            },
        ]

        # Simulate upload time
        time.sleep(0.1)

        total_size = sum(d["size_bytes"] for d in documents)

        logger.info(f"Uploaded {len(documents)} documents (batch_id={batch_id}, total_size={total_size} bytes)")

        # Return metadata - this becomes available in triggering_asset_events
        return {
            "documents_added": len(documents),
            "batch_id": batch_id,
            "document_ids": [d["id"] for d in documents],
            "total_size_bytes": total_size,
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
    doc_md="""
    ## Embedding Generator

    Triggered when new documents are available.
    Generates embeddings and produces the embeddings asset.
    """,
)
def embedding_generator():
    """Embedding Generator DAG - consumes documents, produces embeddings."""

    @task
    def get_triggering_info(**context: Any) -> dict[str, Any]:
        """
        Extract information from triggering asset event.

        Returns:
            Info about what triggered this DAG.
        """
        triggering_events = context.get("triggering_asset_events", {})

        trigger_info = {
            "batch_id": None,
            "documents_to_process": 0,
            "document_ids": [],
            "triggered_by": None,
        }

        # Process triggering events
        for asset_uri, events in triggering_events.items():
            trigger_info["triggered_by"] = str(asset_uri)

            for event in events:
                # Access metadata from producer via event.extra
                event_extra = event.extra if hasattr(event, "extra") else {}

                trigger_info["batch_id"] = event_extra.get("batch_id", "unknown")
                trigger_info["documents_to_process"] = event_extra.get("documents_added", 0)
                trigger_info["document_ids"] = event_extra.get("document_ids", [])

                logger.info(
                    f"Triggered by {asset_uri}: "
                    f"batch_id={trigger_info['batch_id']}, "
                    f"docs={trigger_info['documents_to_process']}"
                )

        return trigger_info

    @task
    def load_documents(trigger_info: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Load documents that need embedding.

        Args:
            trigger_info: Information about triggering event.

        Returns:
            List of documents to embed.
        """
        batch_id = trigger_info.get("batch_id", "unknown")
        document_ids = trigger_info.get("document_ids", [])

        logger.info(f"Loading documents for batch: {batch_id}, ids: {document_ids}")

        # In production, query document store by IDs
        # Mock document loading with realistic content
        documents = [
            {
                "id": "doc-001",
                "content": "Introduction to artificial intelligence and its applications. "
                "AI encompasses machine learning, natural language processing, and robotics.",
            },
            {
                "id": "doc-002",
                "content": "Machine learning fundamentals cover supervised, unsupervised, "
                "and reinforcement learning approaches for pattern recognition.",
            },
            {
                "id": "doc-003",
                "content": "Deep neural networks explained: layers, activation functions, "
                "backpropagation, and modern architectures like transformers.",
            },
        ]

        return documents

    @task(outlets=[embeddings_asset])
    def generate_embeddings(documents: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Generate embeddings for documents.

        Args:
            documents: Documents to embed.

        Returns:
            Metadata about generated embeddings (becomes asset extra).
        """
        embeddings_data = []

        for doc in documents:
            doc_id = doc.get("id", "unknown")
            content = doc.get("content", "")

            # Generate embedding
            embedding = mock_embedding(content)

            embeddings_data.append(
                {
                    "document_id": doc_id,
                    "embedding_dim": len(embedding),
                    "content_hash": hashlib.md5(content.encode()).hexdigest()[:8],
                }
            )

            logger.debug(f"Generated embedding for {doc_id}: dim={len(embedding)}")

        logger.info(f"Generated {len(embeddings_data)} embeddings")

        # Return metadata for downstream consumers
        return {
            "embeddings_generated": len(embeddings_data),
            "embedding_ids": [e["document_id"] for e in embeddings_data],
            "model": "text-embedding-3-small",
            "dimension": 384,
            "processing_batch_id": generate_batch_id(),
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
    doc_md="""
    ## Vector Store Updater

    Final consumer in the pipeline chain.
    Updates the vector store when new embeddings are available.
    """,
)
def embedding_vectorstore_updater():
    """Vector Store Updater DAG - final consumer that updates vector store."""

    @task
    def get_embedding_info(**context: Any) -> dict[str, Any]:
        """
        Extract information from triggering embeddings event.

        Returns:
            Info about embeddings to upsert.
        """
        triggering_events = context.get("triggering_asset_events", {})

        embedding_info = {
            "embeddings_count": 0,
            "embedding_ids": [],
            "model": "unknown",
            "triggered_at": datetime.now().isoformat(),
        }

        for asset_uri, events in triggering_events.items():
            for event in events:
                event_extra = event.extra if hasattr(event, "extra") else {}

                embedding_info["embeddings_count"] = event_extra.get("embeddings_generated", 0)
                embedding_info["embedding_ids"] = event_extra.get("embedding_ids", [])
                embedding_info["model"] = event_extra.get("model", "unknown")
                embedding_info["source_batch_id"] = event_extra.get("processing_batch_id")

                logger.info(f"Triggered by {asset_uri}: {embedding_info['embeddings_count']} embeddings to upsert")

        return embedding_info

    @task
    def load_embeddings(embedding_info: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Load embeddings from storage.

        Args:
            embedding_info: Info about which embeddings to load.

        Returns:
            Embeddings with vectors ready for upsert.
        """
        embedding_ids = embedding_info.get("embedding_ids", [])

        # In production, load actual embeddings from storage
        # Mock embeddings with realistic structure
        embeddings = []
        for emb_id in embedding_ids:
            embeddings.append(
                {
                    "id": emb_id,
                    "vector": mock_embedding(f"content-for-{emb_id}"),
                    "metadata": {"source": "document", "processed_at": datetime.now().isoformat()},
                }
            )

        logger.info(f"Loaded {len(embeddings)} embeddings for upsert")
        return embeddings

    @task
    def update_vector_store(embeddings: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Update vector store with new embeddings.

        Args:
            embeddings: Embeddings with vectors to upsert.

        Returns:
            Update statistics.
        """
        # Simulate vector store operations
        upserted = 0
        updated = 0
        skipped = 0

        for emb in embeddings:
            # Simulate upsert logic
            time.sleep(0.01)
            upserted += 1

        logger.info(f"Vector store update complete: upserted={upserted}, updated={updated}, skipped={skipped}")

        return {
            "vectors_upserted": upserted,
            "vectors_updated": updated,
            "vectors_skipped": skipped,
            "total_vectors_processed": len(embeddings),
            "update_timestamp": datetime.now().isoformat(),
        }

    @task
    def log_pipeline_complete(update_stats: dict[str, Any]) -> None:
        """Log pipeline completion with summary."""
        logger.info(
            f"Embedding pipeline complete: "
            f"total_processed={update_stats['total_vectors_processed']}, "
            f"upserted={update_stats['vectors_upserted']}, "
            f"updated={update_stats['vectors_updated']}"
        )

    # DAG flow
    embedding_info = get_embedding_info()
    embeddings = load_embeddings(embedding_info)
    update_stats = update_vector_store(embeddings)
    log_pipeline_complete(update_stats)


# Instantiate DAGs
embedding_doc_producer()
embedding_generator()
embedding_vectorstore_updater()
