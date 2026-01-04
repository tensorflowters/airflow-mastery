"""
RAG Ingestion Pipeline Starter Code.

Build a complete RAG pipeline that ingests documents, generates embeddings,
and updates a vector store with incremental processing.

Requirements:
1. Discover new/modified documents in a source directory
2. Parse and chunk documents into embedding-ready segments
3. Generate embeddings with batching and rate limiting
4. Store embeddings in a vector store with metadata
5. Track processing state for incremental updates

Instructions:
- Complete all TODO sections
- Run with: airflow dags test rag_ingestion_pipeline <date>
- Test incrementally by adding documents to the source directory
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.sdk import dag, task

# Configuration
SOURCE_DIR = os.environ.get("RAG_SOURCE_DIR", "/tmp/rag_docs")
CHUNK_SIZE = 800  # tokens (approximate)
CHUNK_OVERLAP = 100  # tokens
EMBEDDING_BATCH_SIZE = 100
EMBEDDING_DIMENSION = 384  # OpenAI text-embedding-3-small compatible
USE_MOCK_EMBEDDINGS = True  # Set to False to use real OpenAI API

logger = logging.getLogger(__name__)


# =============================================================================
# Helper Functions (provided)
# =============================================================================


def get_file_hash(filepath: str) -> str:
    """Calculate MD5 hash of file contents."""
    with open(filepath, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def estimate_tokens(text: str) -> int:
    """Estimate token count (approximately 1 token per 4 characters for English)."""
    return len(text) // 4


def mock_embedding(text: str) -> list[float]:
    """Generate deterministic mock embedding for testing."""
    hash_bytes = hashlib.sha256(text.encode()).digest()
    # Extend to full dimension
    extended = (hash_bytes * (EMBEDDING_DIMENSION // len(hash_bytes) + 1))[:EMBEDDING_DIMENSION]
    return [b / 255.0 for b in extended]


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Split text into overlapping chunks by approximate token count."""
    # Convert token counts to character counts (rough approximation)
    char_chunk_size = chunk_size * 4
    char_overlap = overlap * 4

    chunks = []
    start = 0

    while start < len(text):
        end = start + char_chunk_size

        # Try to break at sentence boundary
        if end < len(text):
            # Look for sentence end within last 20% of chunk
            search_start = end - (char_chunk_size // 5)
            for sep in [". ", ".\n", "! ", "? "]:
                last_sep = text.rfind(sep, search_start, end)
                if last_sep != -1:
                    end = last_sep + 1
                    break

        chunks.append(text[start:end].strip())
        start = end - char_overlap

    return [c for c in chunks if c]  # Remove empty chunks


# =============================================================================
# In-Memory Vector Store (for local testing)
# =============================================================================


class SimpleVectorStore:
    """Simple in-memory vector store for testing."""

    def __init__(self, persist_path: str = "/tmp/rag_vectors.json") -> None:
        """Initialize the vector store with optional persistence path."""
        self.persist_path = persist_path
        self.vectors: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        """Load vectors from disk if available."""
        if os.path.exists(self.persist_path):
            with open(self.persist_path) as f:
                self.vectors = json.load(f)

    def _save(self) -> None:
        """Persist vectors to disk."""
        with open(self.persist_path, "w") as f:
            json.dump(self.vectors, f)

    def upsert(self, doc_id: str, embedding: list[float], metadata: dict[str, Any]) -> str:
        """
        Upsert a vector with metadata.

        Returns: 'inserted', 'updated', or 'skipped'
        """
        # TODO: Implement idempotent upsert
        # - Check if doc_id already exists
        # - If exists and embedding matches, return 'skipped'
        # - If exists and embedding differs, update and return 'updated'
        # - If new, insert and return 'inserted'
        # - Don't forget to call _save() after modifications
        pass

    def get(self, doc_id: str) -> dict[str, Any] | None:
        """Get a vector by ID."""
        return self.vectors.get(doc_id)

    def count(self) -> int:
        """Return total vector count."""
        return len(self.vectors)


# =============================================================================
# DAG Definition
# =============================================================================


@dag(
    dag_id="rag_ingestion_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["ai-ml", "rag", "module-15"],
    doc_md=__doc__,
)
def rag_ingestion_pipeline():
    """
    RAG Ingestion Pipeline.

    Processes documents from a source directory, generates embeddings,
    and stores them in a vector store for retrieval.
    """

    @task
    def discover_documents() -> list[dict[str, Any]]:
        """
        Discover new or modified documents that need processing.

        Returns:
            List of document metadata dicts with keys:
            - path: str (absolute file path)
            - filename: str
            - file_type: str ('pdf' or 'markdown')
            - file_hash: str (MD5 hash)
            - modified_time: str (ISO format)
        """
        # TODO: Implement document discovery
        # 1. Scan SOURCE_DIR for .pdf and .md files
        # 2. Load previously processed documents from Airflow Variable "rag_processed_docs"
        # 3. Compare file hashes to identify new/modified documents
        # 4. Return list of documents needing processing
        #
        # Hint: Use Variable.get("rag_processed_docs", default_var={}, deserialize_json=True)

        logger.info(f"Scanning source directory: {SOURCE_DIR}")

        # Create source dir if it doesn't exist (for testing)
        Path(SOURCE_DIR).mkdir(parents=True, exist_ok=True)

        documents_to_process: list[dict[str, Any]] = []

        # TODO: Your implementation here

        logger.info(f"Found {len(documents_to_process)} documents to process")
        return documents_to_process

    @task
    def parse_document(doc_metadata: dict[str, Any]) -> dict[str, Any]:
        """
        Parse a single document and extract text content.

        Args:
            doc_metadata: Document metadata from discover_documents

        Returns:
            Dict with keys:
            - content: str (extracted text)
            - metadata: dict (original metadata)
            - parse_errors: list[str] (any parsing errors)
        """
        # TODO: Implement document parsing
        # 1. Read the file based on file_type
        # 2. For .md files: read directly
        # 3. For .pdf files: extract text (you can mock this for now)
        # 4. Return content with metadata

        filepath = doc_metadata["path"]
        file_type = doc_metadata.get("file_type", "unknown")

        logger.info(f"Parsing document: {filepath} (type: {file_type})")

        content = ""
        parse_errors: list[str] = []

        # TODO: Your implementation here

        return {
            "content": content,
            "metadata": doc_metadata,
            "parse_errors": parse_errors,
        }

    @task
    def chunk_documents(parsed_docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Split parsed documents into chunks for embedding.

        Args:
            parsed_docs: List of parsed document dicts

        Returns:
            List of chunk dicts with keys:
            - chunk_id: str (deterministic hash)
            - text: str (chunk content)
            - chunk_index: int
            - source_file: str
            - token_estimate: int
        """
        # TODO: Implement document chunking
        # 1. For each document, use chunk_text() to split content
        # 2. Generate chunk_id using source_file, chunk_index, and content hash
        # 3. Estimate tokens for each chunk
        # 4. Return flattened list of all chunks

        all_chunks: list[dict[str, Any]] = []

        # TODO: Your implementation here

        logger.info(f"Created {len(all_chunks)} chunks from {len(parsed_docs)} documents")
        return all_chunks

    @task
    def generate_embeddings(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Generate embeddings for all chunks with batching.

        Args:
            chunks: List of chunk dicts from chunk_documents

        Returns:
            List of embedding dicts with keys:
            - chunk_id: str
            - embedding: list[float]
            - text: str
            - metadata: dict
            - token_usage: int
        """
        # TODO: Implement embedding generation with batching
        # 1. Process chunks in batches of EMBEDDING_BATCH_SIZE
        # 2. Use mock_embedding() if USE_MOCK_EMBEDDINGS is True
        # 3. Otherwise, call OpenAI API (if implemented)
        # 4. Add rate limiting delay between batches
        # 5. Track total token usage

        embeddings: list[dict[str, Any]] = []
        total_tokens = 0

        # TODO: Your implementation here
        # Use time.sleep() for rate limiting between batches

        logger.info(f"Generated {len(embeddings)} embeddings, total tokens: {total_tokens}")
        return embeddings

    @task
    def upsert_to_vector_store(embeddings: list[dict[str, Any]]) -> dict[str, int]:
        """
        Upsert embeddings to the vector store.

        Args:
            embeddings: List of embedding dicts from generate_embeddings

        Returns:
            Dict with counts: inserted, updated, skipped
        """
        # TODO: Implement vector store upsert
        # 1. Initialize SimpleVectorStore
        # 2. Upsert each embedding
        # 3. Track and return statistics

        vector_store = SimpleVectorStore()
        stats = {"inserted": 0, "updated": 0, "skipped": 0}

        # TODO: Your implementation here using vector_store.upsert()

        logger.info(f"Vector store upsert complete: {stats}, total vectors: {vector_store.count()}")
        return stats

    @task
    def update_processing_state(
        processed_docs: list[dict[str, Any]],
        upsert_stats: dict[str, int],
    ) -> None:
        """
        Update the processing state to track completed documents.

        Args:
            processed_docs: List of successfully processed document metadata
            upsert_stats: Statistics from vector store upsert
        """
        # TODO: Implement processing state update
        # 1. Load existing state from Variable "rag_processed_docs"
        # 2. Add newly processed documents with their hashes
        # 3. Save updated state back to Variable

        _ = upsert_stats  # Use upsert_stats for logging if needed

        # TODO: Your implementation here

        logger.info(f"Updated processing state for {len(processed_docs)} documents")

    @task
    def summarize_run(
        documents: list[dict[str, Any]],
        chunks: list[dict[str, Any]],
        embeddings: list[dict[str, Any]],
        upsert_stats: dict[str, int],
    ) -> dict[str, Any]:
        """
        Generate a summary of the pipeline run.

        Returns:
            Summary dict with processing statistics and costs.
        """
        # TODO: Implement run summary
        # 1. Count documents, chunks, embeddings
        # 2. Estimate costs (OpenAI embedding pricing: $0.02 per 1M tokens)
        # 3. Include upsert statistics
        # 4. Log the summary

        summary = {
            "timestamp": datetime.now().isoformat(),
            "documents_processed": len(documents),
            "chunks_created": len(chunks),
            "embeddings_generated": len(embeddings),
            "estimated_cost_usd": 0.0,
            "vector_store_stats": upsert_stats,
        }

        # TODO: Calculate estimated_cost_usd based on token usage

        logger.info(f"Pipeline run summary: {json.dumps(summary, indent=2)}")
        return summary

    # =========================================================================
    # DAG Flow
    # =========================================================================

    # Step 1: Discover documents to process
    docs = discover_documents()

    # Step 2: Parse each document (using dynamic task mapping)
    # TODO: Use .expand() to process documents in parallel
    parsed = parse_document.expand(doc_metadata=docs)

    # Step 3: Chunk all documents
    chunks = chunk_documents(parsed)

    # Step 4: Generate embeddings
    embeddings = generate_embeddings(chunks)

    # Step 5: Upsert to vector store
    upsert_stats = upsert_to_vector_store(embeddings)

    # Step 6: Update processing state
    update_state = update_processing_state(docs, upsert_stats)

    # Step 7: Summarize the run
    summary = summarize_run(docs, chunks, embeddings, upsert_stats)

    # Set dependencies
    update_state >> summary


# Instantiate the DAG
rag_ingestion_pipeline()
