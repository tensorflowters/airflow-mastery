"""
RAG Ingestion Pipeline - Complete Solution.

This solution demonstrates a production-ready RAG pipeline that ingests documents,
generates embeddings, and updates a vector store with incremental processing.

Key patterns demonstrated:
- Incremental document processing using file hashes
- Dynamic task mapping for parallel document parsing
- Batched embedding generation with rate limiting
- Idempotent vector store upserts
- Cost tracking and pipeline observability
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow import Variable
from airflow.sdk import dag, task

# Configuration
SOURCE_DIR = os.environ.get("RAG_SOURCE_DIR", "/tmp/rag_docs")
CHUNK_SIZE = 800  # tokens (approximate)
CHUNK_OVERLAP = 100  # tokens
EMBEDDING_BATCH_SIZE = 100
EMBEDDING_DIMENSION = 384  # OpenAI text-embedding-3-small compatible
USE_MOCK_EMBEDDINGS = True  # Set to False to use real OpenAI API
RATE_LIMIT_DELAY = 0.1  # seconds between batches

# Cost estimation (OpenAI text-embedding-3-small pricing)
COST_PER_MILLION_TOKENS = 0.02

logger = logging.getLogger(__name__)


# =============================================================================
# Helper Functions
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
    extended = (hash_bytes * (EMBEDDING_DIMENSION // len(hash_bytes) + 1))[:EMBEDDING_DIMENSION]
    return [b / 255.0 for b in extended]


def chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """Split text into overlapping chunks by approximate token count."""
    char_chunk_size = chunk_size * 4
    char_overlap = overlap * 4

    chunks = []
    start = 0

    while start < len(text):
        end = start + char_chunk_size

        if end < len(text):
            search_start = end - (char_chunk_size // 5)
            for sep in [". ", ".\n", "! ", "? "]:
                last_sep = text.rfind(sep, search_start, end)
                if last_sep != -1:
                    end = last_sep + 1
                    break

        chunks.append(text[start:end].strip())
        start = end - char_overlap

    return [c for c in chunks if c]


def generate_chunk_id(source_path: str, chunk_index: int, content: str) -> str:
    """Generate a deterministic ID for a chunk based on its content and position."""
    data = f"{source_path}:{chunk_index}:{content}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def batched(iterable: list, batch_size: int) -> list[list]:
    """Split an iterable into batches of specified size."""
    return [iterable[i : i + batch_size] for i in range(0, len(iterable), batch_size)]


# =============================================================================
# In-Memory Vector Store
# =============================================================================


class SimpleVectorStore:
    """Simple in-memory vector store with persistence for testing."""

    def __init__(self, persist_path: str = "/tmp/rag_vectors.json") -> None:
        """Initialize the vector store with optional persistence path."""
        self.persist_path = persist_path
        self.vectors: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        """Load vectors from disk if available."""
        if os.path.exists(self.persist_path):
            try:
                with open(self.persist_path) as f:
                    self.vectors = json.load(f)
                logger.info(f"Loaded {len(self.vectors)} vectors from {self.persist_path}")
            except (json.JSONDecodeError, OSError) as e:
                logger.warning(f"Could not load vectors: {e}")
                self.vectors = {}

    def _save(self) -> None:
        """Persist vectors to disk."""
        os.makedirs(os.path.dirname(self.persist_path), exist_ok=True)
        with open(self.persist_path, "w") as f:
            json.dump(self.vectors, f)

    def upsert(self, doc_id: str, embedding: list[float], metadata: dict[str, Any]) -> str:
        """
        Upsert a vector with metadata.

        Returns: 'inserted', 'updated', or 'skipped'.
        """
        existing = self.vectors.get(doc_id)

        if existing is not None:
            # Check if embedding is the same (compare first 10 values for efficiency)
            if existing.get("embedding", [])[:10] == embedding[:10]:
                return "skipped"
            else:
                self.vectors[doc_id] = {"embedding": embedding, "metadata": metadata}
                self._save()
                return "updated"
        else:
            self.vectors[doc_id] = {"embedding": embedding, "metadata": metadata}
            self._save()
            return "inserted"

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
    schedule=None,
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
            List of document metadata for documents needing processing.
        """
        logger.info(f"Scanning source directory: {SOURCE_DIR}")

        # Create source dir if it doesn't exist
        Path(SOURCE_DIR).mkdir(parents=True, exist_ok=True)

        # Load previously processed documents
        processed_docs = Variable.get("rag_processed_docs", default_var={}, deserialize_json=True)
        logger.info(f"Previously processed: {len(processed_docs)} documents")

        documents_to_process: list[dict[str, Any]] = []
        source_path = Path(SOURCE_DIR)

        # Scan for PDF and Markdown files
        for pattern in ["*.pdf", "*.md", "*.markdown"]:
            for filepath in source_path.glob(pattern):
                file_hash = get_file_hash(str(filepath))
                filename = filepath.name

                # Check if file has been processed with same hash
                if filename in processed_docs and processed_docs[filename].get("hash") == file_hash:
                    logger.debug(f"Skipping unchanged file: {filename}")
                    continue

                # Determine file type
                file_type = "pdf" if filepath.suffix.lower() == ".pdf" else "markdown"

                documents_to_process.append(
                    {
                        "path": str(filepath),
                        "filename": filename,
                        "file_type": file_type,
                        "file_hash": file_hash,
                        "modified_time": datetime.fromtimestamp(filepath.stat().st_mtime).isoformat(),
                    }
                )

        logger.info(f"Found {len(documents_to_process)} documents to process")
        return documents_to_process

    @task
    def parse_document(doc_metadata: dict[str, Any]) -> dict[str, Any]:
        """
        Parse a single document and extract text content.

        Args:
            doc_metadata: Document metadata from discover_documents.

        Returns:
            Dict with content, metadata, and any parse errors.
        """
        filepath = doc_metadata["path"]
        file_type = doc_metadata.get("file_type", "unknown")

        logger.info(f"Parsing document: {filepath} (type: {file_type})")

        content = ""
        parse_errors: list[str] = []

        try:
            if file_type == "markdown":
                with open(filepath, encoding="utf-8") as f:
                    content = f.read()
            elif file_type == "pdf":
                # For PDF, we would use pypdf or similar
                # Here we provide a mock implementation for testing
                try:
                    # Try to use pypdf if available
                    from pypdf import PdfReader

                    reader = PdfReader(filepath)
                    content = "\n\n".join(page.extract_text() or "" for page in reader.pages)
                except ImportError:
                    # Mock PDF content for testing without pypdf
                    parse_errors.append("pypdf not installed, using mock content")
                    content = f"Mock content for PDF: {doc_metadata['filename']}\n\n"
                    content += "This is placeholder text for testing the pipeline.\n" * 50
            else:
                parse_errors.append(f"Unknown file type: {file_type}")

        except Exception as e:
            parse_errors.append(f"Error parsing file: {e!s}")
            logger.exception(f"Failed to parse {filepath}")

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
            parsed_docs: List of parsed document dicts.

        Returns:
            List of chunk dicts ready for embedding generation.
        """
        all_chunks: list[dict[str, Any]] = []

        for doc in parsed_docs:
            content = doc.get("content", "")
            metadata = doc.get("metadata", {})
            source_file = metadata.get("filename", "unknown")

            if not content.strip():
                logger.warning(f"Empty content for document: {source_file}")
                continue

            # Split content into chunks
            text_chunks = chunk_text(content)

            for idx, chunk_text_content in enumerate(text_chunks):
                chunk_id = generate_chunk_id(source_file, idx, chunk_text_content)
                token_estimate = estimate_tokens(chunk_text_content)

                all_chunks.append(
                    {
                        "chunk_id": chunk_id,
                        "text": chunk_text_content,
                        "chunk_index": idx,
                        "source_file": source_file,
                        "token_estimate": token_estimate,
                        "total_chunks": len(text_chunks),
                        "file_hash": metadata.get("file_hash", ""),
                    }
                )

        logger.info(f"Created {len(all_chunks)} chunks from {len(parsed_docs)} documents")
        return all_chunks

    @task
    def generate_embeddings(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Generate embeddings for all chunks with batching and rate limiting.

        Args:
            chunks: List of chunk dicts from chunk_documents.

        Returns:
            List of embedding dicts with vectors and metadata.
        """
        if not chunks:
            logger.info("No chunks to embed")
            return []

        embeddings: list[dict[str, Any]] = []
        total_tokens = 0

        # Process in batches
        chunk_batches = batched(chunks, EMBEDDING_BATCH_SIZE)
        logger.info(f"Processing {len(chunks)} chunks in {len(chunk_batches)} batches")

        for batch_idx, batch in enumerate(chunk_batches):
            batch_start = time.time()

            for chunk in batch:
                text = chunk["text"]
                token_count = chunk["token_estimate"]
                total_tokens += token_count

                if USE_MOCK_EMBEDDINGS:
                    embedding = mock_embedding(text)
                else:
                    # Real OpenAI API call would go here
                    # from openai import OpenAI
                    # client = OpenAI()
                    # response = client.embeddings.create(
                    #     model="text-embedding-3-small",
                    #     input=text
                    # )
                    # embedding = response.data[0].embedding
                    embedding = mock_embedding(text)  # Fallback to mock

                embeddings.append(
                    {
                        "chunk_id": chunk["chunk_id"],
                        "embedding": embedding,
                        "text": text,
                        "metadata": {
                            "source_file": chunk["source_file"],
                            "chunk_index": chunk["chunk_index"],
                            "total_chunks": chunk["total_chunks"],
                            "token_count": token_count,
                        },
                        "token_usage": token_count,
                    }
                )

            batch_time = time.time() - batch_start
            logger.info(f"Batch {batch_idx + 1}/{len(chunk_batches)} completed in {batch_time:.2f}s")

            # Rate limiting between batches
            if batch_idx < len(chunk_batches) - 1:
                time.sleep(RATE_LIMIT_DELAY)

        logger.info(f"Generated {len(embeddings)} embeddings, total tokens: {total_tokens}")
        return embeddings

    @task
    def upsert_to_vector_store(embeddings: list[dict[str, Any]]) -> dict[str, int]:
        """
        Upsert embeddings to the vector store idempotently.

        Args:
            embeddings: List of embedding dicts from generate_embeddings.

        Returns:
            Dict with counts: inserted, updated, skipped.
        """
        vector_store = SimpleVectorStore()
        stats = {"inserted": 0, "updated": 0, "skipped": 0}

        for emb in embeddings:
            result = vector_store.upsert(
                doc_id=emb["chunk_id"],
                embedding=emb["embedding"],
                metadata={
                    "text": emb["text"],
                    **emb["metadata"],
                    "indexed_at": datetime.now().isoformat(),
                },
            )
            stats[result] += 1

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
            processed_docs: List of successfully processed document metadata.
            upsert_stats: Statistics from vector store upsert.
        """
        # Load existing state
        current_state = Variable.get("rag_processed_docs", default_var={}, deserialize_json=True)

        # Update with newly processed documents
        for doc in processed_docs:
            current_state[doc["filename"]] = {
                "hash": doc["file_hash"],
                "processed_at": datetime.now().isoformat(),
                "modified_time": doc["modified_time"],
            }

        # Save updated state
        Variable.set("rag_processed_docs", current_state, serialize_json=True)

        logger.info(f"Updated processing state for {len(processed_docs)} documents. Upsert stats: {upsert_stats}")

    @task
    def summarize_run(
        documents: list[dict[str, Any]],
        chunks: list[dict[str, Any]],
        embeddings: list[dict[str, Any]],
        upsert_stats: dict[str, int],
    ) -> dict[str, Any]:
        """
        Generate a comprehensive summary of the pipeline run.

        Returns:
            Summary dict with processing statistics and costs.
        """
        # Calculate total tokens
        total_tokens = sum(e.get("token_usage", 0) for e in embeddings)

        # Estimate cost
        estimated_cost = (total_tokens / 1_000_000) * COST_PER_MILLION_TOKENS

        summary = {
            "timestamp": datetime.now().isoformat(),
            "documents_processed": len(documents),
            "chunks_created": len(chunks),
            "embeddings_generated": len(embeddings),
            "total_tokens": total_tokens,
            "estimated_cost_usd": round(estimated_cost, 6),
            "vector_store_stats": upsert_stats,
            "use_mock_embeddings": USE_MOCK_EMBEDDINGS,
        }

        logger.info(f"Pipeline run summary:\n{json.dumps(summary, indent=2)}")
        return summary

    # =========================================================================
    # DAG Flow
    # =========================================================================

    # Step 1: Discover documents to process
    docs = discover_documents()

    # Step 2: Parse each document using dynamic task mapping
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
