# Exercise 15.1: RAG Ingestion Pipeline

Build a complete RAG (Retrieval-Augmented Generation) pipeline that ingests documents, generates embeddings, and updates a vector store with incremental processing.

## Learning Goals

- Design an end-to-end document ingestion pipeline
- Implement incremental processing (only new/changed documents)
- Generate embeddings with proper batching and rate limiting
- Integrate with a vector store (Chroma for local development)
- Apply idempotent upsert patterns

## Scenario

You're building a knowledge base for a company's internal documentation. The pipeline should:

1. Scan a source directory for new/modified documents
2. Parse and chunk documents into embedding-ready segments
3. Generate embeddings using OpenAI's API (or mock for testing)
4. Store embeddings in a vector store with metadata
5. Track processed documents to avoid reprocessing

## Requirements

### Task 1: Document Discovery

Create a task that identifies documents needing processing:

- Scan a source directory for PDF and Markdown files
- Track previously processed documents using Airflow Variables
- Return only new or modified documents (based on modification time or checksum)

### Task 2: Document Processing

Create a dynamic task mapping that processes each document:

- Parse PDF files using `pypdf` or similar
- Parse Markdown files directly
- Split documents into chunks (500-1000 tokens, 100 token overlap)
- Preserve document metadata (filename, path, type)

### Task 3: Embedding Generation

Create a task that generates embeddings with batching:

- Batch chunks to respect API rate limits (100 chunks per batch)
- Mock the embedding API for local testing
- Add rate limiting (0.1s delay between batches)
- Track token usage for cost monitoring

### Task 4: Vector Store Upsert

Create an idempotent upsert task:

- Use content hash as document ID for deduplication
- Include metadata: source file, chunk index, timestamp
- Return statistics: inserted, updated, skipped counts

### Task 5: Pipeline Summary

Create a final task that summarizes the run:

- Total documents processed
- Total chunks generated
- Embedding costs (estimated)
- Any errors or warnings

## Success Criteria

- [ ] Pipeline handles empty source directory gracefully
- [ ] Incremental processing works (re-running skips already processed docs)
- [ ] Embedding generation respects rate limits
- [ ] Vector store upserts are idempotent (same doc = same ID)
- [ ] Pipeline logs include cost/token tracking
- [ ] All tasks have proper error handling and retries

## Hints

<details>
<summary>Hint 1: Document Tracking</summary>

Use Airflow Variables to persist processing state:

```python
from airflow import Variable

processed_docs = Variable.get("rag_processed_docs", default_var={}, deserialize_json=True)
# After processing:
Variable.set("rag_processed_docs", processed_docs, serialize_json=True)
```

</details>

<details>
<summary>Hint 2: Content Hashing</summary>

Create deterministic IDs for chunks:

```python
import hashlib


def chunk_id(source_path: str, chunk_index: int, content: str) -> str:
    """Generate stable ID for chunk."""
    data = f"{source_path}:{chunk_index}:{content}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]
```

</details>

<details>
<summary>Hint 3: Dynamic Task Mapping</summary>

Use `expand()` for parallel document processing:

```python
documents = discover_documents()
chunks = process_document.expand(doc=documents)
embeddings = generate_embeddings(chunks)
```

</details>

<details>
<summary>Hint 4: Mock Embedding API</summary>

For local testing without API costs:

```python
def mock_embedding(text: str) -> list[float]:
    """Generate deterministic mock embedding."""
    import hashlib

    hash_bytes = hashlib.sha256(text.encode()).digest()
    return [b / 255.0 for b in hash_bytes[:384]]  # 384-dim like OpenAI small
```

</details>

## Files

- **Starter**: `exercise_15_1_rag_pipeline_starter.py`
- **Solution**: `../solutions/solution_15_1_rag_pipeline.py`

## Estimated Time

60-90 minutes

## Next Steps

After completing this exercise:

1. Test with real PDF documents in a `test_docs/` directory
2. Try with actual OpenAI API (set `OPENAI_API_KEY`)
3. Explore Pinecone or Weaviate for production vector stores
