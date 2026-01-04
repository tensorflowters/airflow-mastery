# Exercise 6.4: Parallel Embedding Generation

Build a dynamic task mapping pipeline that processes document chunks in parallel for embedding generation, demonstrating `expand()` patterns for AI/ML workloads.

## Learning Goals

- Use dynamic task mapping for parallel document processing
- Apply `expand()` to distribute embedding generation across workers
- Implement batching with `map_batches()` for API efficiency
- Handle variable-length inputs with mapped tasks

## Scenario

You're processing a large document corpus where:

1. Documents are split into variable-length chunks
2. Each chunk needs an embedding generated (parallelizable)
3. Embeddings should be batched to respect API rate limits
4. Results must be aggregated for vector store upsert

## Requirements

### Task 1: Dynamic Chunk Processing

Create a pipeline that:

- Discovers documents and splits into chunks
- Uses `.expand()` to create one task per chunk
- Processes chunks in parallel within Airflow's constraints

### Task 2: Batched API Calls

Implement batching with:

- Group chunks into batches of configurable size
- Use `.map_batches()` for efficient processing
- Handle partial batches at the end

### Task 3: Result Aggregation

Aggregate mapped task outputs:

- Collect embeddings from all mapped tasks
- Handle failures in individual tasks gracefully
- Produce summary statistics

### Task 4: Cross-Product Mapping

Demonstrate advanced patterns:

- Process chunks with multiple embedding models (cross-product)
- Compare model outputs for quality assessment

## Success Criteria

- [ ] Chunks process in parallel across mapped tasks
- [ ] Batching respects API limits
- [ ] Failed chunks don't break the entire pipeline
- [ ] Results are properly aggregated

## Hints

<details>
<summary>Hint 1: Basic expand() Pattern</summary>

```python
@task
def get_chunks() -> list[dict]:
    """Return list of chunks to process."""
    return [
        {"id": "chunk-1", "text": "First chunk..."},
        {"id": "chunk-2", "text": "Second chunk..."},
        {"id": "chunk-3", "text": "Third chunk..."},
    ]


@task
def generate_embedding(chunk: dict) -> dict:
    """Process single chunk - called once per chunk."""
    embedding = mock_embedding(chunk["text"])
    return {"id": chunk["id"], "embedding": embedding}


# Creates 3 parallel tasks - one per chunk
chunks = get_chunks()
embeddings = generate_embedding.expand(chunk=chunks)
```

</details>

<details>
<summary>Hint 2: Batched Processing</summary>

```python
@task
def process_batch(batch: list[dict]) -> list[dict]:
    """Process a batch of chunks together."""
    results = []
    for chunk in batch:
        embedding = mock_embedding(chunk["text"])
        results.append({"id": chunk["id"], "embedding": embedding})
    return results


# Process in batches of 10
chunks = get_chunks()
batched_results = process_batch.map_batches(chunks, batch_size=10)
```

</details>

<details>
<summary>Hint 3: Aggregating Mapped Results</summary>

```python
@task
def aggregate_embeddings(embeddings: list[dict]) -> dict:
    """
    Aggregate results from mapped tasks.

    The `embeddings` parameter receives a list of all outputs
    from the mapped task.
    """
    successful = [e for e in embeddings if e.get("embedding")]
    failed = [e for e in embeddings if not e.get("embedding")]

    return {
        "total": len(embeddings),
        "successful": len(successful),
        "failed": len(failed),
        "embeddings": successful,
    }


# Mapped task outputs are automatically collected into a list
embeddings = generate_embedding.expand(chunk=chunks)
summary = aggregate_embeddings(embeddings)
```

</details>

<details>
<summary>Hint 4: Cross-Product with expand_kwargs</summary>

```python
@task
def embed_with_model(chunk: dict, model: str) -> dict:
    """Generate embedding with specific model."""
    embedding = mock_embedding(chunk["text"], model=model)
    return {"id": chunk["id"], "model": model, "embedding": embedding}


# Cross-product: each chunk x each model
chunks = [{"id": "1", "text": "..."}, {"id": "2", "text": "..."}]
models = ["model-a", "model-b"]

# Creates len(chunks) * len(models) tasks
results = embed_with_model.expand(
    chunk=chunks,
    model=models,
)
```

</details>

## Files

- **Starter**: `exercise_6_4_parallel_embeddings_starter.py`
- **Solution**: `../solutions/solution_6_4_parallel_embeddings.py`

## Estimated Time

45-60 minutes

## Next Steps

After completing this exercise:

1. Experiment with different batch sizes
2. Add progress tracking for long-running pipelines
3. Implement partial failure recovery
4. See Module 15 for complete RAG pipeline patterns
