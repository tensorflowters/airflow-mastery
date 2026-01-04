# Exercise 5.4: Asset-Driven Embedding Regeneration

Build an asset-driven pipeline that automatically regenerates embeddings when source documents change, demonstrating data-aware scheduling for AI/ML workflows.

## Learning Goals

- Use Assets to trigger embedding regeneration automatically
- Model document-to-embedding dependencies with Assets
- Implement incremental embedding updates based on asset changes
- Coordinate producer-consumer patterns for vector store updates

## Scenario

You're building a document embedding system where:

1. **Source documents** are uploaded to a storage location (Asset: `documents`)
2. **Embedding pipeline** triggers when documents change (consumes `documents`)
3. **Vector store** is updated with new embeddings (Asset: `embeddings`)
4. **Search index** rebuilds when embeddings change (consumes `embeddings`)

This demonstrates how Assets provide data-aware scheduling for ML pipelines.

## Requirements

### Task 1: Document Asset Producer

Create a DAG that produces the document asset:

- Define an Asset for the document source location
- Emit the asset when new documents are added
- Include metadata about the document batch

### Task 2: Embedding Consumer-Producer

Create a DAG that:

- Triggers on the `documents` asset (consumer)
- Generates embeddings for changed documents
- Produces the `embeddings` asset when complete
- Tracks which documents were processed

### Task 3: Vector Store Consumer

Create a DAG that:

- Triggers on the `embeddings` asset
- Updates the vector store with new embeddings
- Handles incremental updates efficiently
- Logs update statistics

### Task 4: Asset Metadata

Demonstrate asset metadata usage:

- Pass document counts through asset metadata
- Include processing timestamps
- Enable downstream tasks to access upstream metadata

## Success Criteria

- [ ] Document upload triggers embedding generation
- [ ] Embedding completion triggers vector store update
- [ ] Asset dependencies are correctly modeled
- [ ] Metadata flows between producer and consumer
- [ ] Incremental updates work correctly

## Hints

<details>
<summary>Hint 1: Defining Assets</summary>

```python
from airflow.sdk import Asset

# Define assets for the pipeline
documents_asset = Asset(
    uri="s3://bucket/documents/",
    extra={"format": "pdf", "source": "upload_system"},
)

embeddings_asset = Asset(
    uri="s3://bucket/embeddings/",
    extra={"model": "text-embedding-3-small", "dimension": 384},
)
```

</details>

<details>
<summary>Hint 2: Producing Assets</summary>

```python
from airflow.sdk import Asset, dag, task

documents_asset = Asset("s3://bucket/documents/")


@dag(schedule=None)
def document_producer():
    @task(outlets=[documents_asset])
    def upload_documents() -> dict:
        """Upload documents and emit asset."""
        # Process documents
        docs_processed = 10

        # Return value becomes asset metadata
        return {
            "documents_added": docs_processed,
            "batch_id": "batch-001",
            "timestamp": datetime.now().isoformat(),
        }

    upload_documents()
```

</details>

<details>
<summary>Hint 3: Consuming Assets</summary>

```python
from airflow.sdk import Asset, dag, task

documents_asset = Asset("s3://bucket/documents/")
embeddings_asset = Asset("s3://bucket/embeddings/")


# This DAG triggers when documents_asset is updated
@dag(schedule=documents_asset)
def embedding_generator():
    @task(outlets=[embeddings_asset])
    def generate_embeddings(**context) -> dict:
        """Generate embeddings for new documents."""
        # Access triggering asset info
        triggering_asset_events = context.get("triggering_asset_events", {})

        for asset_uri, events in triggering_asset_events.items():
            for event in events:
                # Access metadata from producer
                batch_id = event.extra.get("batch_id")
                doc_count = event.extra.get("documents_added", 0)

                logger.info(f"Processing batch {batch_id} with {doc_count} docs")

        return {"embeddings_generated": 10}

    generate_embeddings()
```

</details>

<details>
<summary>Hint 4: Asset Chain</summary>

```python
from airflow.sdk import Asset, dag, task

embeddings_asset = Asset("s3://bucket/embeddings/")


# Final consumer in the chain
@dag(schedule=embeddings_asset)
def vector_store_updater():
    @task
    def update_vector_store(**context) -> dict:
        """Update vector store with new embeddings."""
        events = context.get("triggering_asset_events", {})

        total_embeddings = 0
        for asset_uri, asset_events in events.items():
            for event in asset_events:
                total_embeddings += event.extra.get("embeddings_generated", 0)

        logger.info(f"Updating vector store with {total_embeddings} embeddings")

        return {
            "vectors_upserted": total_embeddings,
            "update_timestamp": datetime.now().isoformat(),
        }

    update_vector_store()
```

</details>

## Files

- **Starter**: `exercise_5_4_embedding_assets_starter.py`
- **Solution**: `../solutions/solution_5_4_embedding_assets.py`

## Estimated Time

45-60 minutes

## Next Steps

After completing this exercise:

1. Add error handling for failed embedding generation
2. Implement partial updates (only changed documents)
3. Add monitoring for asset event delays
4. See Module 15 for complete RAG pipeline patterns
