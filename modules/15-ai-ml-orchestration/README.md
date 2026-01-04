# Module 15: AI/ML Orchestration with Airflow

Orchestrate modern AI and machine learning workflows using Apache Airflow 3's powerful scheduling, data-aware capabilities, and production-grade patterns.

## Learning Objectives

By completing this module, you will be able to:

- Design and implement RAG (Retrieval-Augmented Generation) pipelines with Airflow
- Orchestrate multi-step LLM workflows with proper error handling and cost tracking
- Build data preparation pipelines with quality gates and human-in-the-loop patterns
- Apply rate limiting, caching, and retry strategies for LLM API calls
- Monitor and optimize AI/ML pipeline costs and performance

## Prerequisites

- Completion of Modules 01-14 (especially Dynamic Tasks, Assets, and Production Patterns)
- Basic understanding of embeddings and vector databases
- Familiarity with LLM APIs (OpenAI, Anthropic, or similar)
- Python libraries: `openai`, `langchain`, or similar

## Why Airflow for AI/ML?

While specialized ML orchestration tools exist, Airflow excels for AI/ML workflows because:

| Feature                    | Airflow Strength                               | Use Case                       |
| -------------------------- | ---------------------------------------------- | ------------------------------ |
| **Data-Aware Scheduling**  | Assets trigger pipelines when data changes     | Retrain models on new data     |
| **Dynamic Task Mapping**   | Process variable-size batches efficiently      | Parallel embedding generation  |
| **Production Maturity**    | Battle-tested scheduling, monitoring, alerting | Mission-critical ML pipelines  |
| **Hybrid Workflows**       | Mix ML tasks with data engineering             | End-to-end data + ML pipelines |
| **Kubernetes Integration** | Per-task resource allocation                   | GPU pods for inference         |

### Comparison with Alternatives

```
Airflow         → Best for: Hybrid data + ML, production scheduling, existing data infrastructure
Kubeflow        → Best for: Pure ML training, Kubernetes-native teams
Prefect/Dagster → Best for: Python-first teams, simpler deployments
MLflow          → Best for: Experiment tracking, model registry (complementary to Airflow)
```

## RAG Pipeline Patterns

RAG (Retrieval-Augmented Generation) pipelines ingest documents, generate embeddings, and update vector stores for LLM retrieval.

### Architecture Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Document  │────▶│   Chunking  │────▶│  Embedding  │────▶│   Vector    │
│   Source    │     │   Strategy  │     │  Generation │     │   Store     │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
     S3/GCS              Semantic           OpenAI           Pinecone
     Local               Recursive          Cohere           Weaviate
     API                 Fixed-size         Local            Chroma
```

### Document Ingestion Strategies

```python
from airflow.sdk import task


@task
def list_new_documents(source_path: str, last_processed: str) -> list[dict]:
    """Identify documents that need processing (incremental updates)."""
    # Compare modification times, checksums, or version markers
    return [{"path": "doc1.pdf", "checksum": "abc123", "modified": "2024-01-15"}]


@task
def load_document(doc_metadata: dict) -> dict:
    """Load and parse a single document."""
    # Use appropriate loader: PDF, HTML, Markdown, etc.
    return {"content": "...", "metadata": doc_metadata}
```

### Chunking Approaches

| Strategy           | Description                    | Best For                               |
| ------------------ | ------------------------------ | -------------------------------------- |
| **Fixed-size**     | Split by character/token count | Simple documents, consistent structure |
| **Semantic**       | Split by meaning boundaries    | Technical docs, articles               |
| **Recursive**      | Hierarchical splitting         | Code, nested structures                |
| **Document-aware** | Preserve sections, headers     | Manuals, legal documents               |

```python
@task
def chunk_document(document: dict, strategy: str = "semantic") -> list[dict]:
    """Split document into chunks for embedding."""
    # Typical chunk sizes: 500-2000 tokens with 10-20% overlap
    chunks = semantic_chunker(document["content"], max_tokens=1000, overlap=100)
    return [{"text": chunk, "metadata": document["metadata"]} for chunk in chunks]
```

### Embedding Generation with Batching

```python
@task
def generate_embeddings(chunks: list[dict], batch_size: int = 100) -> list[dict]:
    """Generate embeddings with batching and rate limiting."""
    embeddings = []
    for batch in batched(chunks, batch_size):
        # Respect rate limits, implement retry logic
        batch_embeddings = embedding_model.embed_batch([c["text"] for c in batch])
        embeddings.extend(batch_embeddings)
        time.sleep(0.1)  # Rate limiting
    return embeddings
```

### Vector Store Integration

Popular vector stores and their Airflow integration patterns:

- **Pinecone**: Managed service, excellent for production scale
- **Weaviate**: Self-hosted or cloud, GraphQL interface
- **Chroma**: Lightweight, perfect for development and small scale
- **Qdrant**: High-performance, good filtering capabilities

## LLM API Orchestration

Orchestrating LLM calls requires careful attention to rate limiting, cost tracking, and error handling.

### Rate Limiting Strategies

```python
from airflow.sdk import task


@task(retries=3, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True)
def call_llm_with_retry(prompt: str, model: str = "gpt-4") -> str:
    """LLM call with Airflow-native retry configuration."""
    response = openai.chat.completions.create(model=model, messages=[{"role": "user", "content": prompt}])
    return response.choices[0].message.content
```

### Cost Tracking Callbacks

```python
def track_llm_cost(context):
    """Callback to track LLM API costs per task."""
    task_instance = context["task_instance"]

    # Extract token usage from XCom or task output
    usage = task_instance.xcom_pull(key="token_usage")
    if usage:
        cost = calculate_cost(usage["prompt_tokens"], usage["completion_tokens"])

        # Log to monitoring system
        log_metric("llm_cost", cost, tags={"dag": context["dag"].dag_id})

        # Alert if budget exceeded
        if get_daily_spend() > DAILY_BUDGET:
            send_alert("LLM budget exceeded!")


@dag(on_success_callback=track_llm_cost)
def llm_pipeline(): ...
```

### Multi-Model Routing

```python
@task
def route_to_model(query: str, complexity_score: float) -> str:
    """Route queries to appropriate models based on complexity."""
    if complexity_score < 0.3:
        return "gpt-3.5-turbo"  # Fast, cheap for simple queries
    elif complexity_score < 0.7:
        return "gpt-4"  # Balanced
    else:
        return "gpt-4-turbo"  # Complex reasoning
```

### Prompt Versioning

```python
from airflow import Variable


@task
def get_prompt_template(prompt_name: str, version: str = "latest") -> str:
    """Retrieve versioned prompt templates."""
    prompts = Variable.get("prompt_templates", deserialize_json=True)

    if version == "latest":
        return prompts[prompt_name]["versions"][-1]["template"]

    return next(v["template"] for v in prompts[prompt_name]["versions"] if v["version"] == version)
```

## Data Preparation Workflows

AI/ML models require high-quality, well-prepared data. Airflow excels at building robust data preparation pipelines.

### Quality Gates Pattern

```python
@task.branch
def quality_gate(data_stats: dict) -> str:
    """Route based on data quality checks."""
    if data_stats["null_rate"] > 0.1:
        return "handle_missing_data"
    if data_stats["outlier_rate"] > 0.05:
        return "handle_outliers"
    return "proceed_to_training"
```

### Human-in-the-Loop

```python
from airflow.sdk import task
from airflow.sensors.external_task import ExternalTaskSensor


@task
def flag_for_review(low_confidence_items: list[dict]) -> None:
    """Send items to human review queue."""
    for item in low_confidence_items:
        review_queue.add(
            item=item, reason="Low confidence classification", deadline=datetime.now() + timedelta(hours=24)
        )


# Sensor waits for human review completion
wait_for_review = ExternalTaskSensor(
    task_id="wait_for_review",
    external_dag_id="human_review_dag",
    external_task_id="review_complete",
    timeout=86400,  # 24 hours
)
```

### Metadata Extraction Pipeline

```python
@task
def extract_metadata(document: dict) -> dict:
    """Extract structured metadata from documents."""
    return {
        "title": extract_title(document),
        "author": extract_author(document),
        "date": extract_date(document),
        "entities": extract_entities(document),
        "topics": classify_topics(document),
        "language": detect_language(document),
        "quality_score": calculate_quality_score(document),
    }
```

## Best Practices

### Idempotency for Embeddings

```python
@task
def upsert_embeddings(embeddings: list[dict], namespace: str) -> dict:
    """Idempotent embedding upsert using content hashes."""
    results = {"inserted": 0, "updated": 0, "skipped": 0}

    for emb in embeddings:
        content_hash = hash_content(emb["text"])
        existing = vector_store.get(content_hash)

        if existing and existing["embedding"] == emb["embedding"]:
            results["skipped"] += 1
        else:
            vector_store.upsert(id=content_hash, **emb)
            results["updated" if existing else "inserted"] += 1

    return results
```

### Caching Expensive LLM Calls

```python
import hashlib


def get_cache_key(prompt: str, model: str) -> str:
    """Generate deterministic cache key for LLM calls."""
    return hashlib.sha256(f"{model}:{prompt}".encode()).hexdigest()


@task
def cached_llm_call(prompt: str, model: str) -> str:
    """LLM call with persistent caching."""
    cache_key = get_cache_key(prompt, model)

    # Check cache first
    cached = cache.get(cache_key)
    if cached:
        return cached

    # Make API call and cache result
    result = call_llm(prompt, model)
    cache.set(cache_key, result, ttl=86400)  # 24-hour TTL

    return result
```

### Observability and Cost Monitoring

```python
@dag(
    default_args={
        "on_failure_callback": alert_on_failure,
        "on_success_callback": track_metrics,
    },
    tags=["ai-ml", "rag", "production"],
)
def rag_pipeline():
    """RAG pipeline with comprehensive observability."""
    # Track document processing metrics
    docs = process_documents()

    # Track embedding costs and latency
    embeddings = generate_embeddings(docs)

    # Track vector store operations
    upsert_results = upsert_to_vector_store(embeddings)

    # Final cost summary
    summarize_run_costs(docs, embeddings, upsert_results)
```

## Exercises

Complete these exercises to master AI/ML orchestration with Airflow:

### Exercise 15.1: RAG Ingestion Pipeline

Build a complete RAG pipeline that ingests documents, generates embeddings, and updates a vector store with incremental processing.

**Files**:

- `exercises/exercise_15_1_rag_pipeline.md`
- `exercises/exercise_15_1_rag_pipeline_starter.py`
- `solutions/solution_15_1_rag_pipeline.py`

### Exercise 15.2: LLM Chain Orchestration

Create a multi-step LLM workflow with rate limiting, cost tracking, and fallback model routing.

**Files**:

- `exercises/exercise_15_2_llm_chain.md`
- `exercises/exercise_15_2_llm_chain_starter.py`
- `solutions/solution_15_2_llm_chain.py`

### Exercise 15.3: Data Preparation Pipeline

Build a data preparation pipeline with quality gates, human-in-the-loop review, and metadata extraction.

**Files**:

- `exercises/exercise_15_3_data_prep.md`
- `exercises/exercise_15_3_data_prep_starter.py`
- `solutions/solution_15_3_data_prep.py`

## Checkpoint

Before moving on, verify you can:

- [ ] Explain why Airflow is suitable for AI/ML orchestration
- [ ] Design a RAG pipeline with incremental document processing
- [ ] Implement rate limiting and retry strategies for LLM APIs
- [ ] Build cost tracking and budget alerting for LLM workflows
- [ ] Create data quality gates and human-in-the-loop patterns
- [ ] Apply caching strategies for expensive LLM operations
- [ ] Monitor AI/ML pipeline costs and performance

## Further Reading

### Official Documentation

- [Apache Airflow TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- [Assets (Data-Aware Scheduling)](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/assets.html)

### AI/ML Integration Resources

- [LangChain Documentation](https://python.langchain.com/docs/)
- [OpenAI API Best Practices](https://platform.openai.com/docs/guides/production-best-practices)
- [Pinecone Documentation](https://docs.pinecone.io/)
- [Weaviate Documentation](https://weaviate.io/developers/weaviate)

### Community Resources

- [Airflow for ML Pipelines](https://www.astronomer.io/guides/airflow-ml-pipelines/)
- [Building RAG Applications](https://www.pinecone.io/learn/retrieval-augmented-generation/)
