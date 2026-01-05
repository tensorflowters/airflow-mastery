# Exercise 16.3: LLM Processing Pipeline

Build the AI-powered data enrichment DAG that extracts, summarizes, and classifies documents using LLM chains with production-grade retry strategies and cost tracking.

## Learning Goals

- Design multi-step LLM processing chains with proper orchestration
- Implement pool-based rate limiting for LLM API calls
- Build quality gates using `@task.branch` for confidence-based routing
- Track LLM costs and token usage at the task level
- Create Asset outlets for downstream pipeline triggering

## Background

### LLM Chain Orchestration Patterns

LLM chains decompose complex reasoning into specialized steps. Each step focuses on a specific task, improving accuracy and enabling targeted error handling:

```
                    ┌─────────────────────────────────────────────────────────┐
                    │              LLM Processing Pipeline                     │
                    ├─────────────────────────────────────────────────────────┤
                    │                                                          │
                    │  ┌──────────┐   ┌──────────┐   ┌──────────┐            │
                    │  │ Extract  │──▶│Summarize │──▶│ Classify │            │
                    │  │ Entities │   │  Content │   │  Topics  │            │
                    │  └──────────┘   └──────────┘   └──────────┘            │
                    │       │              │              │                   │
                    │       ▼              ▼              ▼                   │
                    │  ┌──────────────────────────────────────────┐          │
                    │  │           Quality Gate                    │          │
                    │  │    (Confidence-Based Routing)             │          │
                    │  └──────────────────────────────────────────┘          │
                    │            │                    │                       │
                    │     ┌──────▼──────┐    ┌───────▼───────┐               │
                    │     │  High Conf  │    │   Low Conf    │               │
                    │     │   (>0.8)    │    │    (<0.8)     │               │
                    │     └──────┬──────┘    └───────┬───────┘               │
                    │            │                   │                        │
                    │            ▼                   ▼                        │
                    │     ┌──────────────┐   ┌──────────────┐                │
                    │     │   Publish    │   │   Manual     │                │
                    │     │   [Asset]    │   │   Review     │                │
                    │     └──────────────┘   └──────────────┘                │
                    │                                                          │
                    └─────────────────────────────────────────────────────────┘
```

### Key Patterns Applied

| Pattern                  | Module Reference | Application in This Exercise                   |
| ------------------------ | ---------------- | ---------------------------------------------- |
| TaskFlow with XCom       | Module 02        | Chain LLM outputs through task return values   |
| Pool-based rate limiting | Module 14        | Use `pool="llm_api"` to limit concurrent calls |
| Exponential backoff      | Module 09        | Configure retries for transient API failures   |
| Branch operators         | Module 04        | Route based on confidence scores               |
| Asset dependencies       | Module 05        | Trigger downstream vector store pipeline       |
| Cost tracking callbacks  | Module 15        | Monitor and budget LLM API costs               |

## Prerequisites

- Completed Exercises 16.1 (Setup) and 16.2 (Ingestion)
- `llm_api` pool created with 5 slots (from Exercise 16.1)
- OpenAI API key configured (or mock implementation)
- Asset `documents.raw` being produced by ingestion pipeline

## Requirements

### Task 1: LLM Extraction with Pool and Retry Strategy

Create an extraction task that pulls key entities from documents:

```python
@task(
    pool="llm_api",
    pool_slots=1,
    retries=3,
    retry_delay=timedelta(seconds=10),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=2),
)
def extract_entities(document: dict) -> dict:
    """
    Extract entities using LLM with production-grade configuration.

    Pool ensures we don't exceed API rate limits.
    Exponential backoff handles transient failures gracefully.
    """
    # Your implementation here
    pass
```

Requirements:

- Use `pool="llm_api"` to limit concurrent API calls
- Configure exponential backoff with 3 retries
- Return structured entity data (people, organizations, dates, topics)
- Track token usage in the response for cost calculation

### Task 2: Chain Summarization and Classification

Create a processing chain where summarization feeds into classification:

```python
@task(pool="llm_api", retries=3, retry_exponential_backoff=True)
def summarize_content(document: dict, entities: dict) -> dict:
    """Generate concise summary using extracted entities as context."""
    pass


@task(pool="llm_api", retries=3, retry_exponential_backoff=True)
def classify_document(document: dict, summary: dict) -> dict:
    """Classify document into predefined categories with confidence scores."""
    pass
```

Requirements:

- Chain outputs through XCom (automatic with TaskFlow)
- Include confidence scores in classification output
- Handle partial extraction failures gracefully
- Aggregate token usage across the chain

### Task 3: Quality Gate with Branching

Implement confidence-based routing to handle varying quality outputs:

```python
@task.branch
def quality_gate(classification: dict) -> str:
    """
    Route based on classification confidence.

    Returns task_id to execute next:
    - 'publish_enriched' for high confidence (>= 0.8)
    - 'manual_review_queue' for low confidence (< 0.8)
    """
    confidence = classification.get("confidence", 0)

    if confidence >= 0.8:
        return "publish_enriched"
    else:
        return "manual_review_queue"
```

Requirements:

- Use `@task.branch` decorator for conditional routing
- Define clear confidence thresholds (configurable via Variable)
- Create both high-confidence and low-confidence task paths
- Log routing decisions for monitoring

### Task 4: Asset Outlet for Downstream Triggering

Create the publication task that outputs the enriched asset:

```python
from airflow.sdk import Asset

ENRICHED_DOCS_ASSET = Asset("documents.enriched")


@task(outlets=[ENRICHED_DOCS_ASSET])
def publish_enriched(
    document: dict,
    entities: dict,
    summary: dict,
    classification: dict,
) -> dict:
    """
    Publish enriched document and trigger downstream pipelines.

    The Asset outlet signals completion to the vector store pipeline.
    """
    enriched = {
        "document_id": document["id"],
        "original_content": document["content"],
        "entities": entities,
        "summary": summary["text"],
        "classification": classification,
        "enriched_at": datetime.utcnow().isoformat(),
    }

    # Persist enriched document (to file, database, or object storage)
    # ...

    return enriched
```

Requirements:

- Define `Asset("documents.enriched")` as outlet
- Combine all processing results into enriched structure
- Persist to storage (configure via connection)
- Return document ID for downstream reference

### Task 5: Cost Tracking for LLM API Calls

Implement cost tracking using callbacks:

```python
MODEL_PRICING = {
    "gpt-4-turbo": {"input": 0.01, "output": 0.03},  # per 1K tokens
    "gpt-4": {"input": 0.03, "output": 0.06},
    "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
}


def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate cost in USD for an LLM call."""
    pricing = MODEL_PRICING.get(model, MODEL_PRICING["gpt-3.5-turbo"])
    input_cost = (input_tokens / 1000) * pricing["input"]
    output_cost = (output_tokens / 1000) * pricing["output"]
    return input_cost + output_cost


def track_llm_cost(context):
    """On-success callback to track LLM costs."""
    ti = context["task_instance"]
    usage = ti.xcom_pull(key="token_usage")

    if usage:
        cost = calculate_cost(
            usage.get("model", "gpt-3.5-turbo"),
            usage.get("input_tokens", 0),
            usage.get("output_tokens", 0),
        )

        # Store in XCom for aggregation
        ti.xcom_push(key="llm_cost", value=cost)

        # Update daily spend tracking
        from airflow.models import Variable

        daily_key = f"llm_spend_{datetime.now().strftime('%Y-%m-%d')}"
        current_spend = Variable.get(daily_key, default_var=0.0, deserialize_json=True)
        Variable.set(daily_key, current_spend + cost, serialize_json=True)
```

Requirements:

- Track input/output tokens for each LLM call
- Calculate costs using model-specific pricing
- Store per-task costs in XCom
- Aggregate daily spend in Airflow Variables
- Alert when approaching budget threshold (optional)

## Complete DAG Structure

```python
from datetime import datetime, timedelta

from airflow.sdk import Asset, dag, task

RAW_DOCS_ASSET = Asset("documents.raw")
ENRICHED_DOCS_ASSET = Asset("documents.enriched")


@dag(
    dag_id="llm_processing_pipeline",
    schedule=RAW_DOCS_ASSET,  # Triggered by ingestion pipeline
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "llm", "processing"],
    default_args={
        "owner": "data-team",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=2),
    },
)
def llm_processing_pipeline():
    # Task 1: Get documents to process
    documents = get_pending_documents()

    # Task 2-4: Process each document through LLM chain
    @task
    def process_document(doc: dict) -> dict:
        # Orchestrate the chain for a single document
        entities = extract_entities(doc)
        summary = summarize_content(doc, entities)
        classification = classify_document(doc, summary)

        return {
            "document": doc,
            "entities": entities,
            "summary": summary,
            "classification": classification,
        }

    processed = process_document.expand(doc=documents)

    # Task 5: Quality gate and routing
    route = quality_gate(processed)

    # Parallel paths based on quality gate
    published = publish_enriched(processed)
    review = manual_review_queue(processed)

    # Set trigger rules for branching
    published.trigger_rule = "none_failed_min_one_success"
    review.trigger_rule = "none_failed_min_one_success"

    # Task 6: Aggregate costs
    aggregate_costs([published, review])


llm_processing_pipeline()
```

## Success Criteria

- [ ] LLM tasks use `pool="llm_api"` for rate limiting
- [ ] Retry configuration uses exponential backoff
- [ ] Entity extraction returns structured data with token counts
- [ ] Summarization and classification chain correctly
- [ ] Quality gate routes based on confidence threshold
- [ ] Asset outlet triggers downstream vector store pipeline
- [ ] Cost tracking accumulates across the DAG run
- [ ] All tasks handle failures gracefully with clear logging

## Hints

<details>
<summary>Hint 1: Token Tracking Pattern</summary>

```python
@task(pool="llm_api", on_success_callback=track_llm_cost)
def extract_entities(document: dict) -> dict:
    """Extract with token tracking."""
    response = call_llm(document["content"], prompt=EXTRACTION_PROMPT)

    # Push token usage for callback
    from airflow.models import TaskInstance

    ti = TaskInstance.get_current()
    ti.xcom_push(
        key="token_usage",
        value={
            "model": response["model"],
            "input_tokens": response["usage"]["prompt_tokens"],
            "output_tokens": response["usage"]["completion_tokens"],
        },
    )

    return response["extracted_entities"]
```

</details>

<details>
<summary>Hint 2: Branch Task Pattern</summary>

```python
@task.branch
def quality_gate(enrichment_result: dict) -> str:
    """
    Branch based on overall confidence.

    Important: Return the exact task_id string, not a task reference.
    """
    from airflow.models import Variable

    threshold = float(Variable.get("llm_confidence_threshold", default_var="0.8"))
    confidence = enrichment_result["classification"]["confidence"]

    logger.info(f"Quality gate: confidence={confidence}, threshold={threshold}")

    if confidence >= threshold:
        return "publish_enriched"
    return "manual_review_queue"
```

</details>

<details>
<summary>Hint 3: Mock LLM for Testing</summary>

```python
def mock_llm_call(content: str, prompt: str) -> dict:
    """Mock LLM for testing without API calls."""
    import hashlib
    import random

    # Deterministic but varied mock response
    seed = int(hashlib.md5(content.encode()).hexdigest()[:8], 16)
    random.seed(seed)

    return {
        "model": "gpt-3.5-turbo",
        "usage": {
            "prompt_tokens": len(content.split()) * 2,
            "completion_tokens": random.randint(50, 200),
        },
        "content": {
            "entities": ["Entity1", "Entity2"],
            "summary": f"Summary of: {content[:50]}...",
            "confidence": random.uniform(0.6, 0.95),
        },
    }
```

</details>

<details>
<summary>Hint 4: Handling Branch Trigger Rules</summary>

```python
from airflow.utils.trigger_rule import TriggerRule

# After the branch, downstream tasks need special trigger rules
# because only ONE path will run


@task(
    outlets=[ENRICHED_DOCS_ASSET],
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)
def publish_enriched(result: dict) -> dict:
    """Only runs if quality_gate returned this task."""
    # ...


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def manual_review_queue(result: dict) -> None:
    """Only runs if quality_gate returned this task."""
    # ...
```

</details>

## Files

- **Starter**: `../starter/exercise_16_3_llm_processing_starter.py`
- **Solution**: `../solutions/dags/processing_pipeline.py`

## Estimated Time

4 hours

## References

- [Module 02: TaskFlow API](../../02-taskflow-api/README.md) - XCom and task chaining
- [Module 09: Production Patterns](../../09-production-patterns/README.md) - Retry strategies
- [Module 14: Resource Management](../../14-resource-management/README.md) - Pool configuration
- [Module 15: AI/ML Orchestration](../../15-ai-ml-orchestration/README.md) - LLM patterns

---

[Previous: Exercise 16.2 - Ingestion Pipeline](exercise_16_2_ingestion.md) | [Next: Exercise 16.4 - Vector Store Pipeline](exercise_16_4_vector_store.md)
