# Case Study: Modern RAG Pipeline Architecture

## Industry Context

**Trend**: Retrieval-Augmented Generation (RAG) has become the standard pattern for production LLM applications
**Challenge**: Orchestrate complex document processing, embedding generation, and retrieval pipelines
**Requirements**: Maintain freshness, ensure quality, control costs, scale horizontally

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MODERN RAG ARCHITECTURE                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │   Document   │───▶│   Content    │───▶│   Chunking   │                  │
│  │   Sources    │    │   Extraction │    │   Strategy   │                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│                                                │                           │
│                                                ▼                           │
│  ┌────────────────────────────────────────────────────────────┐            │
│  │                    AIRFLOW ORCHESTRATION                    │            │
│  │                                                              │            │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐            │            │
│  │  │  Embedding │  │   Vector   │  │   Index    │            │            │
│  │  │  Pipeline  │──│   Store    │──│   Refresh  │            │            │
│  │  │  (Batch)   │  │   Upsert   │  │   Sensor   │            │            │
│  │  └────────────┘  └────────────┘  └────────────┘            │            │
│  │                                                              │            │
│  └────────────────────────────────────────────────────────────┘            │
│                                                │                           │
│                                                ▼                           │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │   Query      │───▶│   Retrieval  │───▶│   LLM        │                  │
│  │   Router     │    │   Engine     │    │   Generation │                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Patterns Used

### 1. Document Ingestion with Dynamic Mapping

Process documents in parallel with intelligent chunking:

```python
from datetime import datetime

from airflow.sdk import Asset, dag, task

# Assets for triggering downstream pipelines
raw_documents = Asset("rag/raw_documents")
document_chunks = Asset("rag/chunks")
vector_embeddings = Asset("rag/embeddings")


@dag(
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    tags=["rag", "ingestion"],
)
def rag_document_ingestion():
    @task
    def discover_documents(source_path: str) -> list[dict]:
        """Discover new/modified documents for processing."""
        documents = []

        for doc in scan_directory(source_path):
            if is_new_or_modified(doc):
                documents.append(
                    {
                        "path": doc.path,
                        "type": doc.extension,
                        "size": doc.size,
                        "modified": doc.modified_at.isoformat(),
                    }
                )

        return documents

    @task
    def extract_content(doc_info: dict) -> dict:
        """Extract text content from document."""
        doc_type = doc_info["type"]
        path = doc_info["path"]

        # Route to appropriate extractor
        if doc_type == "pdf":
            content = extract_pdf(path)
        elif doc_type == "docx":
            content = extract_docx(path)
        elif doc_type == "html":
            content = extract_html(path)
        else:
            content = extract_text(path)

        return {
            "path": path,
            "content": content,
            "char_count": len(content),
            "extracted_at": datetime.now().isoformat(),
        }

    @task
    def chunk_document(extracted: dict) -> list[dict]:
        """Split document into semantic chunks."""
        content = extracted["content"]
        path = extracted["path"]

        # Semantic chunking with overlap
        chunks = semantic_chunk(
            text=content,
            chunk_size=512,
            overlap=50,
            separators=["\n\n", "\n", ". ", " "],
        )

        return [
            {
                "doc_path": path,
                "chunk_index": i,
                "content": chunk,
                "char_count": len(chunk),
            }
            for i, chunk in enumerate(chunks)
        ]

    @task(outlets=[document_chunks])
    def persist_chunks(all_chunks: list[list[dict]]) -> dict:
        """Flatten and persist all chunks."""
        flat_chunks = [c for chunks in all_chunks for c in chunks]

        # Store chunks with metadata
        chunk_ids = store_chunks(flat_chunks)

        return {
            "documents_processed": len(all_chunks),
            "total_chunks": len(flat_chunks),
            "chunk_ids": chunk_ids,
        }

    # Pipeline flow with dynamic mapping
    docs = discover_documents("/data/documents")
    extracted = extract_content.expand(doc_info=docs)
    chunks = chunk_document.expand(extracted=extracted)
    persist_chunks(chunks)
```

### 2. Parallel Embedding Generation with Rate Limiting

Generate embeddings efficiently while respecting API limits:

```python
import time
from dataclasses import dataclass, field


@dataclass
class EmbeddingRateLimiter:
    """Rate limiter for embedding API calls."""

    tokens_per_minute: int = 150000
    requests_per_minute: int = 3000
    _token_count: int = 0
    _request_count: int = 0
    _window_start: float = field(default_factory=time.time)

    def wait_if_needed(self, estimated_tokens: int):
        """Wait if rate limits would be exceeded."""
        current_time = time.time()
        elapsed = current_time - self._window_start

        # Reset window every minute
        if elapsed >= 60:
            self._token_count = 0
            self._request_count = 0
            self._window_start = current_time
            return

        # Check if we need to wait
        if (
            self._token_count + estimated_tokens > self.tokens_per_minute
            or self._request_count + 1 > self.requests_per_minute
        ):
            sleep_time = 60 - elapsed
            time.sleep(sleep_time)
            self._token_count = 0
            self._request_count = 0
            self._window_start = time.time()

        self._token_count += estimated_tokens
        self._request_count += 1


@dag(
    schedule=[document_chunks],  # Triggered by chunk asset
    tags=["rag", "embeddings"],
)
def rag_embedding_pipeline():
    @task
    def get_pending_chunks() -> list[dict]:
        """Get chunks that need embedding generation."""
        return query_chunks_without_embeddings(limit=10000)

    @task
    def batch_chunks(chunks: list[dict], batch_size: int = 100) -> list[list[dict]]:
        """Batch chunks for efficient API calls."""
        return [chunks[i : i + batch_size] for i in range(0, len(chunks), batch_size)]

    @task
    def generate_embeddings(batch: list[dict]) -> list[dict]:
        """Generate embeddings for a batch of chunks."""
        rate_limiter = EmbeddingRateLimiter()

        results = []
        texts = [chunk["content"] for chunk in batch]

        # Estimate tokens (rough approximation)
        estimated_tokens = sum(len(t) // 4 for t in texts)
        rate_limiter.wait_if_needed(estimated_tokens)

        # Generate embeddings
        embeddings = embedding_model.embed(texts)

        for chunk, embedding in zip(batch, embeddings):
            results.append(
                {
                    "chunk_id": chunk["chunk_id"],
                    "embedding": embedding,
                    "model": "text-embedding-3-small",
                    "dimensions": len(embedding),
                }
            )

        return results

    @task(outlets=[vector_embeddings])
    def store_embeddings(all_embeddings: list[list[dict]]) -> dict:
        """Store embeddings in vector database."""
        flat_embeddings = [e for batch in all_embeddings for e in batch]

        # Upsert to vector store
        upsert_result = vector_store.upsert(
            vectors=[
                {
                    "id": e["chunk_id"],
                    "values": e["embedding"],
                    "metadata": {"model": e["model"]},
                }
                for e in flat_embeddings
            ]
        )

        return {
            "embeddings_stored": len(flat_embeddings),
            "upsert_status": upsert_result,
        }

    chunks = get_pending_chunks()
    batches = batch_chunks(chunks)
    embeddings = generate_embeddings.expand(batch=batches)
    store_embeddings(embeddings)
```

### 3. Vector Store Sensor for Index Readiness

Use deferrable sensors to wait for index updates:

```python
import asyncio
from collections.abc import AsyncIterator

from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent


class VectorIndexTrigger(BaseTrigger):
    """Trigger that waits for vector index to be ready."""

    def __init__(
        self,
        index_name: str,
        expected_vectors: int,
        poll_interval: float = 30.0,
        timeout: float = 3600.0,
    ):
        super().__init__()
        self.index_name = index_name
        self.expected_vectors = expected_vectors
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self):
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "index_name": self.index_name,
                "expected_vectors": self.expected_vectors,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Poll for index readiness."""
        from datetime import datetime, timedelta

        start_time = datetime.utcnow()
        timeout_at = start_time + timedelta(seconds=self.timeout)

        while datetime.utcnow() < timeout_at:
            stats = await self._get_index_stats()

            if stats["vector_count"] >= self.expected_vectors:
                if stats["status"] == "ready":
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "index": self.index_name,
                            "vector_count": stats["vector_count"],
                        }
                    )
                    return

            self.log.info(f"Index {self.index_name}: {stats['vector_count']}/{self.expected_vectors} vectors")
            await asyncio.sleep(self.poll_interval)

        yield TriggerEvent(
            {
                "status": "timeout",
                "message": f"Index not ready after {self.timeout}s",
            }
        )


class VectorIndexSensor(BaseSensorOperator):
    """Deferrable sensor for vector index readiness."""

    def __init__(
        self,
        index_name: str,
        expected_vectors: int,
        deferrable: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.index_name = index_name
        self.expected_vectors = expected_vectors
        self._deferrable = deferrable

    def execute(self, context):
        # Quick check first
        stats = get_index_stats(self.index_name)
        if stats["vector_count"] >= self.expected_vectors:
            return stats

        if self._deferrable:
            self.defer(
                trigger=VectorIndexTrigger(
                    index_name=self.index_name,
                    expected_vectors=self.expected_vectors,
                    timeout=self.timeout,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context)

    def execute_complete(self, context, event):
        if event["status"] == "success":
            return event
        raise AirflowException(event["message"])
```

### 4. LLM Chain Orchestration with Cost Tracking

Multi-step LLM pipelines with cost monitoring:

```python
from dataclasses import dataclass, field


@dataclass
class LLMCostTracker:
    """Track LLM API costs across pipeline."""

    daily_budget: float = 100.0
    current_spend: float = 0.0
    call_history: list = field(default_factory=list)

    # Pricing per 1K tokens (example rates)
    PRICING = {
        "gpt-4o": {"input": 0.005, "output": 0.015},
        "gpt-4o-mini": {"input": 0.00015, "output": 0.0006},
        "claude-3-5-sonnet": {"input": 0.003, "output": 0.015},
    }

    def track_call(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
        operation: str,
    ) -> float:
        """Track an LLM call and return cost."""
        pricing = self.PRICING.get(model, self.PRICING["gpt-4o-mini"])

        cost = input_tokens * pricing["input"] / 1000 + output_tokens * pricing["output"] / 1000

        self.current_spend += cost
        self.call_history.append(
            {
                "model": model,
                "operation": operation,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost": cost,
            }
        )

        if self.current_spend > self.daily_budget * 0.8:
            self.log.warning(f"LLM spend at {self.current_spend / self.daily_budget:.0%} of budget")

        return cost

    def get_summary(self) -> dict:
        """Get cost summary."""
        return {
            "total_spend": self.current_spend,
            "budget_remaining": self.daily_budget - self.current_spend,
            "calls": len(self.call_history),
            "by_operation": self._group_by_operation(),
        }


@dag(schedule="@hourly", tags=["rag", "generation"])
def rag_query_pipeline():
    @task
    def get_pending_queries() -> list[dict]:
        """Get queries waiting for processing."""
        return fetch_pending_queries(limit=100)

    @task
    def retrieve_context(query: dict) -> dict:
        """Retrieve relevant context from vector store."""
        # Generate query embedding
        query_embedding = embedding_model.embed(query["text"])

        # Search vector store
        results = vector_store.query(
            vector=query_embedding,
            top_k=5,
            include_metadata=True,
        )

        return {
            "query_id": query["id"],
            "query_text": query["text"],
            "context": [r["metadata"]["content"] for r in results],
            "scores": [r["score"] for r in results],
        }

    @task
    def generate_response(retrieval_result: dict) -> dict:
        """Generate response using retrieved context."""
        cost_tracker = LLMCostTracker()

        query = retrieval_result["query_text"]
        context = "\n\n".join(retrieval_result["context"])

        # Build prompt
        prompt = f"""Based on the following context, answer the question.

Context:
{context}

Question: {query}

Answer:"""

        # Generate response
        response = llm.generate(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
        )

        # Track cost
        cost_tracker.track_call(
            model="gpt-4o-mini",
            input_tokens=response["usage"]["prompt_tokens"],
            output_tokens=response["usage"]["completion_tokens"],
            operation="generate_response",
        )

        return {
            "query_id": retrieval_result["query_id"],
            "response": response["content"],
            "cost": cost_tracker.get_summary(),
            "sources": retrieval_result["context"][:3],
        }

    @task
    def store_responses(responses: list[dict]) -> dict:
        """Store generated responses."""
        total_cost = sum(r["cost"]["total_spend"] for r in responses)

        store_query_responses(responses)

        return {
            "responses_generated": len(responses),
            "total_cost": total_cost,
        }

    queries = get_pending_queries()
    retrieval_results = retrieve_context.expand(query=queries)
    responses = generate_response.expand(retrieval_result=retrieval_results)
    store_responses(responses)
```

## Lessons Learned

### What Worked

1. **Asset-driven pipelines** - Documents → Chunks → Embeddings cascade automatically
2. **Dynamic mapping for parallelism** - Scales with document volume
3. **Deferrable sensors for indexing** - Long indexing doesn't block workers
4. **Cost tracking integration** - Prevents runaway LLM costs

### Challenges Encountered

1. **Chunking strategy selection** - Required experimentation for optimal retrieval
2. **Embedding model versioning** - Model changes require re-embedding
3. **Context window management** - Large retrievals exceeded LLM limits
4. **Latency vs. freshness** - Batch processing adds delay to knowledge updates

### Key Metrics

| Metric                       | Typical Values      |
| ---------------------------- | ------------------- |
| Document → Queryable latency | 15-60 minutes       |
| Embedding generation cost    | $0.0001/chunk       |
| Vector upsert throughput     | 1000 vectors/second |
| Query generation latency     | 2-5 seconds         |

## Code Patterns

### Pattern: Asset-Driven RAG Pipeline

```python
from airflow.sdk import Asset

raw_docs = Asset("rag/documents")
chunks = Asset("rag/chunks")
embeddings = Asset("rag/embeddings")


# Triggered by document changes
@dag(schedule=[raw_docs])
def chunk_pipeline():
    pass


# Triggered by new chunks
@dag(schedule=[chunks])
def embedding_pipeline():
    pass
```

### Pattern: Batch Embedding with Rate Limiting

```python
@task
def generate_embeddings_batch(texts: list[str]) -> list[list[float]]:
    """Generate embeddings with rate limiting."""
    limiter = TokenBucketRateLimiter(tokens_per_minute=150000)

    embeddings = []
    for batch in batch_texts(texts, size=100):
        tokens = estimate_tokens(batch)
        limiter.wait_if_needed(tokens)
        embeddings.extend(embed_batch(batch))

    return embeddings
```

## Related Exercises

| Exercise                                                                                                                 | Concepts Applied                        |
| ------------------------------------------------------------------------------------------------------------------------ | --------------------------------------- |
| [Exercise 15.1: RAG Pipeline](../../modules/15-ai-ml-orchestration/exercises/exercise_15_1_rag_pipeline.md)              | Complete RAG ingestion pipeline         |
| [Exercise 15.2: LLM Chain](../../modules/15-ai-ml-orchestration/exercises/exercise_15_2_llm_chain.md)                    | Multi-step LLM orchestration            |
| [Exercise 11.4: Vector Store Sensor](../../modules/11-sensors-deferrable/exercises/exercise_11_4_vector_store_sensor.md) | Deferrable sensors for indexing         |
| [Exercise 6.4: Parallel Embeddings](../../modules/06-dynamic-tasks/exercises/exercise_6_4_parallel_embeddings.md)        | Dynamic mapping for parallel processing |

## Further Reading

- [LangChain + Airflow Integration](https://python.langchain.com/)
- [Pinecone Best Practices](https://docs.pinecone.io/)
- [OpenAI Embeddings Guide](https://platform.openai.com/docs/guides/embeddings)
- [Anthropic Claude Best Practices](https://docs.anthropic.com/)

---

[← Airbnb Experimentation](airbnb-experimentation.md) | [Back to Case Studies](README.md)
