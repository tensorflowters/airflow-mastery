# Module 16: Capstone Project

Build a production-grade AI-powered document processing pipeline that synthesizes concepts from all previous modules.

## Learning Objectives

By completing this capstone, you will demonstrate mastery of:

| Skill Area            | Modules Applied | Concepts                                          |
| --------------------- | --------------- | ------------------------------------------------- |
| DAG Architecture      | 01-04           | Multi-DAG orchestration, TaskFlow, trigger rules  |
| Data-Aware Scheduling | 05              | Asset dependencies, materialization, lineage      |
| Dynamic Processing    | 06              | Task mapping, parallel processing, cross-product  |
| Quality Assurance     | 07              | Testing, validation, debugging                    |
| Kubernetes Deployment | 08              | Pod templates, GPU scheduling, resource isolation |
| Production Patterns   | 09              | Retry strategies, alerting, SLA monitoring        |
| Advanced Topics       | 10              | Custom timetables, plugins, optimization          |
| Deferrable Operations | 11              | Async sensors, custom triggers                    |
| API Integration       | 12              | REST API, programmatic control                    |
| Security              | 13              | Secrets management, connections                   |
| Resource Management   | 14              | Pools, priorities, concurrency                    |
| AI/ML Orchestration   | 15              | LLM pipelines, embeddings, vector stores          |

## Prerequisites

- **Completed**: Modules 00-15
- **API Access**: OpenAI API key (or compatible LLM provider)
- **Infrastructure**: Docker Desktop with Kubernetes enabled
- **Optional**: GPU for accelerated embedding generation

## Project Overview

### Intelligent Document Processing Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DOCUMENT PROCESSING PLATFORM                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   Ingestion  │───▶│  Processing  │───▶│   Storage    │              │
│  │     DAG      │    │     DAG      │    │     DAG      │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│         │                   │                   │                       │
│         ▼                   ▼                   ▼                       │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   Raw Docs   │    │   Enriched   │    │   Vector     │              │
│  │   [Asset]    │    │   [Asset]    │    │   Store      │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                     MONITORING DAG                                │  │
│  │   • SLA Tracking  • Quality Metrics  • Resource Usage            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### What You'll Build

1. **Ingestion Pipeline**: Ingest documents from multiple sources with deferrable sensors
2. **Processing Pipeline**: Extract, summarize, and classify using LLM chains
3. **Vector Store Pipeline**: Generate embeddings and maintain searchable index
4. **Monitoring Pipeline**: Track SLAs, quality metrics, and system health

## Exercises

### Phase 1: Foundation

| Exercise                                 | Topic            | Time | Description                           |
| ---------------------------------------- | ---------------- | ---- | ------------------------------------- |
| [16.1](exercises/exercise_16_1_setup.md) | Foundation Setup | 2h   | Project structure, connections, pools |

### Phase 2: Core Pipelines

| Exercise                                          | Topic              | Time | Description                            |
| ------------------------------------------------- | ------------------ | ---- | -------------------------------------- |
| [16.2](exercises/exercise_16_2_ingestion.md)      | Ingestion Pipeline | 4h   | Sensors, dynamic tasks, error handling |
| [16.3](exercises/exercise_16_3_llm_processing.md) | LLM Processing     | 4h   | LLM chains, retries, quality gates     |
| [16.4](exercises/exercise_16_4_vector_store.md)   | Vector Store       | 3h   | Embeddings, indexing, custom triggers  |

### Phase 3: Production

| Exercise                                      | Topic      | Time | Description                         |
| --------------------------------------------- | ---------- | ---- | ----------------------------------- |
| [16.5](exercises/exercise_16_5_deployment.md) | Deployment | 4h   | Kubernetes, GPU scheduling, scaling |
| [16.6](exercises/exercise_16_6_monitoring.md) | Monitoring | 3h   | Observability, alerting, dashboards |
| [16.7](exercises/exercise_16_7_testing.md)    | Testing    | 4h   | Unit, integration, E2E tests        |

**Total Estimated Time: ~24 hours**

## Project Structure

```
16-capstone/
├── README.md                    # This file
├── exercises/
│   ├── exercise_16_1_setup.md
│   ├── exercise_16_2_ingestion.md
│   ├── exercise_16_3_llm_processing.md
│   ├── exercise_16_4_vector_store.md
│   ├── exercise_16_5_deployment.md
│   ├── exercise_16_6_monitoring.md
│   └── exercise_16_7_testing.md
├── solutions/
│   ├── dags/
│   │   ├── ingestion_pipeline.py
│   │   ├── processing_pipeline.py
│   │   ├── vector_store_pipeline.py
│   │   └── monitoring_pipeline.py
│   ├── operators/
│   │   └── llm_operator.py
│   ├── sensors/
│   │   └── vector_store_sensor.py
│   ├── triggers/
│   │   └── indexing_trigger.py
│   └── config/
│       └── pools.yaml
└── starter/
    └── capstone_starter.py
```

## Evaluation Criteria

### Functional Requirements (60 points)

| Requirement                                 | Points |
| ------------------------------------------- | ------ |
| Document ingestion works end-to-end         | 15     |
| LLM processing with proper error handling   | 15     |
| Vector store population and querying        | 10     |
| Asset-based DAG dependencies work correctly | 10     |
| Monitoring and alerting functional          | 10     |

### Production Readiness (25 points)

| Requirement                                | Points |
| ------------------------------------------ | ------ |
| Proper retry strategies and error handling | 5      |
| Resource management (pools, priorities)    | 5      |
| Kubernetes deployment configuration        | 5      |
| Security (secrets management)              | 5      |
| Comprehensive logging                      | 5      |

### Code Quality (15 points)

| Requirement                      | Points |
| -------------------------------- | ------ |
| Test coverage > 80%              | 5      |
| Code follows project conventions | 5      |
| Documentation complete           | 5      |

## Getting Started

1. **Verify Prerequisites**

   ```bash
   # Check Python and uv
   python --version  # Should be 3.11+
   uv --version

   # Check Kubernetes
   kubectl cluster-info

   # Verify API key is set
   echo $OPENAI_API_KEY
   ```

2. **Start with Exercise 16.1**

   ```bash
   # Open the first exercise
   cat modules/16-capstone/exercises/exercise_16_1_setup.md
   ```

3. **Follow the Phased Approach**
   - Complete exercises in order
   - Each builds on the previous
   - Test incrementally

## Tips for Success

1. **Use the Starter Template**: Begin with `starter/capstone_starter.py` for scaffolding
2. **Reference Solutions**: Check `solutions/` when stuck, but try first
3. **Test Incrementally**: Validate each component before moving on
4. **Leverage Previous Modules**: Refer back to specific module exercises
5. **Ask Questions**: Complex integrations often need clarification

## Key Concepts Demonstrated

### Multi-DAG Asset Orchestration

```python
# Producer DAG creates asset
@dag(schedule="@hourly")
def ingestion_dag():
    @task(outlets=[Asset("documents.raw")])
    def ingest_documents(): ...


# Consumer DAG triggered by asset
@dag(schedule=Asset("documents.raw"))
def processing_dag(): ...
```

### Production-Grade LLM Integration

```python
@task(
    pool="llm_api",
    retries=3,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
)
def extract_entities(doc: dict) -> dict:
    """LLM call with proper error handling."""
    ...
```

### Deferrable Async Operations

```python
wait_for_indexing = VectorStoreIndexingSensor(
    task_id="wait_for_indexing",
    collection_name="documents",
    deferrable=True,  # Release worker during wait
    poke_interval=60,
)
```

---

## Additional Resources

- [Apache Airflow 3.x Documentation](https://airflow.apache.org/docs/)
- [LangChain Integration Patterns](https://python.langchain.com/docs/)
- [Vector Database Best Practices](https://www.pinecone.io/learn/)
- [Kubernetes Executor Guide](../08-kubernetes-executor/README.md)

---

[← Module 15: AI/ML Orchestration](../15-ai-ml-orchestration/README.md) | [Back to Curriculum](../README.md)
