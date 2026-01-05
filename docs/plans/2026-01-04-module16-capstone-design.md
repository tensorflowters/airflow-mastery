# Module 16: Capstone Project Design

## Overview

A comprehensive capstone project that synthesizes concepts from all 16 modules into a production-grade AI-powered data pipeline. Students build an end-to-end system demonstrating mastery of Apache Airflow 3.x.

## Project: Intelligent Document Processing Pipeline

### Business Context

Build a production-ready document processing system that:

- Ingests documents from multiple sources
- Extracts and enriches data using LLMs
- Generates embeddings for semantic search
- Maintains a vector store for retrieval
- Provides monitoring and alerting
- Runs on Kubernetes with proper resource management

### Learning Objectives

By completing this capstone, students will demonstrate:

1. **DAG Architecture** (Modules 01-04)
    - Complex multi-DAG orchestration
    - Producer-consumer patterns with Assets
    - Trigger rules for error handling paths

2. **Data-Aware Scheduling** (Module 05)
    - Asset-based dependencies across DAGs
    - Materialization tracking
    - Data lineage

3. **Dynamic Processing** (Module 06)
    - Dynamic task mapping for parallel document processing
    - Cross-product operations for multi-model evaluation

4. **Quality Assurance** (Module 07)
    - Comprehensive test suite
    - DAG integrity validation
    - Integration testing

5. **Kubernetes Deployment** (Module 08)
    - Pod templates for different workload types
    - GPU scheduling for ML tasks
    - Resource isolation

6. **Production Patterns** (Module 09)
    - Retry strategies with exponential backoff
    - Error alerting and notifications
    - SLA monitoring

7. **Advanced Topics** (Module 10)
    - Custom timetable for business hours processing
    - Plugin for custom UI elements

8. **Deferrable Operations** (Module 11)
    - Deferrable sensors for vector store indexing
    - Custom triggers for API polling

9. **API Integration** (Module 12)
    - REST API for triggering pipelines
    - Status monitoring endpoints

10. **Security** (Module 13)
    - Secrets management for API keys
    - Connection management for databases

11. **Resource Management** (Module 14)
    - Pools for rate limiting
    - Priority weights for critical paths

12. **AI/ML Orchestration** (Module 15)
    - LLM chain orchestration
    - Embedding generation
    - Vector store management

---

## Project Architecture

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
│  │   📄 Raw     │    │   📊 LLM     │    │   🔍 Vector  │              │
│  │   Documents  │    │   Enriched   │    │   Store      │              │
│  │   [Asset]    │    │   [Asset]    │    │   [Asset]    │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                     MONITORING DAG                                │  │
│  │   • SLA Tracking  • Quality Metrics  • Resource Usage            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Exercises

### Exercise 16.1: Foundation Setup

**Objective**: Set up the project structure and base configuration.

**Tasks**:

1. Create project directory structure
2. Configure connections for LLM API, vector store, and storage
3. Define asset hierarchy
4. Set up pools and priorities
5. Create base DAG templates

**Modules Applied**: 00, 13, 14

---

### Exercise 16.2: Ingestion Pipeline

**Objective**: Build the document ingestion DAG.

**Tasks**:

1. Create sensors for source systems (S3, SFTP, API)
2. Implement deferrable waiting for large batch arrivals
3. Dynamic task mapping for parallel file processing
4. Asset creation for raw documents
5. Error handling with dead-letter queue

**Modules Applied**: 03, 05, 06, 11

**Key Code Patterns**:

```python
@dag(schedule=Asset("source_system.files_available"))
def ingestion_pipeline():
    # Deferrable sensor for efficient waiting
    wait_for_files = FileSensor(
        task_id="wait_for_files",
        filepath="/data/incoming/*.pdf",
        deferrable=True,
        poke_interval=60,
    )

    # Dynamic mapping for parallel processing
    @task
    def list_files() -> list[str]:
        return glob.glob("/data/incoming/*.pdf")

    @task(map_index_template="{{ file }}")
    def process_file(file: str) -> dict:
        # Extract metadata and content
        ...
```

---

### Exercise 16.3: LLM Processing Pipeline

**Objective**: Build the AI-powered data enrichment DAG.

**Tasks**:

1. Chain LLM calls for extraction, summarization, classification
2. Implement retry strategies for LLM API failures
3. Rate limiting with pools
4. Quality validation of LLM outputs
5. Asset creation for enriched documents

**Modules Applied**: 02, 09, 14, 15

**Key Code Patterns**:

```python
@task(
    pool="llm_api",
    pool_slots=1,
    retries=3,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
)
def extract_entities(document: dict) -> dict:
    """LLM-powered entity extraction with retry."""
    ...


@task.branch
def quality_gate(result: dict) -> str:
    """Branch based on extraction quality."""
    if result["confidence"] > 0.9:
        return "high_confidence_path"
    return "human_review_path"
```

---

### Exercise 16.4: Vector Store Pipeline

**Objective**: Build the embedding and indexing DAG.

**Tasks**:

1. Generate embeddings with parallel processing
2. Batch upsert to vector store
3. Custom trigger for indexing completion
4. Asset-based dependency on processing DAG
5. Index health validation

**Modules Applied**: 06, 11, 15

**Key Code Patterns**:

```python
@dag(schedule=Asset("documents.enriched"))
def vector_store_pipeline():
    @task.map
    def generate_embeddings(docs: list[dict]) -> list[dict]:
        """Parallel embedding generation."""
        ...

    wait_indexing = VectorStoreIndexingSensor(
        task_id="wait_indexing",
        collection_name="documents",
        deferrable=True,
    )
```

---

### Exercise 16.5: Production Deployment

**Objective**: Deploy the pipeline to Kubernetes with production patterns.

**Tasks**:

1. Create pod templates for different workload types
2. Configure GPU scheduling for embedding generation
3. Set up HPA for dynamic scaling
4. Implement SLA monitoring
5. Create alerting for failures

**Modules Applied**: 08, 09, 10

**Key Code Patterns**:

```python
# Pod template for GPU tasks
gpu_pod_template = """
apiVersion: v1
kind: Pod
spec:
  containers:
  - name: base
    resources:
      limits:
        nvidia.com/gpu: 1
      requests:
        memory: "8Gi"
        cpu: "2"
  tolerations:
  - key: "nvidia.com/gpu"
    operator: "Exists"
    effect: "NoSchedule"
"""


@task(executor_config={"pod_template": gpu_pod_template})
def generate_embeddings_gpu(batch: list[dict]) -> list[dict]: ...
```

---

### Exercise 16.6: Monitoring & Observability

**Objective**: Build comprehensive monitoring for the pipeline.

**Tasks**:

1. Create monitoring DAG for system health
2. Implement custom metrics collection
3. Build status dashboard (REST API integration)
4. Set up SLA miss callbacks
5. Quality metrics tracking

**Modules Applied**: 09, 10, 12, 14

---

### Exercise 16.7: Testing & Validation

**Objective**: Create comprehensive test suite for the pipeline.

**Tasks**:

1. DAG integrity tests for all DAGs
2. Unit tests for custom operators and sensors
3. Integration tests with mock services
4. End-to-end validation pipeline
5. Performance benchmarking

**Modules Applied**: 07

---

## Evaluation Criteria

### Functional Requirements (60%)

| Requirement                                 | Points |
| ------------------------------------------- | ------ |
| Document ingestion works end-to-end         | 15     |
| LLM processing with proper error handling   | 15     |
| Vector store population and querying        | 10     |
| Asset-based DAG dependencies work correctly | 10     |
| Monitoring and alerting functional          | 10     |

### Production Readiness (25%)

| Requirement                                | Points |
| ------------------------------------------ | ------ |
| Proper retry strategies and error handling | 5      |
| Resource management (pools, priorities)    | 5      |
| Kubernetes deployment configuration        | 5      |
| Security (secrets management)              | 5      |
| Comprehensive logging                      | 5      |

### Code Quality (15%)

| Requirement                      | Points |
| -------------------------------- | ------ |
| Test coverage > 80%              | 5      |
| Code follows project conventions | 5      |
| Documentation complete           | 5      |

---

## Deliverables

1. **DAG Files**
    - `ingestion_pipeline.py`
    - `processing_pipeline.py`
    - `vector_store_pipeline.py`
    - `monitoring_pipeline.py`

2. **Custom Components**
    - `operators/llm_operator.py`
    - `sensors/vector_store_sensor.py`
    - `triggers/indexing_trigger.py`

3. **Configuration**
    - `config/pools.yaml`
    - `config/connections.yaml`
    - `k8s/pod_templates/`

4. **Tests**
    - `tests/test_dags.py`
    - `tests/test_operators.py`
    - `tests/integration/`

5. **Documentation**
    - `README.md` with setup instructions
    - Architecture diagram
    - Runbook for common operations

---

## Implementation Notes

### Phased Approach

**Phase 1** (Exercises 16.1-16.2): Foundation and ingestion

- Focus on asset patterns and dynamic tasks

**Phase 2** (Exercises 16.3-16.4): AI processing

- Focus on LLM orchestration and vector stores

**Phase 3** (Exercises 16.5-16.7): Production

- Focus on deployment, monitoring, testing

### Prerequisites

- Completed Modules 00-15
- Access to OpenAI API (or compatible LLM)
- Docker Desktop with Kubernetes enabled
- Optional: GPU for accelerated embeddings

---

## File Structure

```
modules/16-capstone/
├── README.md                    # Module overview
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
│       ├── pools.yaml
│       └── pod_templates/
└── starter/
    └── capstone_starter.py      # Starter template
```

---

## Timeline Estimate

| Phase         | Estimated Time |
| ------------- | -------------- |
| Exercise 16.1 | 2 hours        |
| Exercise 16.2 | 4 hours        |
| Exercise 16.3 | 4 hours        |
| Exercise 16.4 | 3 hours        |
| Exercise 16.5 | 4 hours        |
| Exercise 16.6 | 3 hours        |
| Exercise 16.7 | 4 hours        |
| **Total**     | **~24 hours**  |

---

## Success Criteria

The capstone is complete when:

1. All DAGs are deployable and functional
2. End-to-end document processing works
3. Vector store is searchable with semantic queries
4. Monitoring detects and alerts on failures
5. Tests pass with >80% coverage
6. Deployment to Kubernetes works
7. Documentation is complete and accurate
