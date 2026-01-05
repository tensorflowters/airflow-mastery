# Exercise 16.1: Foundation Setup

Set up the project structure, connections, and resource configuration for the intelligent document processing pipeline.

## Learning Goals

- Design a multi-DAG project structure for production workloads
- Configure connections for LLM APIs, vector stores, and cloud storage
- Define Asset hierarchies for data-aware scheduling
- Set up pools for resource management and rate limiting
- Create reusable DAG templates with common configurations

## Scenario

You are building an intelligent document processing platform that will:

1. **Ingest documents** from multiple sources (S3 buckets, APIs, file systems)
2. **Process documents** using LLM chains for extraction, summarization, and classification
3. **Store embeddings** in a vector database for semantic search
4. **Monitor pipeline health** with SLA tracking and quality metrics

Before writing any DAG code, you need to establish the foundation: project structure, external connections, data assets, and resource pools.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DOCUMENT PROCESSING PLATFORM                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   CONNECTIONS                    ASSETS                    POOLS            │
│   ┌──────────────┐              ┌──────────────┐          ┌──────────────┐  │
│   │ openai_      │              │ documents.   │          │ llm_api      │  │
│   │ default      │              │ raw          │          │ (5 slots)    │  │
│   └──────────────┘              └──────────────┘          └──────────────┘  │
│   ┌──────────────┐              ┌──────────────┐          ┌──────────────┐  │
│   │ qdrant_      │──────────────│ documents.   │          │ embedding_   │  │
│   │ default      │              │ enriched     │          │ api (10)     │  │
│   └──────────────┘              └──────────────┘          └──────────────┘  │
│   ┌──────────────┐              ┌──────────────┐          ┌──────────────┐  │
│   │ s3_default   │              │ documents.   │          │ database_    │  │
│   │              │              │ indexed      │          │ connections  │  │
│   └──────────────┘              └──────────────┘          │ (20 slots)   │  │
│                                                           └──────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Requirements

### Task 1: Create Project Directory Structure

Create a well-organized directory structure for the capstone project:

```
capstone/
├── dags/
│   ├── ingestion/
│   │   └── __init__.py
│   ├── processing/
│   │   └── __init__.py
│   ├── storage/
│   │   └── __init__.py
│   └── monitoring/
│       └── __init__.py
├── plugins/
│   ├── operators/
│   │   └── __init__.py
│   ├── sensors/
│   │   └── __init__.py
│   ├── triggers/
│   │   └── __init__.py
│   └── hooks/
│       └── __init__.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── conftest.py
├── config/
│   ├── connections.yaml
│   ├── pools.yaml
│   └── variables.yaml
└── scripts/
    └── setup_connections.py
```

**Acceptance Criteria:**

- All directories have `__init__.py` files where appropriate
- Test directories follow pytest conventions
- Config files use YAML format for declarative configuration

### Task 2: Configure External Connections

Set up connections for the three external services:

| Connection ID    | Type   | Purpose                      | Configuration                        |
| ---------------- | ------ | ---------------------------- | ------------------------------------ |
| `openai_default` | HTTP   | LLM API calls                | Host, API key in password field      |
| `qdrant_default` | HTTP   | Vector store operations      | Host, port, optional API key         |
| `s3_default`     | AWS S3 | Document storage and staging | Access key, secret, region, endpoint |

Create a script that provisions these connections programmatically:

```python
# scripts/setup_connections.py
from airflow import settings
from airflow.models import Connection


def create_connections():
    """Create all required connections for the capstone project."""
    connections = [
        Connection(
            conn_id="openai_default",
            conn_type="http",
            host="https://api.openai.com",
            password="{{ OPENAI_API_KEY }}",  # From environment
            extra={"Content-Type": "application/json", "timeout": 60},
        ),
        # Add qdrant_default and s3_default...
    ]

    session = settings.Session()
    for conn in connections:
        # Upsert logic here
        ...
```

**Acceptance Criteria:**

- Connections are created idempotently (can run multiple times)
- Secrets are loaded from environment variables, not hardcoded
- Connection extras include appropriate timeout and header configurations

### Task 3: Define Asset Hierarchy

Define the three-tier Asset hierarchy for data-aware scheduling:

| Asset Name           | Producer       | Consumers      | Description                  |
| -------------------- | -------------- | -------------- | ---------------------------- |
| `documents.raw`      | Ingestion DAG  | Processing DAG | Newly ingested raw documents |
| `documents.enriched` | Processing DAG | Storage DAG    | LLM-processed documents      |
| `documents.indexed`  | Storage DAG    | Monitoring DAG | Documents in vector store    |

Create an assets configuration module:

```python
# dags/assets.py
from airflow.sdk import Asset

# Document lifecycle assets
DOCUMENTS_RAW = Asset(
    name="documents.raw",
    uri="s3://capstone-bucket/raw/",
    extra={"format": "json", "schema_version": "1.0", "owner": "ingestion-team"},
)

DOCUMENTS_ENRICHED = Asset(
    name="documents.enriched",
    uri="s3://capstone-bucket/enriched/",
    extra={"format": "json", "schema_version": "1.0", "owner": "processing-team", "includes_embeddings": False},
)

DOCUMENTS_INDEXED = Asset(
    name="documents.indexed",
    uri="qdrant://collections/documents",
    extra={"vector_dimensions": 1536, "distance_metric": "cosine", "owner": "storage-team"},
)

# Export all assets for easy import
ALL_ASSETS = [DOCUMENTS_RAW, DOCUMENTS_ENRICHED, DOCUMENTS_INDEXED]
```

**Acceptance Criteria:**

- Each Asset has a unique URI that identifies its location
- Extra metadata includes schema version for compatibility tracking
- Assets can be imported and used across multiple DAGs

### Task 4: Set Up Resource Pools

Configure three pools to manage concurrent access to external resources:

| Pool Name              | Slots | Purpose                                  |
| ---------------------- | ----- | ---------------------------------------- |
| `llm_api`              | 5     | Rate limit LLM API calls (OpenAI tier 1) |
| `embedding_api`        | 10    | Embedding generation (higher throughput) |
| `database_connections` | 20    | Vector store and metadata DB connections |

Create a pools configuration file and setup script:

```yaml
# config/pools.yaml
pools:
  - name: llm_api
    slots: 5
    description: |
      Controls concurrent LLM API calls to prevent rate limiting.
      Based on OpenAI Tier 1 limits: 60 RPM for GPT-4.
      5 concurrent tasks with ~10s average = 30 RPM (50% headroom).

  - name: embedding_api
    slots: 10
    description: |
      Controls concurrent embedding API calls.
      Embedding models have higher rate limits (3000 RPM).
      10 concurrent tasks provides good throughput.

  - name: database_connections
    slots: 20
    description: |
      Controls concurrent database connections.
      Prevents overwhelming vector store or metadata DB.
      Matches typical connection pool sizes.
```

Create the pool setup script:

```python
# scripts/setup_pools.py
import yaml
from airflow import settings
from airflow.models import Pool


def create_pools(config_path: str = "config/pools.yaml"):
    """Create pools from YAML configuration."""
    with open(config_path) as f:
        config = yaml.safe_load(f)

    session = settings.Session()

    for pool_config in config["pools"]:
        pool = Pool.get_pool(pool_config["name"], session=session)

        if pool is None:
            pool = Pool(pool=pool_config["name"], slots=pool_config["slots"], description=pool_config["description"])
            session.add(pool)
        else:
            pool.slots = pool_config["slots"]
            pool.description = pool_config["description"]

    session.commit()
    print(f"Created/updated {len(config['pools'])} pools")


if __name__ == "__main__":
    create_pools()
```

**Acceptance Criteria:**

- Pools are created idempotently from YAML configuration
- Pool descriptions document the rationale for slot counts
- Script can be run during deployment to ensure pools exist

### Task 5: Create Base DAG Templates

Create reusable DAG templates with common configurations:

```python
# dags/common/dag_templates.py
from datetime import timedelta

from airflow.sdk.definitions.decorators import dag

# Common default arguments for all capstone DAGs
CAPSTONE_DEFAULT_ARGS = {
    "owner": "capstone-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# LLM task defaults (for tasks using LLM APIs)
LLM_TASK_DEFAULTS = {
    "pool": "llm_api",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=5),
}

# Embedding task defaults
EMBEDDING_TASK_DEFAULTS = {
    "pool": "embedding_api",
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "execution_timeout": timedelta(minutes=10),
}

# Database task defaults
DATABASE_TASK_DEFAULTS = {
    "pool": "database_connections",
    "retries": 3,
    "retry_delay": timedelta(seconds=5),
    "execution_timeout": timedelta(minutes=2),
}


def capstone_dag(dag_id: str, schedule: str | None = None, tags: list[str] | None = None, **kwargs):
    """Decorator factory for capstone DAGs with common configuration."""
    base_tags = ["capstone", "document-processing"]
    all_tags = base_tags + (tags or [])

    return dag(
        dag_id=dag_id,
        schedule=schedule,
        default_args=CAPSTONE_DEFAULT_ARGS,
        tags=all_tags,
        catchup=False,
        max_active_runs=1,
        doc_md=__doc__,
        **kwargs,
    )
```

Create example usage demonstrating the templates:

```python
# dags/ingestion/example_dag.py
from airflow.sdk.definitions.decorators import task
from common.dag_templates import LLM_TASK_DEFAULTS, capstone_dag


@capstone_dag(dag_id="example_ingestion", schedule="@hourly", tags=["ingestion"])
def example_ingestion_dag():
    """Example DAG demonstrating template usage."""

    @task(**LLM_TASK_DEFAULTS)
    def process_with_llm(document: dict) -> dict:
        """Task automatically uses llm_api pool with proper retries."""
        # Implementation here
        return document

    # DAG structure
    process_with_llm({"id": "test"})


example_ingestion_dag()
```

**Acceptance Criteria:**

- Templates provide sensible defaults for all capstone DAGs
- Task-specific defaults match the resource pools created in Task 4
- Templates are documented and easy to use

## Success Criteria

- [ ] Project structure created with all required directories
- [ ] Connections script creates all three connections idempotently
- [ ] Assets module defines the three-tier hierarchy correctly
- [ ] Pools configuration and script work correctly
- [ ] DAG templates demonstrate proper configuration inheritance
- [ ] All configuration is declarative (YAML) where possible
- [ ] Scripts are idempotent and safe to run multiple times

## Hints

<details>
<summary>Hint 1: Idempotent Connection Creation</summary>

Use `get_or_create` pattern for connections:

```python
from airflow import settings
from airflow.models import Connection


def upsert_connection(conn: Connection) -> None:
    """Create or update a connection."""
    session = settings.Session()

    existing = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if existing:
        existing.conn_type = conn.conn_type
        existing.host = conn.host
        existing.password = conn.password
        existing.extra = conn.extra
    else:
        session.add(conn)

    session.commit()
```

</details>

<details>
<summary>Hint 2: Environment Variable Integration</summary>

Load secrets from environment variables safely:

```python
import os


def get_secret(key: str, default: str = "") -> str:
    """Get secret from environment with validation."""
    value = os.environ.get(key, default)
    if not value and not default:
        raise ValueError(f"Required environment variable {key} not set")
    return value


# Usage
openai_key = get_secret("OPENAI_API_KEY")
```

</details>

<details>
<summary>Hint 3: Asset URI Schemes</summary>

Asset URIs should follow consistent patterns:

```python
# S3-style URIs
Asset("documents.raw", uri="s3://bucket/prefix/")

# Database-style URIs
Asset("documents.indexed", uri="qdrant://host:port/collection")

# File system URIs
Asset("documents.local", uri="file:///data/documents/")
```

</details>

<details>
<summary>Hint 4: Pool Slot Calculation</summary>

Calculate pool slots based on rate limits:

```python
# OpenAI GPT-4: 60 requests per minute (Tier 1)
# Target: 50% utilization for headroom
# Average task duration: ~10 seconds
# Concurrent tasks = (60 RPM * 0.5) / (60s / 10s) = 5 slots

LLM_POOL_SLOTS = 5

# Embeddings: 3000 RPM
# Average task duration: ~5 seconds
# Concurrent tasks = (3000 * 0.5) / (60 / 5) = 125
# Cap at 10 for resource management
EMBEDDING_POOL_SLOTS = 10
```

</details>

## Files

- **Configuration**: `config/pools.yaml`, `config/connections.yaml`
- **Scripts**: `scripts/setup_connections.py`, `scripts/setup_pools.py`
- **Templates**: `dags/common/dag_templates.py`, `dags/assets.py`
- **Solution**: `../solutions/config/pools.yaml`

## Estimated Time

2 hours

## Related Modules

- [Module 00: Environment Setup](../../00-environment-setup/README.md) - Development environment
- [Module 13: Connections and Secrets](../../13-connections-secrets/README.md) - Connection management
- [Module 14: Resource Management](../../14-resource-management/README.md) - Pools and priorities

## Next Steps

After completing this exercise:

1. Verify all connections work with a simple test DAG
2. Confirm pools appear in the Airflow UI under Admin > Pools
3. Test the DAG templates with a minimal example
4. Proceed to [Exercise 16.2: Ingestion Pipeline](exercise_16_2_ingestion.md)

---

[Back to Module 16](../README.md) | [Next: Exercise 16.2 - Ingestion Pipeline](exercise_16_2_ingestion.md)
