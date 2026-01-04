# References & Learning Resources

This document contains all references, official documentation, and community resources used to create this learning curriculum. Organized by topic for easy navigation.

---

## Modern Python Tooling (Module 00)

### uv - Fast Python Package Manager

- **Official Documentation**: https://docs.astral.sh/uv/
- **Installation Guide**: https://docs.astral.sh/uv/getting-started/installation/
- **Project Management**: https://docs.astral.sh/uv/concepts/projects/
- **Docker Integration**: https://docs.astral.sh/uv/guides/integration/docker/
- **GitHub Repository**: https://github.com/astral-sh/uv
- **Docker Examples**: https://github.com/astral-sh/uv-docker-example

Key concepts:

- 10-100x faster than pip for package installation
- Replaces pip, pip-tools, virtualenv, and pyenv
- Built in Rust for maximum performance
- Drop-in replacement for existing workflows

### Ruff - Python Linter & Formatter

- **Official Documentation**: https://docs.astral.sh/ruff/
- **Configuration Guide**: https://docs.astral.sh/ruff/configuration/
- **Rules Reference**: https://docs.astral.sh/ruff/rules/
- **Airflow Rules (AIR)**: https://docs.astral.sh/ruff/rules/#airflow-air
- **GitHub Repository**: https://github.com/astral-sh/ruff

Airflow-specific rules:

- `AIR001`: Task variable name mismatch
- `AIR301`: Airflow 3.0 removal (import changes)
- `AIR302`: Moved-to-provider deprecation fixes

### Pre-commit Hooks

- **Official Documentation**: https://pre-commit.com/
- **Hook Repository**: https://github.com/pre-commit/pre-commit-hooks
- **Ruff Pre-commit**: https://github.com/astral-sh/ruff-pre-commit
- **Configuration Guide**: https://pre-commit.com/#adding-pre-commit-plugins-to-your-project

### pyproject.toml Standards

- **PEP 518**: https://peps.python.org/pep-0518/ - Build system specification
- **PEP 621**: https://peps.python.org/pep-0621/ - Project metadata
- **PEP 735**: https://peps.python.org/pep-0735/ - Dependency groups
- **Packaging Guide**: https://packaging.python.org/en/latest/guides/writing-pyproject-toml/

### Docker Best Practices

- **Multi-stage Builds**: https://docs.docker.com/build/building/multi-stage/
- **Python Docker Images**: https://hub.docker.com/_/python
- **uv Docker Integration**: https://docs.astral.sh/uv/guides/integration/docker/
- **BuildKit Cache Mounts**: https://docs.docker.com/build/cache/

---

## Official Apache Airflow Documentation

### Core Documentation

- **Airflow 3.0 Release Notes**: https://airflow.apache.org/docs/apache-airflow/3.0.0/release_notes.html
- **Airflow 3.1.5 Release Notes (Stable)**: https://airflow.apache.org/docs/apache-airflow/stable/release_notes.html
- **Upgrading to Airflow 3**: https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html
- **TaskFlow Tutorial**: https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
- **Dynamic Task Mapping**: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
- **Concepts Overview**: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html
- **Best Practices**: https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html

### Helm Chart Documentation

- **Helm Chart for Apache Airflow**: https://airflow.apache.org/docs/helm-chart/stable/index.html
- **Production Guide**: https://airflow.apache.org/docs/helm-chart/stable/production-guide.html
- **Manage DAG Files**: https://airflow.apache.org/docs/helm-chart/stable/manage-dag-files.html
- **Adding Connections and Variables**: https://airflow.apache.org/docs/helm-chart/stable/adding-connections-and-variables.html
- **Helm Chart Parameters Reference**: https://airflow.apache.org/docs/helm-chart/stable/parameters-ref.html

### API & SDK Documentation

- **REST API Reference**: https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html
- **Task SDK Documentation**: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/task-sdk.html
- **Python Client**: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/python-client.html

### Sensors & Operators

- **Sensors**: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html
- **Deferrable Operators**: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html
- **Custom Triggers**: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/triggers.html

### Connections & Secrets

- **Managing Connections**: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
- **Secrets Backend**: https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html
- **Variables**: https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html
- **Environment Variables Secrets Backend**: https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/local-filesystem-secrets-backend.html

### Resource Management

- **Pools**: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html
- **Priority Weights**: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/priority-weight.html
- **Concurrency**: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/concurrency.html

---

## Official Blog Posts & Announcements

- **Apache Airflow 3 is Generally Available!**: https://airflow.apache.org/blog/airflow-three-point-oh-is-here/
    - Primary source for Airflow 3 feature announcements
    - Task Execution Interface (AIP-72) details
    - DAG Versioning explanation

---

## Airflow Improvement Proposals (AIPs)

Understanding the design decisions behind Airflow 3:

- **AIP-65: DAG Versioning** - Core versioning architecture
- **AIP-66: DAG Bundles** - DAG deployment and bundling
- **AIP-69: Edge Executor** - Hybrid and edge deployments
- **AIP-72: Task Execution Interface** - Client-server architecture

Full AIP list: https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals

---

## Migration Tools & Automation

- **Airflow 2 to 3 Auto Migration Rules (GitHub Issue #41641)**: https://github.com/apache/airflow/issues/41641
    - Ruff linter rules for automated migration
    - AIR301+ rule set for code fixes

- **Ruff Linter**: https://docs.astral.sh/ruff/
    - AIR rule set for Airflow-specific linting
    - Automated code fixes for deprecations

---

## Community Articles & Tutorials

### Kubernetes Deployment Guides

- **NextLytics: Apache Airflow 3.0 - Everything You Need to Know**: https://www.nextlytics.com/blog/apache-airflow-3.0-everything-you-need-to-know-about-the-new-release
    - Comprehensive feature overview
    - Migration considerations

- **Microsoft Learn: Deploy Apache Airflow on AKS with Helm**: https://learn.microsoft.com/en-us/azure/aks/airflow-overview
    - Azure-specific but architecture patterns applicable to any K8s

- **Medium: Deploying Apache Airflow on Kubernetes with Helm**: https://medium.com/@jdegbun/deploying-apache-airflow-on-kubernetes-with-helm-and-minikube-syncing-dags-from-github-bce4730d7881
    - Practical walkthrough with git-sync

### Technical Deep Dives

- **An Outing with Airflow on Kubernetes**: https://varunbpatil.github.io/2020/10/01/airflow-on-kubernetes.html
    - KubernetesExecutor internals
    - Pod template configuration

---

## GitHub Repositories

### Official

- **Apache Airflow**: https://github.com/apache/airflow
- **Airflow Helm Chart**: https://github.com/apache/airflow/tree/main/chart
- **Airflow Provider Packages**: https://github.com/apache/airflow/tree/main/providers

### Community

- **Helm Charts (Legacy)**: https://github.com/helm/charts/tree/master/stable/airflow
    - Historical reference (now deprecated in favor of official chart)

---

## Video Resources

### Official Airflow YouTube

- Apache Airflow YouTube Channel: https://www.youtube.com/@ApacheAirflow
- Airflow Summit recordings: Various deep-dive sessions

### Recommended Playlists

- Astronomer Academy (free courses): https://academy.astronomer.io/
- Data Engineering Podcast episodes on Airflow

---

## Books

- **Data Pipelines with Apache Airflow** by Bas Harenslak & Julian de Ruiter (Manning)
    - Note: Written for Airflow 2.x, concepts still applicable but imports/APIs differ

- **Apache Airflow Best Practices** (O'Reilly)
    - Production patterns and anti-patterns

---

## Tools & Integrations

### Development Tools

- **Ruff**: https://docs.astral.sh/ruff/ - Fast Python linter with Airflow rules
- **pytest-airflow**: DAG testing utilities
- **pre-commit hooks**: https://pre-commit.com/ - Code quality automation

### Monitoring & Observability

- **StatsD**: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html
- **Prometheus/Grafana**: Community dashboards available
- **OpenTelemetry**: Tracing support in Airflow 3

### Infrastructure

- **PgBouncer**: https://www.pgbouncer.org/ - Connection pooling (critical for production)
- **KEDA**: https://keda.sh/ - Event-driven autoscaling for CeleryExecutor

---

## AI/ML Orchestration Resources

### LLM & RAG Pipelines

- **LangChain Airflow Integration**: https://python.langchain.com/docs/integrations/
- **LlamaIndex with Airflow**: https://docs.llamaindex.ai/en/stable/examples/
- **OpenAI API Best Practices**: https://platform.openai.com/docs/guides/production-best-practices
- **Anthropic API Rate Limits**: https://docs.anthropic.com/en/api/rate-limits

### Vector Databases

- **Pinecone Airflow Integration**: https://docs.pinecone.io/
- **Weaviate Python Client**: https://weaviate.io/developers/weaviate/client-libraries/python
- **Chroma Documentation**: https://docs.trychroma.com/
- **Qdrant Airflow Operator**: https://qdrant.tech/documentation/

### Embedding & Text Processing

- **OpenAI Embeddings Guide**: https://platform.openai.com/docs/guides/embeddings
- **Sentence Transformers**: https://www.sbert.net/
- **tiktoken for Token Counting**: https://github.com/openai/tiktoken

### ML Pipeline Patterns

- **MLflow Integration**: https://mlflow.org/docs/latest/
- **Weights & Biases Airflow**: https://docs.wandb.ai/guides/integrations/
- **Feature Store Patterns**: https://feast.dev/

### Case Studies (Internal)

- [Spotify Recommendations](case-studies/spotify-recommendations.md) - Dynamic mapping for ML pipelines
- [Stripe Fraud Detection](case-studies/stripe-fraud-detection.md) - Real-time ML scoring with retry patterns
- [Airbnb Experimentation](case-studies/airbnb-experimentation.md) - A/B testing at scale
- [Modern RAG Architecture](case-studies/modern-rag-architecture.md) - Production RAG pipeline patterns

---

## Community & Support

### Official Channels

- **Slack**: https://apache-airflow-slack.herokuapp.com/
    - #troubleshooting for help
    - #announcements for updates
    - #kubernetes for K8s-specific discussions

- **GitHub Discussions**: https://github.com/apache/airflow/discussions
- **Stack Overflow**: Tag `apache-airflow`

### Mailing Lists

- dev@airflow.apache.org - Development discussions
- users@airflow.apache.org - User questions

---

## Cheat Sheets & Quick References

### CLI Commands (Airflow 3)

```bash
# Core commands
airflow dags list                    # List all DAGs
airflow dags test <dag_id> <date>    # Test a DAG
airflow tasks test <dag_id> <task>   # Test a task
airflow dags trigger <dag_id>        # Trigger a DAG run

# New in Airflow 3
airflow api-server                   # Start API server (replaces webserver)
airflow dag-processor                # Start DAG processor (now separate)
airflow kubernetes generate-dag-yaml # Generate K8s pod specs

# Database
airflow db migrate                   # Apply migrations
airflow db check                     # Check DB connection

# Debug
airflow config list                  # Show configuration
airflow info                         # Show system info
```

### Key Configuration Variables

```ini
[core]
executor = KubernetesExecutor
dags_folder = /opt/airflow/dags
load_examples = False

[kubernetes]
namespace = airflow
worker_container_repository = apache/airflow
worker_container_tag = 3.0.2
delete_worker_pods = True
delete_worker_pods_on_failure = False

[logging]
remote_logging = True
remote_base_log_folder = s3://bucket/logs

[api]
auth_backends = airflow.providers.fab.auth_manager.api.backend.basic_auth
```

---

## Version Compatibility Matrix

| Component          | Minimum Version | Recommended |
| ------------------ | --------------- | ----------- |
| Python             | 3.9             | 3.11+       |
| Kubernetes         | 1.26            | 1.30+       |
| Helm               | 3.10            | 3.14+       |
| PostgreSQL         | 12              | 15+         |
| Airflow Helm Chart | 1.18.0          | Latest      |

---

## Glossary

| Term                         | Definition                                                    |
| ---------------------------- | ------------------------------------------------------------- |
| **Asset**                    | Formerly Dataset; represents a data dependency for scheduling |
| **DAG Bundle**               | Packaging mechanism for DAG files (replaces --subdir)         |
| **Task SDK**                 | Lightweight runtime for task execution in Airflow 3           |
| **API Server**               | New component replacing webserver; serves UI and REST API     |
| **DAG Processor**            | Component that parses DAG files; now runs separately          |
| **Task Execution Interface** | REST API for worker-to-scheduler communication                |
| **Edge Executor**            | Execute tasks on remote/edge devices                          |
| **Logical Date**             | Replaces execution_date; represents the scheduled time        |
| **Data Interval**            | Time range a DAG run covers (start, end)                      |

---

_Last updated: January 2025_
_Airflow version: 3.1.5_
_Helm chart version: 1.18.0_
