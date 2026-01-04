# Apache Airflow 3 self-hosted Kubernetes deployment guide

Airflow 3.0 represents the most significant architectural shift in the project's history, introducing a client-server model where workers communicate via a new Task Execution API rather than directly accessing the metadata database. Released on April 22, 2025, Airflow 3 (current stable: **3.1.5**) brings DAG versioning—the most requested community feature—alongside a completely rewritten React-based UI, event-driven scheduling, and enhanced security isolation. For Kubernetes deployments, the official Helm chart (version 1.18.0) fully supports Airflow 3 with the **KubernetesExecutor** remaining the recommended choice for self-hosted production environments.

## Architectural changes redefine how Airflow components communicate

The fundamental change in Airflow 3 is the **Task Execution Interface (AIP-72)**, which introduces a true client-server architecture. In Airflow 2.x, all components—scheduler, workers, webserver—communicated directly with the metadata database, creating security risks and scaling challenges. Airflow 3 eliminates this by routing all worker communication through a dedicated REST API.

**New core components** you'll encounter include the **API Server** (replacing the webserver via `airflow api-server`), which serves both the REST API and static UI files, and the **DAG Processor**, which must now be started independently with `airflow dag-processor`. The scheduler continues as before but no longer expects workers to have database access.

The **Task SDK** provides a lightweight runtime for task execution, enabling containerized execution with zero-trust principles and scoped token authentication. This SDK is versioned separately (currently Task SDK 1.1.5) and opens the door for multi-language support—a Golang SDK is planned. Tasks must now use the Task SDK or the Python Client (`apache-airflow-client`) for metadata interactions; direct database access via sessions is prohibited.

**DAG Versioning (AIP-65, AIP-66)** ensures DAGs run to completion based on the version at trigger time, even if the DAG file changes mid-execution. DAG Bundles replace the deprecated `--subdir` CLI flag, with `LocalDagBundle` and `GitDagBundle` as primary options. The UI now shows version-aware task structure, code, and logs.

**Assets** (renamed from Datasets) gain significant enhancements including the `@asset` decorator for asset-centric DAG definitions, Asset Watchers for monitoring external systems, and AWS SQS integration for event-driven triggers. The **Edge Executor (AIP-69)**, available via `apache-airflow-providers-edge3`, enables task execution on edge devices, remote data centers, and hybrid multi-cloud environments.

## Breaking changes require careful DAG migration

Several features have been removed entirely in Airflow 3, requiring updates before migration:

| Removed feature | Replacement |
|-----------------|-------------|
| SubDAGs | TaskGroups, Assets, Data-Aware Scheduling |
| SequentialExecutor | LocalExecutor (works with SQLite) |
| CeleryKubernetesExecutor | Multiple Executor Configuration |
| LocalKubernetesExecutor | Multiple Executor Configuration |
| SLAs | Deadline Alerts |
| REST API `/api/v1` | FastAPI-based `/api/v2` |
| DAG/XCom Pickling | Removed entirely |
| `--subdir` CLI flag | DAG Bundles |

Context variables including `execution_date`, `tomorrow_ds`, `yesterday_ds`, `prev_ds`, `next_ds`, and their derivatives are no longer available—use `logical_date` instead. The `catchup_by_default` configuration now defaults to `False`, meaning new DAGs won't backfill automatically.

Import paths have shifted to the new **`airflow.sdk`** namespace. Update imports from `airflow.decorators.dag` to `airflow.sdk.dag`, `airflow.models.DAG` to `airflow.sdk.DAG`, and `airflow.datasets.Dataset` to `airflow.sdk.Asset`. Common operators have moved to `apache-airflow-providers-standard`, requiring explicit installation—`BashOperator` is now at `airflow.providers.standard.operators.bash`.

**Run Ruff with AIR rules** (version 0.13.1+) to identify and auto-fix most issues:
```bash
ruff check dags/ --select AIR301 --show-fixes
ruff check dags/ --select AIR301 --fix --unsafe-fixes
```

## Kubernetes deployment uses KubernetesExecutor for optimal isolation

For self-hosted Kubernetes deployments, the **KubernetesExecutor** provides complete task isolation by creating a dedicated pod for each task instance. This approach scales to zero when idle, requires no Redis or message broker, and allows per-task resource specification—ideal for variable workloads with heterogeneous resource requirements.

The official Helm chart installation is straightforward:
```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow --create-namespace \
  -f values.yaml
```

**Essential values.yaml configuration** for production:
```yaml
executor: KubernetesExecutor
useStandardNaming: true
defaultAirflowTag: "3.0.2"

# External PostgreSQL (mandatory for production)
postgresql:
  enabled: false
data:
  metadataSecretName: airflow-database-secret

# Connection pooling
pgbouncer:
  enabled: true
  maxClientConn: 100
  metadataPoolSize: 10

# High availability
scheduler:
  replicas: 2
  resources:
    requests: { cpu: "500m", memory: "1Gi" }
    limits: { cpu: "2", memory: "4Gi" }

webserver:
  replicas: 2
  resources:
    requests: { cpu: "250m", memory: "512Mi" }

# DAG sync via git-sync
dags:
  gitSync:
    enabled: true
    repo: "git@github.com:your-org/airflow-dags.git"
    branch: main
    sshKeySecret: airflow-ssh-secret
    wait: 60

# Security
webserverSecretKeySecretName: airflow-webserver-secret
```

The **CeleryExecutor** remains viable when you need lower task startup latency (workers are always running) or high-throughput predictable workloads, but requires Redis/RabbitMQ and incurs higher baseline costs. The hybrid executors (`CeleryKubernetesExecutor`, `LocalKubernetesExecutor`) are deprecated—use the new Multiple Executor Configuration feature instead.

## Production storage and database configuration

**DAG deployment** should use git-sync (recommended), which continuously pulls DAGs from a Git repository. Create an SSH key secret and configure known_hosts to prevent MITM attacks. Alternatively, bake DAGs directly into custom Docker images for immutable deployments with explicit versioning.

**Log persistence** requires remote logging for production—configure S3, GCS, or Azure Blob Storage:
```yaml
config:
  logging:
    remote_logging: 'True'
    remote_base_log_folder: 's3://your-bucket/airflow-logs'
    remote_log_conn_id: 'aws_default'
```

**Database setup** mandates PostgreSQL or MySQL—never use the embedded PostgreSQL or SQLite for production. Enable **PgBouncer** to handle connection pooling; Airflow opens many database connections, especially with high DAG counts. Configure the connection via Kubernetes Secret rather than plaintext values:
```bash
kubectl create secret generic airflow-database-secret \
  --from-literal=connection=postgresql://user:pass@host:5432/airflow
```

Sizing recommendations scale with workload: **50 DAGs** typically requires ~5 CPUs and 5GB memory total; **200 DAGs** needs 10 CPUs and 20GB; **500+ DAGs** requires horizontal scaling with dedicated node pools.

## Learning path for experienced Airflow 2.x users

For users with Airflow 2.x experience, prioritize learning these concepts in order:

1. **New import paths** (`airflow.sdk`) and installing `apache-airflow-providers-standard`
2. **Task SDK and execution model**—understanding the API Server architecture
3. **DAG Versioning and Bundles**—major workflow changes
4. **Assets** (formerly Datasets)—terminology and API updates
5. **New React UI navigation**—asset-centric and task-centric views

**Core documentation starting points:**
- [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)—essential migration guide
- [Helm Chart Production Guide](https://airflow.apache.org/docs/helm-chart/stable/production-guide.html)—Kubernetes-specific
- [TaskFlow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)—updated patterns
- [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)

**Sample TaskFlow pattern for Airflow 3:**
```python
from airflow.sdk import DAG, task
from datetime import datetime

@task
def extract():
    return {"data": [1, 2, 3]}

@task
def transform(data):
    return {"sum": sum(data["data"])}

@task
def load(values: dict):
    print(f"Loaded: {values}")

with DAG(dag_id="etl_example", start_date=datetime(2024, 1, 1), schedule=None):
    raw = extract()
    transformed = transform(raw)
    load(transformed)
```

## Common Kubernetes deployment pitfalls to avoid

**Configuration mistakes** frequently include forgetting to install `apache-airflow-providers-standard` (BashOperator won't work), not setting `webserverSecretKeySecretName` (causes session issues across pod restarts), and misconfiguring git-sync repository URLs or subPath settings.

**Database connection errors** manifest as `psycopg2.OperationalError: FATAL: sorry, too many clients already`—enable PgBouncer to resolve. Never use SQLite in production; it cannot handle concurrent writes from multiple scheduler replicas.

**Airflow 3-specific issues** include tasks failing when they attempt direct database access (must use REST API or Task SDK), forgetting to start `airflow dag-processor` separately, and using the old `airflow webserver` command instead of `airflow api-server`.

**Resource contention** appears as pod evictions or scheduling failures. Set appropriate resource requests and limits, use `safeToEvict: false` for workers in KubernetesExecutor deployments, and consider KEDA autoscaling for CeleryExecutor workloads.

Debug with these commands:
```bash
# Check scheduler logs
kubectl logs -n airflow deployment/airflow-scheduler -c scheduler

# Validate KubernetesExecutor pod specs
airflow kubernetes generate-dag-yaml

# Access UI via port-forward
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
```

## Step-by-step deployment checklist

**Prerequisites**: Kubernetes 1.30+, Helm 3.10+, external PostgreSQL database, and a Git repository for DAGs.

**Phase 1—Secrets and namespace**:
```bash
kubectl create namespace airflow

# Generate secrets
python3 -c 'import secrets; print(secrets.token_hex(16))'  # webserver key
python3 -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'  # fernet

# Create database secret
kubectl create secret generic airflow-database-secret \
  --from-literal=connection=postgresql://user:pass@host:5432/airflow \
  -n airflow
```

**Phase 2—Create values.yaml** with configurations for executor, database, git-sync, resources, and security contexts as shown in the production configuration section above.

**Phase 3—Install and verify**:
```bash
helm install airflow apache-airflow/airflow -n airflow -f values.yaml --debug --timeout 10m
kubectl get pods -n airflow
kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow
```

**Phase 4—Post-installation**: Create admin users via `kubectl exec`, configure connections and variables, enable remote logging, and set up monitoring with StatsD/Prometheus.

## Conclusion

Airflow 3's architectural shift to a client-server model fundamentally improves security and scalability, but requires thoughtful migration planning. The key insight for Kubernetes deployments is that the KubernetesExecutor pairs naturally with Airflow 3's isolation model—each task runs in its own pod with scoped API access, eliminating direct database exposure. Prioritize updating imports to `airflow.sdk`, removing deprecated features like SubDAGs, and using the official Helm chart with external PostgreSQL and PgBouncer. The migration tooling—particularly Ruff's AIR rules—automates most code changes, making the upgrade tractable for experienced teams willing to test thoroughly in staging environments.
