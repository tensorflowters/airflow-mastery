# Module 08: Kubernetes Executor

## 🎯 Learning Objectives

By the end of this module, you will:

- Deploy Airflow on Kubernetes using the official Helm chart
- Understand KubernetesExecutor architecture and benefits
- Configure pod templates for task customization
- Manage resources, secrets, and persistent storage
- Debug Kubernetes-specific issues

## ⏱️ Estimated Time: 6-8 hours (hands-on deployment)

---

## 1. Why Kubernetes for Airflow?

### KubernetesExecutor Benefits

| Benefit              | Description                                   |
| -------------------- | --------------------------------------------- |
| **Isolation**        | Each task runs in its own pod                 |
| **Scalability**      | Scale to zero when idle, up to cluster limits |
| **Resource Control** | Per-task CPU/memory limits                    |
| **No Queue Needed**  | No Redis/RabbitMQ dependency                  |
| **Security**         | Tasks don't need database access (Airflow 3)  |

### When to Use What

| Executor             | Best For                                        |
| -------------------- | ----------------------------------------------- |
| `LocalExecutor`      | Development, small workloads                    |
| `CeleryExecutor`     | High-throughput, consistent workloads           |
| `KubernetesExecutor` | Variable workloads, isolation needs, K8s native |

---

## 2. Architecture on Kubernetes

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │   API Server    │  │   Scheduler     │                   │
│  │   (Deployment)  │  │   (Deployment)  │                   │
│  │   replicas: 2   │  │   replicas: 2   │                   │
│  └────────┬────────┘  └────────┬────────┘                   │
│           │                    │                             │
│           │    ┌───────────────┘                            │
│           │    │                                            │
│           ▼    ▼                                            │
│  ┌─────────────────┐                                        │
│  │    PostgreSQL   │  ◄── Metadata database                 │
│  │   (or external) │                                        │
│  └─────────────────┘                                        │
│                                                              │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              Task Pods (Dynamic)                     │    │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐            │    │
│  │  │ Task Pod │ │ Task Pod │ │ Task Pod │ ...        │    │
│  │  │ (my_task)│ │(process) │ │(extract) │            │    │
│  │  └──────────┘ └──────────┘ └──────────┘            │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Helm Chart Deployment

### Prerequisites

```bash
# Verify tools
kubectl version --client
helm version

# Verify cluster access
kubectl cluster-info
kubectl get nodes
```

### Basic Installation

```bash
# Add Apache Airflow Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Create namespace
kubectl create namespace airflow

# Install with defaults (for learning)
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --set executor=KubernetesExecutor

# Watch pods come up
kubectl get pods -n airflow -w
```

### Access the UI

```bash
# Port forward to webserver
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow

# Default credentials: admin / admin
# Open: http://localhost:8080
```

---

## 4. Production values.yaml

```yaml
# infrastructure/helm/values.yaml

# Core settings
executor: KubernetesExecutor
defaultAirflowTag: "3.0.2"
airflowVersion: "3.0.2"

# Use standard naming (recommended)
useStandardNaming: true

# ─────────────────────────────────────────────────────────────
# DATABASE (External PostgreSQL - Required for Production)
# ─────────────────────────────────────────────────────────────
postgresql:
  enabled: false # Disable built-in PostgreSQL

data:
  metadataSecretName: airflow-database-secret

# Connection pooling (highly recommended)
pgbouncer:
  enabled: true
  maxClientConn: 100
  metadataPoolSize: 10

# ─────────────────────────────────────────────────────────────
# SCHEDULER
# ─────────────────────────────────────────────────────────────
scheduler:
  replicas: 2 # HA
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: 2
      memory: 4Gi

# ─────────────────────────────────────────────────────────────
# WEBSERVER / API SERVER
# ─────────────────────────────────────────────────────────────
webserver:
  replicas: 2
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: 1
      memory: 2Gi
  service:
    type: ClusterIP # Use Ingress for external access

# ─────────────────────────────────────────────────────────────
# DAG SYNC (Git-Sync)
# ─────────────────────────────────────────────────────────────
dags:
  persistence:
    enabled: false # Using git-sync instead
  gitSync:
    enabled: true
    repo: "git@github.com:your-org/airflow-dags.git"
    branch: main
    subPath: "dags"
    sshKeySecret: airflow-git-ssh-secret
    wait: 60
    containerName: git-sync

# ─────────────────────────────────────────────────────────────
# KUBERNETES EXECUTOR SETTINGS
# ─────────────────────────────────────────────────────────────
workers:
  persistence:
    enabled: false # KubernetesExecutor doesn't use workers deployment

# Default pod template for tasks
airflowPodAnnotations:
  cluster-autoscaler.kubernetes.io/safe-to-evict: "true"

# ─────────────────────────────────────────────────────────────
# SECRETS
# ─────────────────────────────────────────────────────────────
webserverSecretKeySecretName: airflow-webserver-secret
fernetKeySecretName: airflow-fernet-secret

# ─────────────────────────────────────────────────────────────
# LOGGING (Remote)
# ─────────────────────────────────────────────────────────────
config:
  logging:
    remote_logging: "True"
    remote_base_log_folder: "s3://my-bucket/airflow-logs"
    remote_log_conn_id: "aws_default"

  kubernetes:
    delete_worker_pods: "True"
    delete_worker_pods_on_failure: "False"
    worker_pods_pending_timeout: 300

# ─────────────────────────────────────────────────────────────
# EXTRA ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────────────────────
env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: "False"
```

---

## 5. Pod Templates

Customize task pod behavior:

```yaml
# infrastructure/helm/pod-template.yaml
apiVersion: v1
kind: Pod
metadata:
  name: airflow-worker
  annotations:
    cluster-autoscaler.kubernetes.io/safe-to-evict: "false"
spec:
  serviceAccountName: airflow-worker

  containers:
    - name: base
      image: apache/airflow:3.0.2
      imagePullPolicy: IfNotPresent

      resources:
        requests:
          cpu: "500m"
          memory: "512Mi"
        limits:
          cpu: "2"
          memory: "2Gi"

      env:
        - name: AIRFLOW__CORE__EXECUTOR
          value: "LocalExecutor" # Inside the pod

      volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
          readOnly: true
        - name: logs
          mountPath: /opt/airflow/logs

  volumes:
    - name: dags
      emptyDir: {}
    - name: logs
      emptyDir: {}

  restartPolicy: Never

  # Security context
  securityContext:
    runAsUser: 50000
    fsGroup: 0
```

### Per-Task Pod Customization

```python
from airflow.sdk import DAG, task
from kubernetes.client import models as k8s

# Define custom pod configuration
pod_override = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "2", "memory": "4Gi"}, limits={"cpu": "4", "memory": "8Gi"}
                ),
            )
        ],
        tolerations=[k8s.V1Toleration(key="high-memory", operator="Equal", value="true", effect="NoSchedule")],
        node_selector={"workload": "compute-intensive"},
    )
)

with DAG(...):

    @task(executor_config={"pod_override": pod_override})
    def heavy_computation():
        """Task that needs more resources"""
        pass
```

---

## 6. Secrets Management

### Creating Required Secrets

```bash
# Database connection
kubectl create secret generic airflow-database-secret \
  --from-literal=connection=postgresql://user:pass@host:5432/airflow \
  -n airflow

# Webserver secret key (for session security)
kubectl create secret generic airflow-webserver-secret \
  --from-literal=webserver-secret-key=$(python -c 'import secrets; print(secrets.token_hex(16))') \
  -n airflow

# Fernet key (for connection encryption)
kubectl create secret generic airflow-fernet-secret \
  --from-literal=fernet-key=$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())') \
  -n airflow

# Git SSH key for DAG sync
kubectl create secret generic airflow-git-ssh-secret \
  --from-file=gitSshKey=/path/to/private/key \
  -n airflow
```

### Using Secrets in DAGs

```python
from airflow.models import Connection, Variable
from airflow.sdk import task


@task
def use_secrets():
    # From Airflow Variables (stored in DB)
    api_key = Variable.get("my_api_key")

    # From Airflow Connections
    conn = Connection.get_connection_from_secrets("my_database")

    # From Kubernetes Secrets (as env vars in values.yaml)
    import os

    k8s_secret = os.environ.get("MY_K8S_SECRET")
```

---

## 7. Debugging Kubernetes Deployments

### Common Commands

```bash
# Check pod status
kubectl get pods -n airflow

# Check events (great for scheduling issues)
kubectl get events -n airflow --sort-by='.lastTimestamp'

# Scheduler logs
kubectl logs -n airflow deployment/airflow-scheduler -c scheduler -f

# API server logs
kubectl logs -n airflow deployment/airflow-webserver -c webserver -f

# Task pod logs (get pod name from UI or kubectl)
kubectl logs -n airflow <task-pod-name>

# Describe pod for details
kubectl describe pod -n airflow <pod-name>

# Shell into a pod
kubectl exec -it -n airflow deployment/airflow-scheduler -- bash
```

### Common Issues

| Issue                   | Symptom           | Solution                         |
| ----------------------- | ----------------- | -------------------------------- |
| Tasks stuck in "queued" | Pods not starting | Check scheduler logs, K8s events |
| ImagePullBackOff        | Pod pending       | Check image name, registry auth  |
| CrashLoopBackOff        | Pod restarting    | Check container logs             |
| Database connection     | All pods crashing | Verify secret, connection string |
| DAGs not appearing      | UI empty          | Check git-sync logs, DAG folder  |

### Validating Pod Template

```bash
# Generate pod YAML without running
airflow kubernetes generate-dag-yaml my_dag my_task 2024-01-01

# Review the output
```

---

## 8. Monitoring and Observability

### Enable StatsD

```yaml
# In values.yaml
statsd:
  enabled: true

config:
  metrics:
    statsd_on: "True"
    statsd_host: "localhost"
    statsd_port: 8125
```

### Prometheus Integration

```yaml
# In values.yaml
extraEnv:
  - name: AIRFLOW__METRICS__OTEL_ON
    value: "True"
  - name: AIRFLOW__METRICS__OTEL_HOST
    value: "otel-collector"
```

---

## 📝 Exercises

### Exercise 8.1: Local K8s Deployment

Using minikube or kind:

1. Create a cluster
2. Deploy Airflow with KubernetesExecutor
3. Trigger a sample DAG
4. Watch pods being created for tasks

### Exercise 8.2: Custom Pod Template

Create a DAG with:

1. A task that requires 2GB memory
2. A task that needs GPU (nodeSelector)
3. A task with custom environment variables

### Exercise 8.3: Production Checklist

Prepare a production deployment:

1. Configure external PostgreSQL
2. Set up git-sync for DAGs
3. Configure remote logging (S3/GCS)
4. Enable PgBouncer
5. Document all secrets created

---

## ✅ Checkpoint

Before moving to Module 09, ensure you can:

- [ ] Deploy Airflow on Kubernetes using Helm
- [ ] Explain KubernetesExecutor pod lifecycle
- [ ] Configure values.yaml for production
- [ ] Create and use pod templates
- [ ] Manage secrets in Kubernetes
- [ ] Debug common K8s deployment issues

---

## 🏭 Industry Spotlight: Airbnb

**How Airbnb Scales Search Pipeline with KubernetesExecutor**

Airbnb's search infrastructure processes millions of queries daily, requiring elastic scaling to handle traffic spikes during peak booking seasons. The KubernetesExecutor enables dynamic resource allocation:

| Challenge              | KubernetesExecutor Solution                                        |
| ---------------------- | ------------------------------------------------------------------ |
| **Traffic spikes**     | Auto-scaling pods handle 10x load during peak seasons              |
| **Resource isolation** | Each search pipeline task gets dedicated compute resources         |
| **Cost optimization**  | Spot instances for non-critical indexing tasks                     |
| **Multi-tenancy**      | Namespace isolation between search, recommendations, and analytics |

**Pattern in Use**: Airbnb-style dynamic resource allocation:

```python
from airflow.sdk import DAG, task
from kubernetes.client import models as k8s

# Peak traffic configuration
peak_resources = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "4", "memory": "8Gi"}, limits={"cpu": "8", "memory": "16Gi"}
                ),
            )
        ],
        tolerations=[k8s.V1Toleration(key="workload", value="search-indexing", effect="NoSchedule")],
        node_selector={"node-pool": "high-memory"},
    )
)

with DAG(...):

    @task(executor_config={"pod_override": peak_resources})
    def reindex_search_listings():
        """Scale up during peak booking seasons."""
        return rebuild_search_index(region="global")
```

**Key Insight**: KubernetesExecutor reduced Airbnb's infrastructure costs by 40% through intelligent pod scheduling and spot instance utilization, while maintaining 99.9% search availability.

📖 **Related Exercise**: [Exercise 8.2: Custom Pod Template](exercises/) - Configure resource-specific pod templates

---

## 📚 Further Reading

- [Kubernetes Executor Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/kubernetes.html)
- [Helm Chart Configuration](https://airflow.apache.org/docs/helm-chart/stable/index.html)
- [Case Study: Airbnb Experimentation](../../docs/case-studies/airbnb-experimentation.md)

---

Next: [Module 09: Production Patterns →](../09-production-patterns/README.md)
