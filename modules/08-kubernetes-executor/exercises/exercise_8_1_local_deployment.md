# Exercise 8.1: Local Kubernetes Deployment

## Objective

Deploy Apache Airflow on a local Kubernetes cluster using minikube or kind, and understand the KubernetesExecutor task lifecycle.

## Background

The KubernetesExecutor runs each task in its own Pod, providing:
- Complete task isolation
- Dynamic resource allocation
- Scale-to-zero capability when idle
- No need for external message queues

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                        │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  Scheduler  │  │  Webserver  │  │  Task Pods (Dynamic) │  │
│  │    Pod      │  │    Pod      │  │  created per task    │  │
│  └──────┬──────┘  └─────────────┘  └─────────────────────┘  │
│         │                                                    │
│         ▼                                                    │
│  ┌─────────────┐                                            │
│  │  PostgreSQL │  ← Metadata Database                       │
│  └─────────────┘                                            │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

- Docker Desktop installed and running
- kubectl CLI installed
- Helm 3.x installed
- At least 8GB RAM available for local cluster

## Requirements

### Part 1: Set Up Local Kubernetes Cluster

Choose ONE of these options:

**Option A: minikube (Recommended for beginners)**
```bash
# Install minikube
brew install minikube  # macOS
# OR: choco install minikube  # Windows
# OR: apt install minikube  # Linux

# Start cluster with adequate resources
minikube start --cpus=4 --memory=8192 --driver=docker

# Verify cluster
kubectl cluster-info
kubectl get nodes
```

**Option B: kind (Kubernetes IN Docker)**
```bash
# Install kind
brew install kind  # macOS
# OR: go install sigs.k8s.io/kind@latest

# Create cluster
kind create cluster --name airflow-cluster

# Verify
kubectl cluster-info --context kind-airflow-cluster
```

### Part 2: Deploy Airflow with Helm

1. **Add the Apache Airflow Helm repository**
```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

2. **Create namespace**
```bash
kubectl create namespace airflow
```

3. **Create a basic values file** (`local-values.yaml`):
```yaml
# Exercise 8.1: Local Development Configuration

# Use KubernetesExecutor
executor: KubernetesExecutor

# Airflow version
defaultAirflowTag: "3.0.2"
airflowVersion: "3.0.2"

# Minimal resources for local development
scheduler:
  replicas: 1
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: 1
      memory: 2Gi

webserver:
  replicas: 1
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: 500m
      memory: 1Gi

# Use built-in PostgreSQL for local dev
postgresql:
  enabled: true
  resources:
    requests:
      cpu: 250m
      memory: 256Mi

# Disable features not needed locally
statsd:
  enabled: false

redis:
  enabled: false

pgbouncer:
  enabled: false

# Enable example DAGs for testing
env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: "True"

# Pod cleanup (useful for debugging)
config:
  kubernetes:
    delete_worker_pods: "True"
    delete_worker_pods_on_failure: "False"
```

4. **Install Airflow**
```bash
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --values local-values.yaml \
  --timeout 10m
```

5. **Watch deployment progress**
```bash
kubectl get pods -n airflow -w
```

### Part 3: Access and Verify

1. **Port forward to the webserver**
```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
```

2. **Access the UI** at http://localhost:8080
   - Username: `admin`
   - Password: Get from secret
   ```bash
   kubectl get secret -n airflow airflow-webserver-secret -o jsonpath='{.data.webserver-secret-key}' | base64 --decode
   # Or for simpler setup, use:
   echo "Password for admin: admin (default Helm chart)"
   ```

3. **Verify scheduler is running**
```bash
kubectl logs -n airflow deployment/airflow-scheduler -c scheduler | tail -20
```

### Part 4: Observe KubernetesExecutor in Action

1. **Trigger a sample DAG** from the UI (e.g., `example_bash_operator`)

2. **Watch task pods being created**
```bash
# In a separate terminal
kubectl get pods -n airflow -w
```

3. **Observe pod lifecycle**:
   - Pod created when task starts
   - Pod runs to completion
   - Pod terminated (status: Succeeded or Failed)
   - Pod deleted (if `delete_worker_pods: "True"`)

4. **Check task pod logs** (while it's running)
```bash
# Get pod name from kubectl get pods
kubectl logs -n airflow <task-pod-name> -f
```

## Deliverables

Create a file `exercise_8_1_answers.md` with:

1. **Screenshots or terminal output showing**:
   - Cluster running (`kubectl get nodes`)
   - All Airflow pods healthy (`kubectl get pods -n airflow`)
   - Task pods being created during DAG execution

2. **Answers to these questions**:
   - How many pods are created when you trigger a DAG with 3 tasks?
   - What happens to task pods after they complete?
   - What namespace are task pods created in?
   - How does the scheduler communicate with the Kubernetes API?

3. **Pod Lifecycle Observations**:
   - How long did task pods take to start?
   - What was the pod status progression?
   - What happens if you trigger the same task again?

## Verification Commands

```bash
# Check all pods
kubectl get pods -n airflow

# Check scheduler health
kubectl exec -n airflow deployment/airflow-scheduler -- \
  airflow jobs check --hostname "" --limit 1

# View Airflow config
kubectl exec -n airflow deployment/airflow-scheduler -- \
  airflow config get-value core executor

# List DAGs
kubectl exec -n airflow deployment/airflow-scheduler -- \
  airflow dags list
```

## Cleanup

```bash
# Delete Airflow installation
helm uninstall airflow -n airflow

# Delete namespace
kubectl delete namespace airflow

# Stop minikube (if using)
minikube stop

# OR delete kind cluster
kind delete cluster --name airflow-cluster
```

## Hints

<details>
<summary>Hint 1: Pods stuck in Pending</summary>

Check cluster resources:
```bash
kubectl describe pod <pod-name> -n airflow
kubectl get events -n airflow --sort-by='.lastTimestamp'
```

If resources are insufficient, increase minikube resources:
```bash
minikube stop
minikube delete
minikube start --cpus=4 --memory=8192
```

</details>

<details>
<summary>Hint 2: ImagePullBackOff</summary>

This usually means the image can't be pulled. For local development:
```bash
# Verify Docker daemon connectivity
docker ps

# For minikube, use Docker daemon inside minikube
eval $(minikube docker-env)
```

</details>

<details>
<summary>Hint 3: Database connection errors</summary>

Check PostgreSQL pod:
```bash
kubectl logs -n airflow airflow-postgresql-0
kubectl describe pod -n airflow airflow-postgresql-0
```

</details>

## Success Criteria

- [ ] Local Kubernetes cluster running
- [ ] All Airflow pods in Running status
- [ ] Webserver accessible at localhost:8080
- [ ] Successfully triggered a DAG
- [ ] Observed task pods being created and terminated
- [ ] Documented findings in exercise_8_1_answers.md

## Additional Challenge

Try deploying with your own DAG:

1. Create a simple DAG file locally
2. Copy it into the scheduler pod:
```bash
kubectl cp my_dag.py airflow/airflow-scheduler-xxx:/opt/airflow/dags/
```
3. Wait for the scheduler to pick it up
4. Trigger it and observe execution

---

Next: [Exercise 8.2: Custom Pod Templates →](exercise_8_2_pod_templates.md)
