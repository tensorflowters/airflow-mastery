# Local Kubernetes Setup for Airflow

This guide covers setting up a local Kubernetes cluster for Airflow development and testing.

## Option 1: minikube (Recommended for Beginners)

### Installation

**macOS**:
```bash
brew install minikube
```

**Windows**:
```bash
choco install minikube
```

**Linux**:
```bash
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

### Create Cluster

```bash
# Start with adequate resources for Airflow
minikube start \
  --cpus=4 \
  --memory=8192 \
  --disk-size=30g \
  --driver=docker

# Verify cluster
minikube status
kubectl cluster-info
kubectl get nodes
```

### Useful minikube Commands

```bash
# Open Kubernetes dashboard
minikube dashboard

# Get cluster IP
minikube ip

# SSH into node
minikube ssh

# Stop cluster
minikube stop

# Delete cluster
minikube delete

# Add more resources
minikube stop
minikube delete
minikube start --cpus=6 --memory=12288
```

### Enable Add-ons

```bash
# Enable ingress for external access
minikube addons enable ingress

# Enable metrics server for resource monitoring
minikube addons enable metrics-server

# Enable storage provisioner
minikube addons enable storage-provisioner

# List all add-ons
minikube addons list
```

---

## Option 2: kind (Kubernetes IN Docker)

### Installation

**macOS**:
```bash
brew install kind
```

**Windows/Linux**:
```bash
go install sigs.k8s.io/kind@latest
# OR
curl -Lo ./kind https://kind.sigs.k8s.io/dl/latest/kind-linux-amd64
chmod +x ./kind
sudo mv ./kind /usr/local/bin/kind
```

### Create Cluster

**Simple cluster**:
```bash
kind create cluster --name airflow-local
```

**Multi-node cluster** (create `kind-config.yaml`):
```yaml
# kind-config.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
    extraPortMappings:
      - containerPort: 30080
        hostPort: 8080
        protocol: TCP
  - role: worker
  - role: worker
```

```bash
kind create cluster --name airflow-cluster --config kind-config.yaml
```

### Useful kind Commands

```bash
# List clusters
kind get clusters

# Get kubeconfig
kind get kubeconfig --name airflow-cluster

# Delete cluster
kind delete cluster --name airflow-cluster

# Load local Docker image into kind
kind load docker-image my-image:tag --name airflow-cluster
```

---

## Deploy Airflow

### Prerequisites

```bash
# Verify kubectl
kubectl version --client

# Verify Helm
helm version

# Add Airflow Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

### Create Namespace

```bash
kubectl create namespace airflow
```

### Deploy with Local Values

Create `local-values.yaml`:
```yaml
# Minimal local development configuration
executor: KubernetesExecutor

defaultAirflowTag: "3.0.2"
airflowVersion: "3.0.2"

# Single replica for local
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
  service:
    type: NodePort  # Use NodePort for local access

# Built-in PostgreSQL
postgresql:
  enabled: true
  resources:
    requests:
      cpu: 250m
      memory: 256Mi

# Disable unused components
redis:
  enabled: false

statsd:
  enabled: false

pgbouncer:
  enabled: false

# Load examples for testing
env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: "True"

# Task pod cleanup
config:
  kubernetes:
    delete_worker_pods: "True"
    delete_worker_pods_on_failure: "False"
    worker_pods_pending_timeout: 300
```

### Install

```bash
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --values local-values.yaml \
  --timeout 10m
```

### Access the UI

**Option 1: Port Forward**
```bash
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
# Access at http://localhost:8080
```

**Option 2: NodePort (minikube)**
```bash
minikube service airflow-webserver -n airflow --url
```

**Option 3: Ingress (if enabled)**
```yaml
# Add to values.yaml
ingress:
  web:
    enabled: true
    hosts:
      - name: airflow.local
```

### Default Credentials

- Username: `admin`
- Password: `admin` (default Helm chart)

Or retrieve from secret:
```bash
kubectl get secret -n airflow airflow-webserver-secret -o jsonpath='{.data.webserver-secret-key}' | base64 --decode
```

---

## Troubleshooting

### Pods Stuck in Pending

```bash
# Check pod events
kubectl describe pod <pod-name> -n airflow

# Check node resources
kubectl describe nodes

# Common cause: insufficient resources
# Solution: Increase minikube resources or reduce Airflow requests
```

### ImagePullBackOff

```bash
# Check image name and tag
kubectl describe pod <pod-name> -n airflow | grep -A5 "Image"

# For minikube, ensure Docker daemon is accessible
eval $(minikube docker-env)
```

### Database Connection Issues

```bash
# Check PostgreSQL pod
kubectl logs -n airflow airflow-postgresql-0

# Test connection from scheduler
kubectl exec -n airflow deployment/airflow-scheduler -- \
  airflow db check
```

### DAGs Not Appearing

```bash
# Check scheduler logs
kubectl logs -n airflow deployment/airflow-scheduler -c scheduler | grep -i dag

# Verify DAG folder is mounted
kubectl exec -n airflow deployment/airflow-scheduler -- ls /opt/airflow/dags
```

---

## Resource Recommendations

| Scenario | CPUs | Memory | Disk |
|----------|------|--------|------|
| Minimal testing | 2 | 4GB | 20GB |
| Development | 4 | 8GB | 30GB |
| Full testing | 6 | 12GB | 50GB |

---

## Clean Up

```bash
# Uninstall Airflow
helm uninstall airflow -n airflow

# Delete namespace
kubectl delete namespace airflow

# Delete cluster
minikube delete
# OR
kind delete cluster --name airflow-cluster
```
