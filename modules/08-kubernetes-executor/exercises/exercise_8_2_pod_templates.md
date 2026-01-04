# Exercise 8.2: Custom Pod Templates

## Objective

Learn to customize Kubernetes task pods using pod templates and executor_config for resource allocation, node selection, and environment configuration.

## Background

KubernetesExecutor allows you to customize how task pods are created:

1. **Global Pod Template**: Default settings for all tasks (via Helm values)
2. **Per-Task Override**: Customize individual tasks with `executor_config`
3. **Kubernetes Python Client**: Programmatic pod specification

### Use Cases for Custom Pods

| Requirement | Solution |
|------------|----------|
| High memory task | Custom resource limits |
| GPU workload | Node selector + tolerations |
| Task-specific secrets | Volume mounts |
| Sidecar containers | Multi-container pod |
| Network policies | Pod annotations |

## Requirements

### Part 1: Create a Global Pod Template

Create a default pod template that all tasks will use:

**File: `pod-template.yaml`**
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: default-airflow-worker
  labels:
    app: airflow-worker
spec:
  serviceAccountName: airflow-worker
  restartPolicy: Never

  # Security best practices
  securityContext:
    runAsUser: 50000
    runAsGroup: 0
    fsGroup: 0

  containers:
    - name: base
      image: apache/airflow:3.0.2
      imagePullPolicy: IfNotPresent

      # Default resources
      resources:
        requests:
          cpu: "250m"
          memory: "256Mi"
        limits:
          cpu: "1"
          memory: "1Gi"

      # Required volume mounts
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
```

### Part 2: Create Per-Task Pod Configurations

Create a DAG with tasks that have different resource requirements:

**File: `custom_pod_templates_dag.py`** (starter in exercises/)

```python
"""
Exercise 8.2: Custom Pod Templates
==================================

Create a DAG demonstrating different pod configurations:
1. Standard task (default resources)
2. High-memory task (4Gi memory)
3. GPU task (node selector)
4. Task with custom environment variables
5. Task with init container
"""

from airflow.sdk import dag, task
from datetime import datetime

# TODO: Import Kubernetes client models
# from kubernetes.client import models as k8s

@dag(
    dag_id="exercise_8_2_custom_pods",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "kubernetes"],
)
def custom_pods_dag():
    """DAG demonstrating custom pod configurations."""

    # Task 1: Standard task with default resources
    @task
    def standard_task():
        """Uses default pod template."""
        import os
        print(f"Running on node: {os.environ.get('HOSTNAME', 'unknown')}")
        return {"status": "completed"}

    # TODO: Task 2: High memory task
    # Create a task that requests 4Gi memory and 2 CPU cores
    # Use executor_config with pod_override

    # TODO: Task 3: GPU task
    # Create a task that:
    # - Uses node selector: {"gpu": "true"}
    # - Has toleration for GPU nodes

    # TODO: Task 4: Task with environment variables
    # Create a task that has custom env vars injected
    # - API_KEY from a Kubernetes secret
    # - ENVIRONMENT = "production"

    # TODO: Task 5: Task with init container
    # Create a task with an init container that downloads data

    # Wire up dependencies
    result = standard_task()
    # TODO: Add other tasks to the dependency chain

custom_pods_dag()
```

### Part 3: Implement Resource-Intensive Pod Configuration

```python
from kubernetes.client import models as k8s

# High-memory pod configuration
high_memory_pod = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={
                        "cpu": "2",
                        "memory": "4Gi"
                    },
                    limits={
                        "cpu": "4",
                        "memory": "8Gi"
                    }
                )
            )
        ]
    )
)

@task(executor_config={"pod_override": high_memory_pod})
def high_memory_task():
    """Task with increased memory allocation."""
    import resource
    # Verify we can use more memory
    mem_limit = resource.getrlimit(resource.RLIMIT_AS)
    print(f"Memory limit: {mem_limit}")
    return {"memory_available": "4Gi"}
```

### Part 4: GPU Node Selection

```python
# GPU-enabled pod configuration
gpu_pod = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    limits={"nvidia.com/gpu": "1"}
                )
            )
        ],
        node_selector={"accelerator": "nvidia-tesla-t4"},
        tolerations=[
            k8s.V1Toleration(
                key="nvidia.com/gpu",
                operator="Equal",
                value="true",
                effect="NoSchedule"
            )
        ]
    )
)

@task(executor_config={"pod_override": gpu_pod})
def gpu_task():
    """Task that runs on GPU nodes."""
    # Check for GPU
    import subprocess
    result = subprocess.run(["nvidia-smi"], capture_output=True, text=True)
    print(result.stdout)
    return {"gpu": "available"}
```

### Part 5: Secret Injection

```python
# Pod with secrets mounted as environment variables
secret_pod = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                env=[
                    k8s.V1EnvVar(
                        name="API_KEY",
                        value_from=k8s.V1EnvVarSource(
                            secret_key_ref=k8s.V1SecretKeySelector(
                                name="api-credentials",
                                key="api-key"
                            )
                        )
                    ),
                    k8s.V1EnvVar(
                        name="ENVIRONMENT",
                        value="production"
                    )
                ]
            )
        ]
    )
)

@task(executor_config={"pod_override": secret_pod})
def secure_api_task():
    """Task with secrets from Kubernetes."""
    import os
    api_key = os.environ.get("API_KEY")
    # Use api_key securely (don't log it!)
    print(f"API key present: {bool(api_key)}")
    return {"authenticated": True}
```

## Deliverables

1. **`pod-template.yaml`** - Global default pod template
2. **`custom_pod_templates_dag.py`** - Complete DAG with 5 different pod configurations
3. **`pod-configs/`** directory with:
   - `high-memory-pod.yaml`
   - `gpu-pod.yaml`
   - `secret-pod.yaml`

## Testing Your Configuration

### Generate Pod YAML (dry-run)
```bash
# Inside scheduler pod or with Airflow CLI
airflow kubernetes generate-dag-yaml \
    exercise_8_2_custom_pods \
    high_memory_task \
    2024-01-01

# Review generated pod spec
```

### Verify Resource Allocation
```bash
# Get running task pod
kubectl get pods -n airflow -l dag_id=exercise_8_2_custom_pods

# Describe pod to see resource allocation
kubectl describe pod <pod-name> -n airflow

# Check actual resource usage
kubectl top pod <pod-name> -n airflow
```

## Hints

<details>
<summary>Hint 1: Importing Kubernetes models</summary>

```python
from kubernetes.client import models as k8s

# Available models for pod configuration:
# - k8s.V1Pod
# - k8s.V1PodSpec
# - k8s.V1Container
# - k8s.V1ResourceRequirements
# - k8s.V1EnvVar
# - k8s.V1EnvVarSource
# - k8s.V1SecretKeySelector
# - k8s.V1Toleration
# - k8s.V1Volume
# - k8s.V1VolumeMount
```

</details>

<details>
<summary>Hint 2: Combining multiple configurations</summary>

```python
# You can specify multiple container settings
combined_pod = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(...),
                env=[k8s.V1EnvVar(...)],
                volume_mounts=[k8s.V1VolumeMount(...)]
            )
        ],
        volumes=[k8s.V1Volume(...)],
        node_selector={...},
        tolerations=[...]
    )
)
```

</details>

<details>
<summary>Hint 3: Init containers</summary>

```python
init_container_pod = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        init_containers=[
            k8s.V1Container(
                name="download-data",
                image="alpine/curl",
                command=["curl", "-o", "/data/file.csv", "https://..."],
                volume_mounts=[
                    k8s.V1VolumeMount(name="shared-data", mount_path="/data")
                ]
            )
        ],
        containers=[
            k8s.V1Container(
                name="base",
                volume_mounts=[
                    k8s.V1VolumeMount(name="shared-data", mount_path="/data")
                ]
            )
        ],
        volumes=[
            k8s.V1Volume(
                name="shared-data",
                empty_dir=k8s.V1EmptyDirVolumeSource()
            )
        ]
    )
)
```

</details>

## Success Criteria

- [ ] Global pod template created and applied
- [ ] High-memory task runs with 4Gi memory limit
- [ ] GPU task correctly targets GPU nodes (or simulated)
- [ ] Secret injection working via environment variables
- [ ] Init container prepares data before main task
- [ ] All pod configurations verified via `kubectl describe`

---

Next: [Exercise 8.3: Production Checklist →](exercise_8_3_production_checklist.md)
