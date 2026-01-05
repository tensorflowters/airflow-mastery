# Exercise 16.5: Production Deployment

## Objective

Deploy the document processing pipeline to Kubernetes with production-grade patterns including custom pod templates, auto-scaling, SLA monitoring, and alerting integration.

## Background

### KubernetesExecutor in Production

The KubernetesExecutor spins up a separate pod for each task, enabling:

| Capability         | Benefit                        |
| ------------------ | ------------------------------ |
| Resource Isolation | Tasks cannot impact each other |
| Pod Templates      | Custom resources per task type |
| Node Selection     | GPU tasks on GPU nodes         |
| Auto-Scaling       | Scale workers based on demand  |
| Cost Optimization  | Pay only for active tasks      |

### Pod Template Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         POD TEMPLATE HIERARCHY                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │                    Global Pod Template                            │  │
│  │         (Default resources, security context, volumes)            │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                              ▼                                          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │  CPU-Heavy   │    │   GPU Task   │    │   Standard   │              │
│  │   Template   │    │   Template   │    │   Template   │              │
│  │  (4 CPU/8Gi) │    │ (GPU + tol.) │    │ (1 CPU/1Gi)  │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│         │                   │                   │                       │
│         ▼                   ▼                   ▼                       │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   Document   │    │  Embedding   │    │   Metadata   │              │
│  │   Parsing    │    │  Generation  │    │   Tasks      │              │
│  └──────────────┘    └──────────────┘    └──────────────┘              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Requirements

### Task 1: CPU-Intensive Pod Template

Create a pod template for document parsing tasks that require significant CPU and memory.

**File: `kubernetes/cpu-intensive-pod-template.yaml`**

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: cpu-intensive-worker
  labels:
    app: airflow-worker
    workload-type: cpu-intensive
spec:
  serviceAccountName: airflow-worker
  restartPolicy: Never

  # Security best practices
  securityContext:
    runAsUser: 50000
    runAsGroup: 0
    fsGroup: 0
    runAsNonRoot: true

  # Node selection for compute-optimized nodes
  nodeSelector:
    workload-type: compute

  # Tolerations for dedicated compute nodes
  tolerations:
    - key: "workload-type"
      operator: "Equal"
      value: "compute"
      effect: "NoSchedule"

  containers:
    - name: base
      image: apache/airflow:3.0.2
      imagePullPolicy: IfNotPresent

      resources:
        requests:
          cpu: "2"
          memory: "4Gi"
        limits:
          cpu: "4"
          memory: "8Gi"

      # Environment variables for optimization
      env:
        - name: OMP_NUM_THREADS
          value: "4"
        - name: NUMEXPR_MAX_THREADS
          value: "4"

      volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
          readOnly: true
        - name: logs
          mountPath: /opt/airflow/logs
        - name: tmp-data
          mountPath: /tmp/processing

  volumes:
    - name: dags
      emptyDir: {}
    - name: logs
      emptyDir: {}
    - name: tmp-data
      emptyDir:
        sizeLimit: 10Gi
```

**Apply in DAG:**

```python
from kubernetes.client import models as k8s

cpu_intensive_pod = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "2", "memory": "4Gi"}, limits={"cpu": "4", "memory": "8Gi"}
                ),
                env=[
                    k8s.V1EnvVar(name="OMP_NUM_THREADS", value="4"),
                ],
            )
        ],
        node_selector={"workload-type": "compute"},
        tolerations=[k8s.V1Toleration(key="workload-type", operator="Equal", value="compute", effect="NoSchedule")],
    )
)


@task(executor_config={"pod_override": cpu_intensive_pod})
def parse_large_document(document_path: str) -> dict:
    """Parse document with CPU-intensive processing."""
    # TODO: Implement document parsing
    pass
```

### Task 2: GPU Pod Template with NVIDIA Tolerations

Create a pod template for embedding generation tasks that require GPU acceleration.

**File: `kubernetes/gpu-pod-template.yaml`**

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: gpu-worker
  labels:
    app: airflow-worker
    workload-type: gpu
spec:
  serviceAccountName: airflow-worker
  restartPolicy: Never

  securityContext:
    runAsUser: 50000
    runAsGroup: 0
    fsGroup: 0

  # GPU node selection
  nodeSelector:
    accelerator: nvidia-tesla-t4

  # NVIDIA GPU tolerations
  tolerations:
    - key: "nvidia.com/gpu"
      operator: "Exists"
      effect: "NoSchedule"
    - key: "gpu"
      operator: "Equal"
      value: "true"
      effect: "NoSchedule"

  containers:
    - name: base
      image: apache/airflow:3.0.2-cuda
      imagePullPolicy: IfNotPresent

      resources:
        requests:
          cpu: "2"
          memory: "8Gi"
          nvidia.com/gpu: "1"
        limits:
          cpu: "4"
          memory: "16Gi"
          nvidia.com/gpu: "1"

      env:
        - name: CUDA_VISIBLE_DEVICES
          value: "0"
        - name: NVIDIA_VISIBLE_DEVICES
          value: "all"
        - name: NVIDIA_DRIVER_CAPABILITIES
          value: "compute,utility"

      volumeMounts:
        - name: dags
          mountPath: /opt/airflow/dags
          readOnly: true
        - name: logs
          mountPath: /opt/airflow/logs
        - name: model-cache
          mountPath: /root/.cache/huggingface

  volumes:
    - name: dags
      emptyDir: {}
    - name: logs
      emptyDir: {}
    - name: model-cache
      persistentVolumeClaim:
        claimName: model-cache-pvc
```

**Apply in DAG:**

```python
gpu_pod = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "2", "memory": "8Gi", "nvidia.com/gpu": "1"},
                    limits={"cpu": "4", "memory": "16Gi", "nvidia.com/gpu": "1"},
                ),
                env=[
                    k8s.V1EnvVar(name="CUDA_VISIBLE_DEVICES", value="0"),
                    k8s.V1EnvVar(name="NVIDIA_VISIBLE_DEVICES", value="all"),
                ],
            )
        ],
        node_selector={"accelerator": "nvidia-tesla-t4"},
        tolerations=[
            k8s.V1Toleration(key="nvidia.com/gpu", operator="Exists", effect="NoSchedule"),
            k8s.V1Toleration(key="gpu", operator="Equal", value="true", effect="NoSchedule"),
        ],
    )
)


@task(executor_config={"pod_override": gpu_pod}, pool="gpu_pool", pool_slots=1)
def generate_embeddings_gpu(chunks: list[str]) -> list[list[float]]:
    """Generate embeddings using GPU acceleration."""
    # TODO: Implement GPU-accelerated embedding generation
    pass
```

### Task 3: Horizontal Pod Autoscaler Configuration

Configure HPA for worker scaling based on queue depth.

**File: `kubernetes/hpa-worker.yaml`**

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: airflow-worker-hpa
  namespace: airflow
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: airflow-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
    # Scale based on CPU utilization
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70

    # Scale based on memory utilization
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80

    # Scale based on custom queue depth metric
    - type: External
      external:
        metric:
          name: airflow_executor_queued_tasks
          selector:
            matchLabels:
              dag_id: "capstone_*"
        target:
          type: AverageValue
          averageValue: "5"

  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
        - type: Percent
          value: 25
          periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 0
      policies:
        - type: Percent
          value: 100
          periodSeconds: 15
        - type: Pods
          value: 4
          periodSeconds: 15
      selectPolicy: Max
```

**ServiceMonitor for Prometheus Metrics:**

```yaml
# kubernetes/servicemonitor-airflow.yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: airflow-metrics
  namespace: airflow
spec:
  selector:
    matchLabels:
      app: airflow-statsd
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
```

**Prometheus Adapter Configuration:**

```yaml
# kubernetes/prometheus-adapter-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-adapter-config
  namespace: monitoring
data:
  config.yaml: |
    rules:
      - seriesQuery: 'airflow_executor_queued_tasks{namespace="airflow"}'
        resources:
          overrides:
            namespace:
              resource: namespace
        name:
          matches: "^(.*)"
          as: "airflow_executor_queued_tasks"
        metricsQuery: 'sum(<<.Series>>{<<.LabelMatchers>>})'
```

### Task 4: SLA Monitoring with Callbacks

Implement SLA monitoring with callback functions for alerting.

**File: `dags/capstone_sla_config.py`**

```python
"""
SLA Configuration for Capstone Pipeline
========================================

Implements SLA monitoring with callbacks for alerting.
"""

import json
from typing import Any

import requests
from airflow.models import TaskInstance


def sla_miss_callback(
    dag: Any, task_list: str, blocking_task_list: str, slas: list, blocking_tis: list[TaskInstance]
) -> None:
    """
    Callback when SLA is missed.

    Args:
        dag: The DAG object
        task_list: Comma-separated list of tasks that missed SLA
        blocking_task_list: Tasks blocking completion
        slas: List of SLA misses
        blocking_tis: TaskInstances blocking completion
    """
    # Format alert message
    alert_message = {
        "dag_id": dag.dag_id,
        "task_list": task_list,
        "blocking_tasks": blocking_task_list,
        "sla_details": [
            {
                "task_id": sla.task_id,
                "execution_date": str(sla.execution_date),
                "expected_end": str(sla.timestamp),
            }
            for sla in slas
        ],
        "severity": "warning",
        "message": f"SLA missed for tasks: {task_list}",
    }

    # Send to monitoring system
    _send_alert(alert_message)

    # Log for debugging
    print(f"SLA MISS: {json.dumps(alert_message, indent=2)}")


def task_failure_callback(context: dict) -> None:
    """
    Callback when a task fails.

    Args:
        context: Airflow task context dictionary
    """
    ti = context["task_instance"]
    exception = context.get("exception")

    alert_message = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "try_number": ti.try_number,
        "max_tries": ti.max_tries,
        "state": str(ti.state),
        "exception": str(exception) if exception else None,
        "log_url": ti.log_url,
        "severity": "critical" if ti.try_number >= ti.max_tries else "warning",
        "message": f"Task {ti.task_id} failed (attempt {ti.try_number}/{ti.max_tries})",
    }

    _send_alert(alert_message)


def task_retry_callback(context: dict) -> None:
    """Callback when a task retries."""
    ti = context["task_instance"]

    alert_message = {
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": ti.run_id,
        "try_number": ti.try_number,
        "max_tries": ti.max_tries,
        "severity": "info",
        "message": f"Task {ti.task_id} retrying (attempt {ti.try_number}/{ti.max_tries})",
    }

    _send_alert(alert_message)


def _send_alert(message: dict) -> None:
    """Send alert to monitoring systems."""
    # TODO: Configure your alerting endpoints

    # Option 1: Slack Webhook
    slack_webhook = Variable.get("slack_webhook_url", default_var=None)
    if slack_webhook:
        _send_slack_alert(slack_webhook, message)

    # Option 2: PagerDuty
    pagerduty_key = Variable.get("pagerduty_service_key", default_var=None)
    if pagerduty_key and message.get("severity") == "critical":
        _send_pagerduty_alert(pagerduty_key, message)

    # Option 3: Email (via SMTP connection)
    # Uses Airflow's email backend
    if message.get("severity") == "critical":
        _send_email_alert(message)


def _send_slack_alert(webhook_url: str, message: dict) -> None:
    """Send alert to Slack."""
    severity_emoji = {"critical": ":red_circle:", "warning": ":warning:", "info": ":information_source:"}

    slack_message = {
        "attachments": [
            {
                "color": "#ff0000" if message["severity"] == "critical" else "#ffcc00",
                "title": f"{severity_emoji.get(message['severity'], '')} Airflow Alert",
                "text": message["message"],
                "fields": [
                    {"title": "DAG", "value": message["dag_id"], "short": True},
                    {"title": "Task", "value": message.get("task_id", "N/A"), "short": True},
                ],
                "footer": "Airflow Monitoring",
            }
        ]
    }

    try:
        response = requests.post(webhook_url, json=slack_message, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to send Slack alert: {e}")


def _send_pagerduty_alert(service_key: str, message: dict) -> None:
    """Send alert to PagerDuty."""
    payload = {
        "service_key": service_key,
        "event_type": "trigger",
        "description": message["message"],
        "details": message,
        "client": "Airflow",
        "client_url": message.get("log_url", ""),
    }

    try:
        response = requests.post(
            "https://events.pagerduty.com/generic/2010-04-15/create_event.json", json=payload, timeout=10
        )
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to send PagerDuty alert: {e}")


def _send_email_alert(message: dict) -> None:
    """Send alert via email."""
    from airflow.utils.email import send_email

    subject = f"[{message['severity'].upper()}] Airflow Alert: {message['dag_id']}"
    html_content = f"""
    <h2>Airflow Alert</h2>
    <p><strong>Severity:</strong> {message["severity"]}</p>
    <p><strong>Message:</strong> {message["message"]}</p>
    <p><strong>DAG:</strong> {message["dag_id"]}</p>
    <p><strong>Task:</strong> {message.get("task_id", "N/A")}</p>
    <p><a href="{message.get("log_url", "")}">View Logs</a></p>
    """

    try:
        send_email(
            to=Variable.get("alert_email_recipients", default_var="team@example.com"),
            subject=subject,
            html_content=html_content,
        )
    except Exception as e:
        print(f"Failed to send email alert: {e}")
```

**Apply SLA Configuration in DAG:**

```python
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from capstone_sla_config import (
    sla_miss_callback,
    task_failure_callback,
    task_retry_callback,
)


@dag(
    dag_id="capstone_processing_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "on_failure_callback": task_failure_callback,
        "on_retry_callback": task_retry_callback,
        "sla": timedelta(hours=1),  # Default SLA for all tasks
    },
    sla_miss_callback=sla_miss_callback,
    tags=["capstone", "production"],
)
def processing_pipeline():
    @task(sla=timedelta(minutes=15))
    def extract_documents():
        """Must complete within 15 minutes."""
        pass

    @task(sla=timedelta(minutes=30))
    def process_with_llm():
        """Must complete within 30 minutes."""
        pass

    @task(sla=timedelta(minutes=10))
    def store_results():
        """Must complete within 10 minutes."""
        pass

    docs = extract_documents()
    processed = process_with_llm()
    store_results()

    docs >> processed >> store_results


processing_pipeline()
```

### Task 5: Alerting Integration Patterns

Create a comprehensive alerting module with multiple channel support.

**File: `plugins/alerting/__init__.py`**

```python
"""
Alerting Plugin for Capstone Pipeline
=====================================

Provides multi-channel alerting for production monitoring.
"""

from .channels import EmailChannel, PagerDutyChannel, SlackChannel
from .manager import AlertManager

__all__ = ["AlertManager", "EmailChannel", "PagerDutyChannel", "SlackChannel"]
```

**File: `plugins/alerting/manager.py`**

```python
"""Alert manager for coordinating multi-channel notifications."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Protocol


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Alert data structure."""

    title: str
    message: str
    severity: Severity
    source: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    context: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "title": self.title,
            "message": self.message,
            "severity": self.severity.value,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "context": self.context,
        }


class AlertChannel(Protocol):
    """Protocol for alert channels."""

    def send(self, alert: Alert) -> bool:
        """Send alert through this channel."""
        ...

    def supports_severity(self, severity: Severity) -> bool:
        """Check if channel handles this severity."""
        ...


class AlertManager:
    """Manages alert distribution across channels."""

    def __init__(self):
        self.channels: list[AlertChannel] = []
        self._dedup_cache: dict[str, datetime] = {}
        self._dedup_window_seconds = 300  # 5 minutes

    def register_channel(self, channel: AlertChannel) -> None:
        """Register an alert channel."""
        self.channels.append(channel)

    def send_alert(
        self,
        title: str,
        message: str,
        severity: Severity,
        source: str,
        context: dict | None = None,
        deduplicate: bool = True,
    ) -> int:
        """
        Send alert to all applicable channels.

        Returns:
            Number of channels that received the alert
        """
        alert = Alert(title=title, message=message, severity=severity, source=source, context=context or {})

        # Deduplication check
        if deduplicate:
            cache_key = f"{source}:{title}:{severity.value}"
            if self._is_duplicate(cache_key):
                return 0
            self._dedup_cache[cache_key] = datetime.utcnow()

        # Send to all applicable channels
        sent_count = 0
        for channel in self.channels:
            if channel.supports_severity(severity):
                try:
                    if channel.send(alert):
                        sent_count += 1
                except Exception as e:
                    print(f"Failed to send alert via {channel.__class__.__name__}: {e}")

        return sent_count

    def _is_duplicate(self, cache_key: str) -> bool:
        """Check if alert was sent recently."""
        if cache_key not in self._dedup_cache:
            return False

        last_sent = self._dedup_cache[cache_key]
        elapsed = (datetime.utcnow() - last_sent).total_seconds()
        return elapsed < self._dedup_window_seconds
```

## Deliverables

1. **`kubernetes/cpu-intensive-pod-template.yaml`** - Pod template for CPU-heavy tasks
2. **`kubernetes/gpu-pod-template.yaml`** - Pod template for GPU tasks
3. **`kubernetes/hpa-worker.yaml`** - HPA configuration for auto-scaling
4. **`dags/capstone_sla_config.py`** - SLA callbacks and alert functions
5. **`plugins/alerting/`** - Complete alerting plugin module

## Testing Your Configuration

### Verify Pod Templates

```bash
# Generate pod YAML for a task
airflow kubernetes generate-dag-yaml \
    capstone_processing_pipeline \
    parse_large_document \
    2024-01-01

# Apply and test pod template
kubectl apply -f kubernetes/cpu-intensive-pod-template.yaml --dry-run=client

# Check node selector matching
kubectl get nodes -l workload-type=compute
kubectl get nodes -l accelerator=nvidia-tesla-t4
```

### Test HPA Configuration

```bash
# Apply HPA
kubectl apply -f kubernetes/hpa-worker.yaml

# Verify HPA is active
kubectl get hpa -n airflow

# Watch scaling behavior
kubectl get hpa airflow-worker-hpa -n airflow --watch

# Generate load to test scaling
airflow dags trigger capstone_processing_pipeline
```

### Verify SLA Monitoring

```bash
# Check SLA misses in metadata database
airflow db shell
SELECT * FROM sla_miss ORDER BY timestamp DESC LIMIT 10;

# Trigger test alert
python -c "
from capstone_sla_config import _send_alert
_send_alert({'severity': 'warning', 'message': 'Test alert', 'dag_id': 'test'})
"
```

## Hints

<details>
<summary>Hint 1: Debugging GPU scheduling</summary>

```bash
# Check GPU node availability
kubectl describe nodes | grep -A5 "nvidia.com/gpu"

# Verify GPU resource allocation
kubectl get pods -n airflow -o wide | grep gpu

# Check pod events for scheduling issues
kubectl describe pod <pod-name> -n airflow | grep -A10 "Events:"
```

</details>

<details>
<summary>Hint 2: HPA metrics troubleshooting</summary>

```bash
# Check if metrics are available
kubectl get --raw "/apis/custom.metrics.k8s.io/v1beta1" | jq .

# Verify external metrics
kubectl get --raw "/apis/external.metrics.k8s.io/v1beta1" | jq .

# Debug HPA status
kubectl describe hpa airflow-worker-hpa -n airflow
```

</details>

<details>
<summary>Hint 3: Testing callbacks locally</summary>

```python
# Mock context for local testing
mock_context = {
    "task_instance": type(
        "TI",
        (),
        {
            "dag_id": "test_dag",
            "task_id": "test_task",
            "run_id": "test_run",
            "try_number": 1,
            "max_tries": 3,
            "state": "failed",
            "log_url": "http://localhost/logs",
        },
    )(),
    "exception": ValueError("Test error"),
}

task_failure_callback(mock_context)
```

</details>

## Success Criteria

- [ ] CPU-intensive pod template allocates 4 CPU / 8Gi memory
- [ ] GPU pod template includes nvidia.com/gpu resource request
- [ ] GPU pod template has correct NVIDIA tolerations
- [ ] HPA scales workers based on queue depth metric
- [ ] HPA has proper scale-down stabilization (300s)
- [ ] SLA miss callback sends alerts to configured channels
- [ ] Task failure callback includes exception details
- [ ] Alerting supports Slack, email, and PagerDuty channels
- [ ] Alert deduplication prevents notification storms

## References

- [Module 08: Kubernetes Executor](../../08-kubernetes-executor/README.md)
- [Module 09: Production Patterns](../../09-production-patterns/README.md)
- [Module 10: Advanced Topics](../../10-advanced-topics/README.md)
- [Kubernetes HPA Documentation](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/)
- [Airflow SLA Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html#slas)

---

[Previous: Exercise 16.4 - Vector Store](exercise_16_4_vector_store.md) | [Next: Exercise 16.6 - Monitoring](exercise_16_6_monitoring.md)
