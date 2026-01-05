# Exercise 16.6: Monitoring and Observability

## Objective

Build a comprehensive monitoring and observability solution for the document processing pipeline, including metrics collection, quality tracking, REST API integration, and alerting.

## Background

### Observability Patterns for ML Pipelines

Effective ML pipeline monitoring goes beyond traditional infrastructure metrics:

| Monitoring Layer   | Focus Area              | Examples                               |
| ------------------ | ----------------------- | -------------------------------------- |
| **Infrastructure** | System health           | CPU, memory, disk, network             |
| **Application**    | Airflow health          | Scheduler heartbeat, task queue depth  |
| **Pipeline**       | Workflow health         | DAG duration, task success rate        |
| **Data Quality**   | Input/Output validation | Schema drift, null rates               |
| **Model Quality**  | ML-specific metrics     | Embedding quality, extraction accuracy |
| **Business**       | Outcomes                | Documents processed, cost per document |

### Observability Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       OBSERVABILITY STACK                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐                    ┌─────────────────┐             │
│  │   Airflow       │───StatsD Metrics──▶│   Prometheus    │             │
│  │   (Source)      │                    │   (Collection)  │             │
│  └─────────────────┘                    └────────┬────────┘             │
│                                                  │                       │
│  ┌─────────────────┐                    ┌────────▼────────┐             │
│  │   Monitoring    │◀───Query Metrics───│    Grafana      │             │
│  │   DAG           │                    │   (Dashboards)  │             │
│  └─────────────────┘                    └─────────────────┘             │
│          │                                                              │
│          ▼                                                              │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐     │
│  │   REST API      │    │   Alert         │    │   Status        │     │
│  │   Integration   │    │   Manager       │    │   Dashboard     │     │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Requirements

### Task 1: Create Monitoring DAG

Build a monitoring DAG that runs every 5 minutes to collect system health metrics.

**File: `dags/capstone_monitoring_dag.py`**

```python
"""
Capstone Monitoring DAG
=======================

Collects and reports metrics for the document processing pipeline.
Runs every 5 minutes to ensure timely alerting.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.models import DagRun, TaskInstance
from airflow.sdk import dag, task
from airflow.utils.state import DagRunState, TaskInstanceState


@dag(
    dag_id="capstone_monitoring",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "platform-sre",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["capstone", "monitoring", "sre"],
    doc_md="""
    ## Monitoring DAG

    Collects metrics every 5 minutes:
    - Pipeline processing metrics
    - Task success/failure rates
    - Resource utilization
    - Quality metrics

    **Alerts**: Sends notifications on threshold breaches.
    """,
)
def monitoring_dag():
    """Main monitoring DAG for capstone pipeline."""

    @task
    def collect_pipeline_metrics() -> dict[str, Any]:
        """
        Collect pipeline execution metrics from metadata database.

        Returns:
            Dictionary with pipeline health metrics
        """
        from airflow.utils.db import provide_session

        @provide_session
        def get_metrics(session=None):
            # Time windows
            now = datetime.utcnow()
            last_hour = now - timedelta(hours=1)
            last_day = now - timedelta(days=1)

            # Query DAG run statistics
            dag_ids = ["capstone_ingestion_pipeline", "capstone_processing_pipeline", "capstone_vector_store_pipeline"]

            metrics = {"timestamp": now.isoformat(), "dags": {}}

            for dag_id in dag_ids:
                # Last hour statistics
                runs_last_hour = (
                    session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.execution_date >= last_hour).all()
                )

                success_count = sum(1 for r in runs_last_hour if r.state == DagRunState.SUCCESS)
                failed_count = sum(1 for r in runs_last_hour if r.state == DagRunState.FAILED)
                running_count = sum(1 for r in runs_last_hour if r.state == DagRunState.RUNNING)

                # Calculate average duration for successful runs
                successful_runs = [r for r in runs_last_hour if r.state == DagRunState.SUCCESS]
                if successful_runs:
                    durations = [
                        (r.end_date - r.start_date).total_seconds()
                        for r in successful_runs
                        if r.end_date and r.start_date
                    ]
                    avg_duration = sum(durations) / len(durations) if durations else 0
                else:
                    avg_duration = 0

                metrics["dags"][dag_id] = {
                    "runs_last_hour": len(runs_last_hour),
                    "success_count": success_count,
                    "failed_count": failed_count,
                    "running_count": running_count,
                    "success_rate": success_count / len(runs_last_hour) if runs_last_hour else 1.0,
                    "avg_duration_seconds": avg_duration,
                }

            return metrics

        return get_metrics()

    @task
    def collect_task_metrics() -> dict[str, Any]:
        """
        Collect task-level metrics for detailed analysis.

        Returns:
            Dictionary with task execution statistics
        """
        from airflow.utils.db import provide_session

        @provide_session
        def get_task_metrics(session=None):
            now = datetime.utcnow()
            last_hour = now - timedelta(hours=1)

            # Query task instances
            task_instances = (
                session.query(TaskInstance)
                .filter(TaskInstance.dag_id.like("capstone_%"), TaskInstance.start_date >= last_hour)
                .all()
            )

            # Group by task_id
            task_stats = {}
            for ti in task_instances:
                key = f"{ti.dag_id}.{ti.task_id}"
                if key not in task_stats:
                    task_stats[key] = {"total": 0, "success": 0, "failed": 0, "retries": 0, "durations": []}

                task_stats[key]["total"] += 1
                if ti.state == TaskInstanceState.SUCCESS:
                    task_stats[key]["success"] += 1
                    if ti.end_date and ti.start_date:
                        duration = (ti.end_date - ti.start_date).total_seconds()
                        task_stats[key]["durations"].append(duration)
                elif ti.state == TaskInstanceState.FAILED:
                    task_stats[key]["failed"] += 1
                if ti.try_number > 1:
                    task_stats[key]["retries"] += ti.try_number - 1

            # Calculate averages
            for key in task_stats:
                durations = task_stats[key]["durations"]
                task_stats[key]["avg_duration"] = sum(durations) / len(durations) if durations else 0
                task_stats[key]["max_duration"] = max(durations) if durations else 0
                del task_stats[key]["durations"]  # Remove raw data

            return {"timestamp": now.isoformat(), "task_stats": task_stats}

        return get_task_metrics()

    @task
    def collect_resource_metrics() -> dict[str, Any]:
        """
        Collect resource utilization metrics.

        Returns:
            Dictionary with pool and resource statistics
        """
        from airflow.models import Pool
        from airflow.utils.db import provide_session

        @provide_session
        def get_resource_metrics(session=None):
            now = datetime.utcnow()

            # Get pool statistics
            pools = session.query(Pool).all()
            pool_stats = {}

            for pool in pools:
                pool_stats[pool.pool] = {
                    "total_slots": pool.slots,
                    "open_slots": pool.open_slots(),
                    "used_slots": pool.occupied_slots(),
                    "queued_slots": pool.queued_slots(),
                    "utilization": (pool.occupied_slots() / pool.slots if pool.slots > 0 else 0),
                }

            return {"timestamp": now.isoformat(), "pools": pool_stats}

        return get_resource_metrics()

    @task
    def check_thresholds(pipeline_metrics: dict, task_metrics: dict, resource_metrics: dict) -> dict[str, Any]:
        """
        Check metrics against defined thresholds and generate alerts.

        Args:
            pipeline_metrics: DAG-level metrics
            task_metrics: Task-level metrics
            resource_metrics: Pool and resource metrics

        Returns:
            Dictionary with threshold violations
        """
        violations = []
        warnings = []

        # Define thresholds
        THRESHOLDS = {
            "success_rate_critical": 0.90,
            "success_rate_warning": 0.95,
            "avg_duration_increase_pct": 50,  # 50% increase
            "pool_utilization_warning": 0.80,
            "pool_utilization_critical": 0.95,
            "retry_rate_warning": 0.10,
        }

        # Check pipeline success rates
        for dag_id, metrics in pipeline_metrics.get("dags", {}).items():
            success_rate = metrics.get("success_rate", 1.0)
            if success_rate < THRESHOLDS["success_rate_critical"]:
                violations.append(
                    {
                        "type": "success_rate_critical",
                        "dag_id": dag_id,
                        "value": success_rate,
                        "threshold": THRESHOLDS["success_rate_critical"],
                        "severity": "critical",
                    }
                )
            elif success_rate < THRESHOLDS["success_rate_warning"]:
                warnings.append(
                    {
                        "type": "success_rate_warning",
                        "dag_id": dag_id,
                        "value": success_rate,
                        "threshold": THRESHOLDS["success_rate_warning"],
                        "severity": "warning",
                    }
                )

        # Check pool utilization
        for pool_name, metrics in resource_metrics.get("pools", {}).items():
            utilization = metrics.get("utilization", 0)
            if utilization > THRESHOLDS["pool_utilization_critical"]:
                violations.append(
                    {
                        "type": "pool_utilization_critical",
                        "pool": pool_name,
                        "value": utilization,
                        "threshold": THRESHOLDS["pool_utilization_critical"],
                        "severity": "critical",
                    }
                )
            elif utilization > THRESHOLDS["pool_utilization_warning"]:
                warnings.append(
                    {
                        "type": "pool_utilization_warning",
                        "pool": pool_name,
                        "value": utilization,
                        "threshold": THRESHOLDS["pool_utilization_warning"],
                        "severity": "warning",
                    }
                )

        # Check retry rates
        for task_key, metrics in task_metrics.get("task_stats", {}).items():
            total = metrics.get("total", 0)
            retries = metrics.get("retries", 0)
            if total > 0:
                retry_rate = retries / total
                if retry_rate > THRESHOLDS["retry_rate_warning"]:
                    warnings.append(
                        {
                            "type": "high_retry_rate",
                            "task": task_key,
                            "value": retry_rate,
                            "threshold": THRESHOLDS["retry_rate_warning"],
                            "severity": "warning",
                        }
                    )

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "violations": violations,
            "warnings": warnings,
            "healthy": len(violations) == 0,
        }

    @task
    def send_alerts(threshold_results: dict) -> None:
        """
        Send alerts for threshold violations.

        Args:
            threshold_results: Results from threshold checking
        """
        import requests
        from airflow.models import Variable

        if threshold_results.get("healthy", True):
            print("All metrics within thresholds - no alerts needed")
            return

        violations = threshold_results.get("violations", [])
        warnings = threshold_results.get("warnings", [])

        # Get Slack webhook
        slack_webhook = Variable.get("slack_webhook_url", default_var=None)

        if slack_webhook and violations:
            alert_message = {
                "text": ":red_circle: Capstone Pipeline Alert",
                "attachments": [
                    {
                        "color": "#ff0000",
                        "title": "Critical Violations Detected",
                        "fields": [
                            {
                                "title": v["type"],
                                "value": f"Value: {v['value']:.2%}, Threshold: {v['threshold']:.2%}",
                                "short": True,
                            }
                            for v in violations
                        ],
                    }
                ],
            }

            try:
                requests.post(slack_webhook, json=alert_message, timeout=10)
            except Exception as e:
                print(f"Failed to send Slack alert: {e}")

        # Log all issues
        for v in violations:
            print(f"CRITICAL: {v['type']} - {v}")
        for w in warnings:
            print(f"WARNING: {w['type']} - {w}")

    # Task dependencies
    pipeline = collect_pipeline_metrics()
    tasks = collect_task_metrics()
    resources = collect_resource_metrics()
    thresholds = check_thresholds(pipeline, tasks, resources)
    send_alerts(thresholds)


monitoring_dag()
```

### Task 2: Metrics Collection with Custom Tracking

Implement detailed metrics collection for ML-specific quality indicators.

**File: `dags/capstone_metrics_collector.py`**

```python
"""
ML Pipeline Metrics Collector
=============================

Collects ML-specific quality metrics:
- Processing time per document type
- Embedding generation costs
- Extraction accuracy tracking
"""

from datetime import datetime
from typing import Any

from airflow.models import Variable
from airflow.sdk import dag, task


@dag(
    dag_id="capstone_metrics_collector",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["capstone", "monitoring", "metrics"],
)
def metrics_collector():
    """Collect and aggregate ML pipeline metrics."""

    @task
    def collect_processing_metrics() -> dict[str, Any]:
        """
        Collect document processing metrics.

        Tracks:
        - Documents processed per hour
        - Average processing time by document type
        - Error rates by stage
        """
        # In production, query from metadata store or XCom
        # This example uses Variables for demonstration

        metrics_key = "capstone_processing_metrics"
        current_metrics = Variable.get(metrics_key, default_var={}, deserialize_json=True)

        now = datetime.utcnow()
        hour_key = now.strftime("%Y-%m-%d-%H")

        # Initialize hour bucket if needed
        if hour_key not in current_metrics:
            current_metrics[hour_key] = {
                "documents_processed": 0,
                "total_processing_time_ms": 0,
                "by_type": {},
                "errors": {"extraction": 0, "embedding": 0, "storage": 0},
            }

        return {"timestamp": now.isoformat(), "hour_key": hour_key, "metrics": current_metrics.get(hour_key, {})}

    @task
    def collect_cost_metrics() -> dict[str, Any]:
        """
        Collect cost-related metrics.

        Tracks:
        - API token usage
        - Estimated costs
        - Cost per document
        """
        cost_key = "capstone_cost_metrics"
        current_costs = Variable.get(cost_key, default_var={}, deserialize_json=True)

        now = datetime.utcnow()
        day_key = now.strftime("%Y-%m-%d")

        # Cost rates (per 1K tokens)
        COST_RATES = {
            "embedding_input": 0.0001,  # $0.0001 per 1K tokens
            "llm_input": 0.003,  # $0.003 per 1K tokens
            "llm_output": 0.015,  # $0.015 per 1K tokens
        }

        day_costs = current_costs.get(
            day_key, {"embedding_tokens": 0, "llm_input_tokens": 0, "llm_output_tokens": 0, "documents_processed": 0}
        )

        # Calculate costs
        embedding_cost = (day_costs["embedding_tokens"] / 1000) * COST_RATES["embedding_input"]
        llm_input_cost = (day_costs["llm_input_tokens"] / 1000) * COST_RATES["llm_input"]
        llm_output_cost = (day_costs["llm_output_tokens"] / 1000) * COST_RATES["llm_output"]
        total_cost = embedding_cost + llm_input_cost + llm_output_cost

        docs = day_costs["documents_processed"]
        cost_per_doc = total_cost / docs if docs > 0 else 0

        return {
            "timestamp": now.isoformat(),
            "day_key": day_key,
            "costs": {
                "embedding_cost_usd": embedding_cost,
                "llm_input_cost_usd": llm_input_cost,
                "llm_output_cost_usd": llm_output_cost,
                "total_cost_usd": total_cost,
                "cost_per_document_usd": cost_per_doc,
                "documents_processed": docs,
            },
            "tokens": {
                "embedding": day_costs["embedding_tokens"],
                "llm_input": day_costs["llm_input_tokens"],
                "llm_output": day_costs["llm_output_tokens"],
            },
        }

    @task
    def collect_quality_metrics() -> dict[str, Any]:
        """
        Collect quality metrics for ML outputs.

        Tracks:
        - Embedding similarity scores
        - Extraction confidence scores
        - Classification accuracy
        """
        quality_key = "capstone_quality_metrics"
        quality_data = Variable.get(quality_key, default_var={}, deserialize_json=True)

        now = datetime.utcnow()

        # Calculate aggregate quality scores
        embedding_scores = quality_data.get("embedding_similarity_scores", [])
        extraction_scores = quality_data.get("extraction_confidence_scores", [])

        return {
            "timestamp": now.isoformat(),
            "embedding_quality": {
                "avg_similarity": (sum(embedding_scores) / len(embedding_scores) if embedding_scores else 0),
                "min_similarity": min(embedding_scores) if embedding_scores else 0,
                "samples": len(embedding_scores),
            },
            "extraction_quality": {
                "avg_confidence": (sum(extraction_scores) / len(extraction_scores) if extraction_scores else 0),
                "low_confidence_count": sum(1 for s in extraction_scores if s < 0.7),
                "samples": len(extraction_scores),
            },
        }

    @task
    def aggregate_and_store(processing: dict, costs: dict, quality: dict) -> dict[str, Any]:
        """
        Aggregate all metrics and store for dashboard consumption.
        """
        aggregated = {
            "timestamp": datetime.utcnow().isoformat(),
            "processing": processing,
            "costs": costs,
            "quality": quality,
            "summary": {
                "health_score": _calculate_health_score(processing, costs, quality),
                "alerts": _check_metric_alerts(processing, costs, quality),
            },
        }

        # Store latest metrics
        Variable.set("capstone_latest_metrics", aggregated, serialize_json=True)

        return aggregated


def _calculate_health_score(processing: dict, costs: dict, quality: dict) -> float:
    """Calculate overall pipeline health score (0-100)."""
    score = 100.0

    # Deduct for errors
    errors = processing.get("metrics", {}).get("errors", {})
    total_errors = sum(errors.values())
    score -= min(total_errors * 5, 30)  # Max 30 point deduction

    # Deduct for low quality
    avg_confidence = quality.get("extraction_quality", {}).get("avg_confidence", 1.0)
    if avg_confidence < 0.8:
        score -= (0.8 - avg_confidence) * 50

    # Deduct for high costs
    cost_per_doc = costs.get("costs", {}).get("cost_per_document_usd", 0)
    if cost_per_doc > 0.10:  # $0.10 threshold
        score -= min((cost_per_doc - 0.10) * 100, 20)

    return max(0, score)


def _check_metric_alerts(processing: dict, costs: dict, quality: dict) -> list[str]:
    """Check for metric-based alerts."""
    alerts = []

    # Check error rates
    errors = processing.get("metrics", {}).get("errors", {})
    if errors.get("extraction", 0) > 10:
        alerts.append("High extraction error rate")
    if errors.get("embedding", 0) > 10:
        alerts.append("High embedding error rate")

    # Check quality thresholds
    avg_confidence = quality.get("extraction_quality", {}).get("avg_confidence", 1.0)
    if avg_confidence < 0.7:
        alerts.append("Low extraction confidence")

    # Check cost thresholds
    total_cost = costs.get("costs", {}).get("total_cost_usd", 0)
    if total_cost > 100:  # Daily budget threshold
        alerts.append("Daily cost budget exceeded")

    return alerts


metrics_collector()
```

### Task 3: REST API Integration for Status Dashboard

Build REST API integration to provide pipeline status to external dashboards.

**File: `dags/capstone_api_reporter.py`**

```python
"""
REST API Reporter
=================

Pushes pipeline status to external dashboard API.
"""

from datetime import datetime, timedelta
from typing import Any

import requests
from airflow.models import Variable
from airflow.sdk import dag, task


@dag(
    dag_id="capstone_api_reporter",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["capstone", "monitoring", "api"],
)
def api_reporter():
    """Report pipeline status to external dashboard API."""

    @task
    def prepare_status_payload() -> dict[str, Any]:
        """
        Prepare status payload for API.

        Returns:
            Dictionary with pipeline status
        """
        # Get latest metrics
        latest_metrics = Variable.get("capstone_latest_metrics", default_var={}, deserialize_json=True)

        # Get recent DAG runs
        from airflow.models import DagRun
        from airflow.utils.db import provide_session

        @provide_session
        def get_recent_runs(session=None):
            runs = (
                session.query(DagRun)
                .filter(
                    DagRun.dag_id.like("capstone_%"), DagRun.execution_date >= datetime.utcnow() - timedelta(hours=1)
                )
                .order_by(DagRun.execution_date.desc())
                .limit(10)
                .all()
            )

            return [
                {
                    "dag_id": r.dag_id,
                    "run_id": r.run_id,
                    "state": str(r.state),
                    "execution_date": r.execution_date.isoformat(),
                    "start_date": r.start_date.isoformat() if r.start_date else None,
                    "end_date": r.end_date.isoformat() if r.end_date else None,
                }
                for r in runs
            ]

        recent_runs = get_recent_runs()

        # Build status payload
        payload = {
            "pipeline_id": "capstone-document-processor",
            "timestamp": datetime.utcnow().isoformat(),
            "status": {
                "overall": _determine_overall_status(latest_metrics),
                "health_score": latest_metrics.get("summary", {}).get("health_score", 0),
                "alerts": latest_metrics.get("summary", {}).get("alerts", []),
            },
            "metrics": {
                "processing": latest_metrics.get("processing", {}),
                "costs": latest_metrics.get("costs", {}),
                "quality": latest_metrics.get("quality", {}),
            },
            "recent_runs": recent_runs,
        }

        return payload

    @task
    def push_to_dashboard_api(payload: dict) -> dict[str, Any]:
        """
        Push status to external dashboard API.

        Args:
            payload: Status payload to send

        Returns:
            API response details
        """
        # Get API configuration
        api_endpoint = Variable.get(
            "dashboard_api_endpoint", default_var="http://localhost:8080/api/v1/pipeline-status"
        )
        api_key = Variable.get("dashboard_api_key", default_var=None)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}

        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        try:
            response = requests.post(api_endpoint, json=payload, headers=headers, timeout=30)
            response.raise_for_status()

            return {"success": True, "status_code": response.status_code, "timestamp": datetime.utcnow().isoformat()}

        except requests.exceptions.RequestException as e:
            print(f"Failed to push to dashboard API: {e}")
            return {"success": False, "error": str(e), "timestamp": datetime.utcnow().isoformat()}

    @task
    def push_metrics_to_prometheus(payload: dict) -> dict[str, Any]:
        """
        Push metrics to Prometheus Pushgateway.

        Args:
            payload: Metrics payload

        Returns:
            Push result
        """
        pushgateway_url = Variable.get("prometheus_pushgateway_url", default_var="http://localhost:9091")

        # Format metrics for Prometheus
        metrics_lines = []

        # Health score
        health_score = payload.get("status", {}).get("health_score", 0)
        metrics_lines.append(f'capstone_health_score{{pipeline="document-processor"}} {health_score}')

        # Cost metrics
        costs = payload.get("metrics", {}).get("costs", {}).get("costs", {})
        if costs:
            metrics_lines.append(f'capstone_cost_usd{{type="total"}} {costs.get("total_cost_usd", 0)}')
            metrics_lines.append(f"capstone_cost_per_doc_usd {costs.get('cost_per_document_usd', 0)}")

        # Quality metrics
        quality = payload.get("metrics", {}).get("quality", {})
        if quality.get("extraction_quality"):
            metrics_lines.append(
                f"capstone_extraction_confidence {quality['extraction_quality'].get('avg_confidence', 0)}"
            )

        # Push to Pushgateway
        metrics_body = "\n".join(metrics_lines) + "\n"

        try:
            response = requests.post(
                f"{pushgateway_url}/metrics/job/capstone_pipeline",
                data=metrics_body,
                headers={"Content-Type": "text/plain"},
                timeout=10,
            )
            response.raise_for_status()
            return {"success": True, "metrics_pushed": len(metrics_lines)}

        except requests.exceptions.RequestException as e:
            print(f"Failed to push to Prometheus: {e}")
            return {"success": False, "error": str(e)}


def _determine_overall_status(metrics: dict) -> str:
    """Determine overall pipeline status."""
    health_score = metrics.get("summary", {}).get("health_score", 100)
    alerts = metrics.get("summary", {}).get("alerts", [])

    if health_score < 50 or len(alerts) > 3:
        return "critical"
    elif health_score < 80 or len(alerts) > 0:
        return "warning"
    else:
        return "healthy"


api_reporter()
```

### Task 4: SLA Miss Callbacks with Notification

Implement comprehensive SLA miss handling with multi-channel notifications.

**File: `dags/capstone_sla_handler.py`**

```python
"""
SLA Handler for Capstone Pipeline
=================================

Handles SLA misses with escalation and notification.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow.models import Variable


class SLAHandler:
    """Handles SLA misses with escalation logic."""

    # SLA definitions per DAG/task
    SLA_DEFINITIONS = {
        "capstone_ingestion_pipeline": {
            "default": timedelta(hours=1),
            "tasks": {
                "scan_sources": timedelta(minutes=5),
                "download_documents": timedelta(minutes=30),
            },
        },
        "capstone_processing_pipeline": {
            "default": timedelta(hours=2),
            "tasks": {
                "extract_text": timedelta(minutes=15),
                "generate_summary": timedelta(minutes=30),
                "classify_document": timedelta(minutes=10),
            },
        },
        "capstone_vector_store_pipeline": {
            "default": timedelta(minutes=30),
            "tasks": {
                "generate_embeddings": timedelta(minutes=20),
                "upsert_vectors": timedelta(minutes=10),
            },
        },
    }

    # Escalation levels
    ESCALATION_LEVELS = {
        1: {"delay": timedelta(minutes=0), "channels": ["slack"]},
        2: {"delay": timedelta(minutes=15), "channels": ["slack", "email"]},
        3: {"delay": timedelta(minutes=30), "channels": ["slack", "email", "pagerduty"]},
    }

    @classmethod
    def handle_sla_miss(cls, dag, task_list: str, blocking_task_list: str, slas: list, blocking_tis: list) -> None:
        """
        Main SLA miss callback handler.

        Args:
            dag: The DAG object
            task_list: Comma-separated list of tasks that missed SLA
            blocking_task_list: Tasks blocking completion
            slas: List of SLA miss objects
            blocking_tis: TaskInstances blocking completion
        """
        now = datetime.utcnow()

        for sla in slas:
            # Calculate how late
            expected_end = sla.timestamp
            lateness = now - expected_end

            # Determine escalation level
            level = cls._determine_escalation_level(lateness)

            # Build notification
            notification = cls._build_notification(
                dag_id=dag.dag_id,
                task_id=sla.task_id,
                execution_date=sla.execution_date,
                expected_end=expected_end,
                lateness=lateness,
                level=level,
                blocking_tasks=blocking_task_list,
            )

            # Send to appropriate channels
            escalation = cls.ESCALATION_LEVELS[level]
            for channel in escalation["channels"]:
                cls._send_notification(channel, notification)

            # Log for tracking
            cls._log_sla_miss(notification)

    @classmethod
    def _determine_escalation_level(cls, lateness: timedelta) -> int:
        """Determine escalation level based on how late."""
        if lateness > timedelta(hours=1):
            return 3
        elif lateness > timedelta(minutes=30):
            return 2
        else:
            return 1

    @classmethod
    def _build_notification(
        cls,
        dag_id: str,
        task_id: str,
        execution_date: datetime,
        expected_end: datetime,
        lateness: timedelta,
        level: int,
        blocking_tasks: str,
    ) -> dict[str, Any]:
        """Build notification payload."""
        severity_map = {1: "warning", 2: "high", 3: "critical"}

        return {
            "type": "sla_miss",
            "severity": severity_map[level],
            "escalation_level": level,
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": execution_date.isoformat(),
            "expected_end": expected_end.isoformat(),
            "lateness_seconds": lateness.total_seconds(),
            "lateness_human": str(lateness),
            "blocking_tasks": blocking_tasks,
            "timestamp": datetime.utcnow().isoformat(),
            "message": (f"SLA missed for {dag_id}.{task_id} (late by {lateness})"),
        }

    @classmethod
    def _send_notification(cls, channel: str, notification: dict) -> None:
        """Send notification to specified channel."""
        import requests

        if channel == "slack":
            webhook = Variable.get("slack_webhook_url", default_var=None)
            if webhook:
                severity_colors = {"warning": "#ffcc00", "high": "#ff6600", "critical": "#ff0000"}
                color = severity_colors.get(notification["severity"], "#cccccc")

                slack_payload = {
                    "attachments": [
                        {
                            "color": color,
                            "title": f":clock1: SLA Miss - Level {notification['escalation_level']}",
                            "text": notification["message"],
                            "fields": [
                                {"title": "DAG", "value": notification["dag_id"], "short": True},
                                {"title": "Task", "value": notification["task_id"], "short": True},
                                {"title": "Lateness", "value": notification["lateness_human"], "short": True},
                                {"title": "Blocking", "value": notification["blocking_tasks"] or "None", "short": True},
                            ],
                            "footer": f"Escalation Level: {notification['escalation_level']}",
                        }
                    ]
                }

                try:
                    requests.post(webhook, json=slack_payload, timeout=10)
                except Exception as e:
                    print(f"Failed to send Slack notification: {e}")

        elif channel == "email":
            from airflow.utils.email import send_email

            recipients = Variable.get("sla_email_recipients", default_var="team@example.com")

            subject = f"[SLA MISS] {notification['dag_id']}.{notification['task_id']}"
            html_content = f"""
            <h2>SLA Miss Alert</h2>
            <p><strong>Severity:</strong> {notification["severity"]}</p>
            <p><strong>DAG:</strong> {notification["dag_id"]}</p>
            <p><strong>Task:</strong> {notification["task_id"]}</p>
            <p><strong>Lateness:</strong> {notification["lateness_human"]}</p>
            <p><strong>Blocking Tasks:</strong> {notification["blocking_tasks"] or "None"}</p>
            """

            try:
                send_email(to=recipients, subject=subject, html_content=html_content)
            except Exception as e:
                print(f"Failed to send email notification: {e}")

        elif channel == "pagerduty":
            service_key = Variable.get("pagerduty_service_key", default_var=None)
            if service_key:
                payload = {
                    "service_key": service_key,
                    "event_type": "trigger",
                    "description": notification["message"],
                    "incident_key": f"sla-{notification['dag_id']}-{notification['task_id']}",
                    "details": notification,
                }

                try:
                    requests.post(
                        "https://events.pagerduty.com/generic/2010-04-15/create_event.json", json=payload, timeout=10
                    )
                except Exception as e:
                    print(f"Failed to send PagerDuty notification: {e}")

    @classmethod
    def _log_sla_miss(cls, notification: dict) -> None:
        """Log SLA miss for historical tracking."""
        # Store in Variable for simple tracking
        # In production, use a proper metrics store
        history_key = "sla_miss_history"
        history = Variable.get(history_key, default_var=[], deserialize_json=True)

        # Keep last 100 entries
        history.append(notification)
        history = history[-100:]

        Variable.set(history_key, history, serialize_json=True)
```

### Task 5: Quality Metrics Tracking

Implement tracking for ML-specific quality metrics.

**File: `dags/capstone_quality_tracker.py`**

```python
"""
Quality Metrics Tracker
=======================

Tracks embedding quality, extraction accuracy, and data quality.
"""

from datetime import datetime
from typing import Any

import numpy as np
from airflow.models import Variable
from airflow.sdk import dag, task


@dag(
    dag_id="capstone_quality_tracker",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *",  # Every hour
    catchup=False,
    tags=["capstone", "monitoring", "quality"],
)
def quality_tracker():
    """Track and analyze quality metrics."""

    @task
    def analyze_embedding_quality() -> dict[str, Any]:
        """
        Analyze embedding quality metrics.

        Checks:
        - Embedding dimensionality consistency
        - Embedding norm distribution
        - Semantic coherence (sample similarity check)
        """
        # In production, query from vector store
        # This example uses simulated data

        # Get recent embeddings metadata
        embedding_log = Variable.get("embedding_generation_log", default_var=[], deserialize_json=True)

        if not embedding_log:
            return {"status": "no_data", "timestamp": datetime.utcnow().isoformat()}

        # Calculate quality metrics
        dims = [e.get("dimensions", 0) for e in embedding_log[-100:]]
        norms = [e.get("norm", 0) for e in embedding_log[-100:]]

        # Check dimensionality consistency
        dim_consistent = len(set(dims)) == 1

        # Check norm distribution (should be ~1.0 for normalized embeddings)
        if norms:
            avg_norm = sum(norms) / len(norms)
            norm_std = np.std(norms) if len(norms) > 1 else 0
            norm_healthy = 0.9 <= avg_norm <= 1.1 and norm_std < 0.1
        else:
            avg_norm = 0
            norm_std = 0
            norm_healthy = False

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "samples_analyzed": len(embedding_log[-100:]),
            "dimensionality": {
                "consistent": dim_consistent,
                "expected": 1536,  # OpenAI ada-002
                "found": list(set(dims)),
            },
            "norms": {"average": avg_norm, "std": norm_std, "healthy": norm_healthy},
            "quality_score": 100 if (dim_consistent and norm_healthy) else 50,
        }

    @task
    def analyze_extraction_quality() -> dict[str, Any]:
        """
        Analyze document extraction quality.

        Checks:
        - Extraction success rate
        - Character encoding issues
        - Empty extraction rate
        - Average text length
        """
        extraction_log = Variable.get("extraction_log", default_var=[], deserialize_json=True)

        if not extraction_log:
            return {"status": "no_data", "timestamp": datetime.utcnow().isoformat()}

        recent = extraction_log[-100:]

        # Calculate metrics
        success_count = sum(1 for e in recent if e.get("status") == "success")
        empty_count = sum(1 for e in recent if e.get("text_length", 0) == 0)
        encoding_errors = sum(1 for e in recent if e.get("encoding_error", False))
        text_lengths = [e.get("text_length", 0) for e in recent if e.get("text_length", 0) > 0]

        success_rate = success_count / len(recent) if recent else 0
        empty_rate = empty_count / len(recent) if recent else 0
        avg_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0

        # Quality score
        quality_score = 100
        quality_score -= (1 - success_rate) * 40  # Up to 40 points for failures
        quality_score -= empty_rate * 30  # Up to 30 points for empty extractions
        quality_score -= min(encoding_errors / len(recent) * 30, 30)  # Up to 30 for encoding

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "samples_analyzed": len(recent),
            "success_rate": success_rate,
            "empty_rate": empty_rate,
            "encoding_error_count": encoding_errors,
            "avg_text_length": avg_length,
            "quality_score": max(0, quality_score),
        }

    @task
    def analyze_llm_output_quality() -> dict[str, Any]:
        """
        Analyze LLM output quality.

        Checks:
        - Response completeness
        - JSON parsing success rate
        - Hallucination indicators
        - Response latency
        """
        llm_log = Variable.get("llm_response_log", default_var=[], deserialize_json=True)

        if not llm_log:
            return {"status": "no_data", "timestamp": datetime.utcnow().isoformat()}

        recent = llm_log[-100:]

        # Calculate metrics
        complete_responses = sum(1 for r in recent if r.get("complete", False))
        json_success = sum(1 for r in recent if r.get("json_valid", False))
        latencies = [r.get("latency_ms", 0) for r in recent]

        completion_rate = complete_responses / len(recent) if recent else 0
        json_success_rate = json_success / len(recent) if recent else 0
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        p95_latency = np.percentile(latencies, 95) if latencies else 0

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "samples_analyzed": len(recent),
            "completion_rate": completion_rate,
            "json_success_rate": json_success_rate,
            "latency_ms": {"avg": avg_latency, "p95": p95_latency},
            "quality_score": (completion_rate * 50 + json_success_rate * 50),
        }

    @task
    def generate_quality_report(embedding_quality: dict, extraction_quality: dict, llm_quality: dict) -> dict[str, Any]:
        """Generate comprehensive quality report."""
        # Calculate overall score
        scores = [
            embedding_quality.get("quality_score", 0),
            extraction_quality.get("quality_score", 0),
            llm_quality.get("quality_score", 0),
        ]
        valid_scores = [s for s in scores if s > 0]
        overall_score = sum(valid_scores) / len(valid_scores) if valid_scores else 0

        # Identify issues
        issues = []
        if embedding_quality.get("quality_score", 100) < 80:
            issues.append("Embedding quality below threshold")
        if extraction_quality.get("success_rate", 1) < 0.95:
            issues.append("High extraction failure rate")
        if llm_quality.get("completion_rate", 1) < 0.98:
            issues.append("Incomplete LLM responses detected")

        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_score": overall_score,
            "status": "healthy" if overall_score >= 80 else "degraded" if overall_score >= 60 else "critical",
            "components": {"embedding": embedding_quality, "extraction": extraction_quality, "llm": llm_quality},
            "issues": issues,
            "recommendations": _generate_recommendations(issues),
        }

        # Store report
        Variable.set("capstone_quality_report", report, serialize_json=True)

        return report


def _generate_recommendations(issues: list[str]) -> list[str]:
    """Generate actionable recommendations based on issues."""
    recommendations = []

    for issue in issues:
        if "embedding" in issue.lower():
            recommendations.append("Review embedding model configuration")
            recommendations.append("Check for input text preprocessing issues")
        elif "extraction" in issue.lower():
            recommendations.append("Review document parser configuration")
            recommendations.append("Check for unsupported document formats")
        elif "llm" in issue.lower():
            recommendations.append("Review prompt templates for clarity")
            recommendations.append("Consider increasing max_tokens parameter")

    return recommendations


quality_tracker()
```

## Deliverables

1. **`dags/capstone_monitoring_dag.py`** - Main monitoring DAG with metric collection
2. **`dags/capstone_metrics_collector.py`** - ML-specific metrics collection
3. **`dags/capstone_api_reporter.py`** - REST API integration for dashboards
4. **`dags/capstone_sla_handler.py`** - SLA miss handling with escalation
5. **`dags/capstone_quality_tracker.py`** - Quality metrics analysis

## Testing Your Configuration

### Verify Monitoring DAG

```bash
# Trigger monitoring DAG
airflow dags trigger capstone_monitoring

# Check task logs
airflow tasks logs capstone_monitoring collect_pipeline_metrics 2024-01-01

# Verify metrics collection
airflow variables get capstone_latest_metrics
```

### Test API Integration

```bash
# Start mock API server for testing
python -m http.server 8080 &

# Set API endpoint variable
airflow variables set dashboard_api_endpoint http://localhost:8080/api/v1/pipeline-status

# Trigger reporter
airflow dags trigger capstone_api_reporter
```

### Verify Quality Tracking

```bash
# Check quality report
airflow variables get capstone_quality_report | jq .

# Verify quality scores
python -c "
from airflow.models import Variable
import json
report = Variable.get('capstone_quality_report', deserialize_json=True)
print(f\"Overall Score: {report.get('overall_score', 0)}\")
print(f\"Status: {report.get('status', 'unknown')}\")
"
```

## Hints

<details>
<summary>Hint 1: Efficient metric queries</summary>

```python
# Use window functions for efficient aggregations
from sqlalchemy import and_, func

session.query(
    TaskInstance.dag_id,
    TaskInstance.task_id,
    func.count().label("total"),
    func.sum(case((TaskInstance.state == "success", 1), else_=0)).label("success"),
).filter(and_(TaskInstance.dag_id.like("capstone_%"), TaskInstance.start_date >= last_hour)).group_by(
    TaskInstance.dag_id, TaskInstance.task_id
).all()
```

</details>

<details>
<summary>Hint 2: Prometheus metric format</summary>

```python
# Proper Prometheus exposition format
metrics = [
    "# HELP capstone_health_score Pipeline health score",
    "# TYPE capstone_health_score gauge",
    f'capstone_health_score{{pipeline="document-processor"}} {health_score}',
    "",
    "# HELP capstone_documents_processed Total documents processed",
    "# TYPE capstone_documents_processed counter",
    f"capstone_documents_processed {doc_count}",
]
```

</details>

<details>
<summary>Hint 3: Rate limiting API calls</summary>

```python
from datetime import datetime
from functools import lru_cache


@lru_cache(maxsize=1)
def get_cached_metrics(cache_key: str) -> dict:
    """Cache metrics for 5 minutes."""
    # cache_key should include timestamp rounded to 5 min
    return _fetch_metrics()


# Generate cache key
cache_key = datetime.utcnow().strftime("%Y-%m-%d-%H-") + str(datetime.utcnow().minute // 5)
```

</details>

## Success Criteria

- [ ] Monitoring DAG runs every 5 minutes without errors
- [ ] Pipeline metrics include success rate, duration, and error counts
- [ ] Task-level metrics track retries and processing times
- [ ] Pool utilization is monitored and alerts on threshold breach
- [ ] REST API receives status updates every 5 minutes
- [ ] Prometheus Pushgateway receives formatted metrics
- [ ] SLA miss callback triggers appropriate escalation
- [ ] Quality metrics track embedding, extraction, and LLM quality
- [ ] Quality report generates actionable recommendations

## References

- [Module 09: Production Patterns](../../09-production-patterns/README.md)
- [Module 10: Advanced Topics - API Integration](../../10-advanced-topics/README.md)
- [Module 12: REST API](../../12-rest-api/README.md)
- [Module 14: Resource Management](../../14-resource-management/README.md)
- [Airflow Metrics Documentation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html)

---

[Previous: Exercise 16.5 - Deployment](exercise_16_5_deployment.md) | [Next: Exercise 16.7 - Testing](exercise_16_7_testing.md)
