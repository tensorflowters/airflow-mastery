"""
Capstone Solution: Monitoring Pipeline
=======================================

Production-ready monitoring DAG demonstrating:
- Scheduled health checks for all pipelines
- Metrics collection and aggregation
- SLA monitoring with alerting
- Resource utilization tracking
- Quality gate validation

Modules Applied: 09, 10, 12, 14
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Variable, dag, task
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_PATH = Variable.get("capstone_base_path", default_var="/tmp/capstone")
METRICS_PATH = f"{BASE_PATH}/metrics"

# Monitoring thresholds
ALERT_THRESHOLDS = {
    "error_rate": 0.05,  # 5% error rate triggers alert
    "processing_time_seconds": 300,  # 5 minutes max processing time
    "pool_utilization": 0.9,  # 90% pool usage triggers warning
    "quality_score_min": 0.8,  # Minimum acceptable quality
    "cost_per_document_max": 0.10,  # $0.10 max cost per document
}

# Slack webhook for alerts (in production: use Connection)
SLACK_WEBHOOK_URL = Variable.get("slack_webhook_url", default_var="")


# =============================================================================
# ALERTING UTILITIES
# =============================================================================


class AlertManager:
    """Centralized alerting for monitoring pipeline."""

    SEVERITY_LEVELS = {
        "info": "🔵",
        "warning": "🟡",
        "error": "🔴",
        "critical": "🚨",
    }

    @classmethod
    def send_alert(
        cls,
        title: str,
        message: str,
        severity: str = "warning",
        context: dict | None = None,
    ) -> dict:
        """Send alert through configured channels.

        Args:
            title: Alert title for quick identification.
            message: Detailed alert message.
            severity: Alert severity level (info, warning, error, critical).
            context: Additional context data for the alert.

        Returns:
            Dictionary containing alert details and delivery status.
        """
        icon = cls.SEVERITY_LEVELS.get(severity, "⚪")
        alert = {
            "timestamp": datetime.utcnow().isoformat(),
            "severity": severity,
            "title": f"{icon} {title}",
            "message": message,
            "context": context or {},
            "delivered": False,
        }

        # Log alert (always)
        log_method = getattr(logger, severity if severity != "critical" else "error")
        log_method(f"ALERT [{severity.upper()}]: {title} - {message}")

        # In production: send to Slack, PagerDuty, email
        if SLACK_WEBHOOK_URL and severity in ("error", "critical"):
            # Would use httpx or requests to POST to webhook
            alert["delivered"] = True

        return alert


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="capstone_monitoring_pipeline",
    description="Comprehensive monitoring for document processing platform",
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "monitoring", "observability", "production"],
    default_args={
        "owner": "capstone",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
    doc_md="""
    ## Platform Monitoring Pipeline

    Collects metrics, validates health, and triggers alerts for the
    document processing platform.

    ### Schedule
    - Every 5 minutes for near-real-time monitoring

    ### Monitored Components
    - Ingestion pipeline health and throughput
    - Processing pipeline quality and costs
    - Vector store status and index health
    - Resource utilization (pools, workers)

    ### Alerting
    - Slack notifications for warnings
    - PagerDuty escalation for critical issues
    - Email summaries for daily reports
    """,
)
def monitoring_pipeline():
    """Comprehensive platform monitoring pipeline."""
    # =========================================================================
    # PIPELINE HEALTH CHECKS
    # =========================================================================

    @task
    def check_ingestion_health() -> dict:
        """Check ingestion pipeline health and recent activity."""
        processed_dir = Path(f"{BASE_PATH}/processed")
        incoming_dir = Path(f"{BASE_PATH}/incoming")

        health = {
            "pipeline": "ingestion",
            "checked_at": datetime.utcnow().isoformat(),
            "status": "healthy",
            "metrics": {},
            "issues": [],
        }

        # Check directories exist
        if not processed_dir.exists():
            health["issues"].append("Processed directory missing")
            health["status"] = "degraded"

        # Count recent documents (last hour)
        recent_cutoff = datetime.utcnow() - timedelta(hours=1)
        recent_count = 0
        total_count = 0

        if processed_dir.exists():
            for record_path in processed_dir.glob("doc_*.json"):
                total_count += 1
                try:
                    doc = json.loads(record_path.read_text())
                    ingested_at = datetime.fromisoformat(doc.get("ingested_at", "1970-01-01T00:00:00"))
                    if ingested_at > recent_cutoff:
                        recent_count += 1
                except (json.JSONDecodeError, ValueError):
                    health["issues"].append(f"Invalid record: {record_path.name}")

        # Count pending files
        pending_count = 0
        if incoming_dir.exists():
            pending_count = len(list(incoming_dir.glob("*.pdf")))

        health["metrics"] = {
            "total_processed": total_count,
            "recent_processed": recent_count,
            "pending_files": pending_count,
            "throughput_per_hour": recent_count,
        }

        # Evaluate health status
        if pending_count > 100:
            health["issues"].append(f"High pending file count: {pending_count}")
            health["status"] = "warning"

        if health["issues"]:
            logger.warning(f"Ingestion health issues: {health['issues']}")

        return health

    @task
    def check_processing_health() -> dict:
        """Check processing pipeline health and LLM performance."""
        enriched_dir = Path(f"{BASE_PATH}/enriched")
        review_dir = Path(f"{BASE_PATH}/review")

        health = {
            "pipeline": "processing",
            "checked_at": datetime.utcnow().isoformat(),
            "status": "healthy",
            "metrics": {},
            "issues": [],
        }

        # Collect metrics from enriched documents
        total_cost = 0.0
        total_quality = 0.0
        enriched_count = 0
        review_count = 0

        if enriched_dir.exists():
            for record_path in enriched_dir.glob("*.json"):
                try:
                    doc = json.loads(record_path.read_text())
                    enriched_count += 1
                    total_cost += doc.get("total_llm_cost", 0)
                    total_quality += doc.get("overall_quality", 0)
                except (json.JSONDecodeError, ValueError):
                    health["issues"].append(f"Invalid enriched record: {record_path.name}")

        if review_dir.exists():
            review_count = len(list(review_dir.glob("*_review.json")))

        # Calculate averages
        avg_quality = total_quality / enriched_count if enriched_count > 0 else 0
        avg_cost = total_cost / enriched_count if enriched_count > 0 else 0

        health["metrics"] = {
            "enriched_documents": enriched_count,
            "pending_review": review_count,
            "total_llm_cost": round(total_cost, 4),
            "average_quality": round(avg_quality, 2),
            "average_cost_per_doc": round(avg_cost, 4),
            "review_rate": round(review_count / (enriched_count + review_count), 2)
            if (enriched_count + review_count) > 0
            else 0,
        }

        # Check thresholds
        if avg_quality < ALERT_THRESHOLDS["quality_score_min"]:
            health["issues"].append(
                f"Quality below threshold: {avg_quality:.2f} < {ALERT_THRESHOLDS['quality_score_min']}"
            )
            health["status"] = "warning"

        if avg_cost > ALERT_THRESHOLDS["cost_per_document_max"]:
            health["issues"].append(
                f"Cost above threshold: ${avg_cost:.4f} > ${ALERT_THRESHOLDS['cost_per_document_max']}"
            )
            health["status"] = "warning"

        if review_count > enriched_count * 0.2:  # >20% review rate
            health["issues"].append(f"High review rate: {review_count} documents flagged")
            health["status"] = "warning"

        return health

    @task
    def check_vector_store_health() -> dict:
        """Check vector store health and index status."""
        health = {
            "pipeline": "vector_store",
            "checked_at": datetime.utcnow().isoformat(),
            "status": "healthy",
            "metrics": {},
            "issues": [],
        }

        # Simulated vector store health check
        # In production: actual vector store API calls
        health["metrics"] = {
            "total_vectors": 1250,
            "index_size_mb": 48.5,
            "query_latency_ms": 12.3,
            "index_freshness_seconds": 300,
            "collection_count": 3,
        }

        # Check index freshness
        if health["metrics"]["index_freshness_seconds"] > 600:
            health["issues"].append("Index may be stale")
            health["status"] = "warning"

        # Check query latency
        if health["metrics"]["query_latency_ms"] > 100:
            health["issues"].append("High query latency detected")
            health["status"] = "warning"

        return health

    # =========================================================================
    # RESOURCE MONITORING
    # =========================================================================

    @task
    def check_pool_utilization() -> dict:
        """Monitor Airflow pool utilization."""
        # Simulated pool metrics
        # In production: use Airflow REST API
        pools = {
            "llm_api": {"used": 3, "total": 5, "queued": 2},
            "embedding_api": {"used": 2, "total": 3, "queued": 0},
            "database_connections": {"used": 5, "total": 10, "queued": 1},
        }

        metrics = {
            "checked_at": datetime.utcnow().isoformat(),
            "pools": {},
            "issues": [],
        }

        for pool_name, stats in pools.items():
            utilization = stats["used"] / stats["total"] if stats["total"] > 0 else 0
            metrics["pools"][pool_name] = {
                "used": stats["used"],
                "total": stats["total"],
                "queued": stats["queued"],
                "utilization": round(utilization, 2),
            }

            if utilization >= ALERT_THRESHOLDS["pool_utilization"]:
                metrics["issues"].append(f"High utilization on pool '{pool_name}': {utilization:.0%}")

        return metrics

    @task
    def check_dag_run_history() -> dict:
        """Analyze recent DAG run success rates."""
        # Simulated DAG run history
        # In production: use Airflow REST API
        dag_stats = {
            "capstone_ingestion_pipeline": {
                "runs_24h": 96,
                "successes": 94,
                "failures": 2,
                "avg_duration_seconds": 45,
            },
            "capstone_processing_pipeline": {
                "runs_24h": 48,
                "successes": 46,
                "failures": 2,
                "avg_duration_seconds": 180,
            },
            "capstone_vector_store_pipeline": {
                "runs_24h": 48,
                "successes": 48,
                "failures": 0,
                "avg_duration_seconds": 120,
            },
        }

        metrics = {
            "checked_at": datetime.utcnow().isoformat(),
            "dags": {},
            "issues": [],
        }

        for dag_id, stats in dag_stats.items():
            success_rate = stats["successes"] / stats["runs_24h"] if stats["runs_24h"] > 0 else 0
            error_rate = 1 - success_rate

            metrics["dags"][dag_id] = {
                "runs_24h": stats["runs_24h"],
                "success_rate": round(success_rate, 3),
                "avg_duration_seconds": stats["avg_duration_seconds"],
            }

            if error_rate > ALERT_THRESHOLDS["error_rate"]:
                metrics["issues"].append(f"High error rate for '{dag_id}': {error_rate:.1%}")

            if stats["avg_duration_seconds"] > ALERT_THRESHOLDS["processing_time_seconds"]:
                metrics["issues"].append(f"Slow execution for '{dag_id}': {stats['avg_duration_seconds']}s avg")

        return metrics

    # =========================================================================
    # METRICS AGGREGATION
    # =========================================================================

    @task
    def aggregate_metrics(
        ingestion: dict,
        processing: dict,
        vector_store: dict,
        pools: dict,
        dag_history: dict,
        sla: dict,
        costs: dict,
    ) -> dict:
        """Aggregate all metrics into unified report."""
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": "healthy",
            "components": {
                "ingestion": ingestion,
                "processing": processing,
                "vector_store": vector_store,
                "pools": pools,
                "dag_history": dag_history,
                "sla": sla,
                "costs": costs,
            },
            "all_issues": [],
            "summary": {},
        }

        # Collect all issues
        for component in [ingestion, processing, vector_store, pools, dag_history, sla]:
            report["all_issues"].extend(component.get("issues", []))

        # Determine overall status
        if any(c.get("status") == "error" for c in [ingestion, processing, vector_store]):
            report["overall_status"] = "error"
        elif report["all_issues"]:
            report["overall_status"] = "warning"

        # Summary metrics
        report["summary"] = {
            "total_documents_processed": ingestion["metrics"].get("total_processed", 0),
            "total_enriched": processing["metrics"].get("enriched_documents", 0),
            "total_vectors": vector_store["metrics"].get("total_vectors", 0),
            "total_llm_cost": costs.get("total", 0),
            "daily_cost": costs.get("total", 0),
            "average_quality": processing["metrics"].get("average_quality", 0),
            "sla_compliance_rate": sla.get("compliance_rate", 1.0),
            "issue_count": len(report["all_issues"]),
        }

        logger.info(f"Monitoring report: status={report['overall_status']}, issues={len(report['all_issues'])}")

        return report

    # =========================================================================
    # ALERTING
    # =========================================================================

    @task.branch
    def check_alert_conditions(report: dict) -> str:
        """Determine if alerts should be sent based on report."""
        if report["overall_status"] == "error":
            return "send_critical_alert"
        elif report["overall_status"] == "warning":
            return "send_warning_alert"
        return "store_metrics_only"

    @task
    def send_critical_alert(report: dict) -> dict:
        """Send critical alert for severe issues."""
        issues = report["all_issues"]
        return AlertManager.send_alert(
            title="Critical: Document Processing Platform Issue",
            message=f"Critical issues detected: {', '.join(issues[:3])}",
            severity="critical",
            context={
                "overall_status": report["overall_status"],
                "issue_count": len(issues),
                "summary": report["summary"],
            },
        )

    @task
    def send_warning_alert(report: dict) -> dict:
        """Send warning alert for non-critical issues."""
        issues = report["all_issues"]
        return AlertManager.send_alert(
            title="Warning: Document Processing Platform",
            message=f"Issues requiring attention: {', '.join(issues[:3])}",
            severity="warning",
            context={
                "overall_status": report["overall_status"],
                "issue_count": len(issues),
            },
        )

    @task
    def store_metrics_only(report: dict) -> dict:
        """Store metrics when no alerts needed."""
        logger.info("All systems healthy - storing metrics only")
        return {"action": "metrics_stored", "status": report["overall_status"]}

    # =========================================================================
    # PERSISTENCE
    # =========================================================================

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def store_monitoring_report(
        report: dict,
        alert_result: dict | None = None,
    ) -> dict:
        """Persist monitoring report for historical analysis."""
        metrics_dir = Path(METRICS_PATH)
        metrics_dir.mkdir(parents=True, exist_ok=True)

        # Store with timestamp
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        report_path = metrics_dir / f"monitoring_{timestamp}.json"

        final_report = {
            **report,
            "alert_sent": alert_result is not None,
            "alert_details": alert_result,
        }

        report_path.write_text(json.dumps(final_report, indent=2))

        # Also maintain latest report symlink for dashboards
        latest_path = metrics_dir / "latest.json"
        if latest_path.exists():
            latest_path.unlink()
        latest_path.write_text(json.dumps(final_report, indent=2))

        logger.info(f"Monitoring report stored: {report_path}")
        return {"stored_at": str(report_path), "status": report["overall_status"]}

    # =========================================================================
    # SLA TRACKING
    # =========================================================================

    @task
    def check_sla_compliance() -> dict:
        """Check SLA compliance for all pipelines."""
        # Simulated SLA metrics
        # In production: query Airflow DB for SLA misses
        sla_status = {
            "checked_at": datetime.utcnow().isoformat(),
            "sla_definitions": {
                "ingestion_completion": {"target_minutes": 15, "actual_minutes": 12},
                "processing_completion": {"target_minutes": 30, "actual_minutes": 28},
                "end_to_end": {"target_minutes": 60, "actual_minutes": 45},
            },
            "misses_24h": 0,
            "compliance_rate": 1.0,
            "issues": [],
        }

        # Check each SLA
        for sla_name, metrics in sla_status["sla_definitions"].items():
            if metrics["actual_minutes"] > metrics["target_minutes"]:
                sla_status["misses_24h"] += 1
                sla_status["issues"].append(
                    f"SLA miss: {sla_name} - {metrics['actual_minutes']}m > {metrics['target_minutes']}m"
                )

        # Calculate compliance
        total_slas = len(sla_status["sla_definitions"])
        sla_status["compliance_rate"] = (total_slas - sla_status["misses_24h"]) / total_slas

        if sla_status["misses_24h"] > 0:
            logger.warning(f"SLA misses detected: {sla_status['misses_24h']}")

        return sla_status

    # =========================================================================
    # COST TRACKING
    # =========================================================================

    @task
    def calculate_daily_costs() -> dict:
        """Calculate and track daily LLM costs."""
        enriched_dir = Path(f"{BASE_PATH}/enriched")

        costs = {
            "calculated_at": datetime.utcnow().isoformat(),
            "period": "24h",
            "breakdown": {
                "extraction": 0.0,
                "summarization": 0.0,
                "classification": 0.0,
            },
            "total": 0.0,
            "documents_processed": 0,
            "cost_per_document": 0.0,
        }

        today = datetime.utcnow().date()

        if enriched_dir.exists():
            for record_path in enriched_dir.glob("*.json"):
                try:
                    doc = json.loads(record_path.read_text())
                    enriched_at = datetime.fromisoformat(doc.get("enriched_at", "1970-01-01T00:00:00"))

                    if enriched_at.date() == today:
                        costs["documents_processed"] += 1
                        costs["breakdown"]["extraction"] += doc.get("extraction_cost", 0)
                        costs["breakdown"]["summarization"] += doc.get("summary_cost", 0)
                        costs["breakdown"]["classification"] += doc.get("classification_cost", 0)
                except (json.JSONDecodeError, ValueError):
                    pass

        # Calculate totals
        costs["total"] = sum(costs["breakdown"].values())
        if costs["documents_processed"] > 0:
            costs["cost_per_document"] = costs["total"] / costs["documents_processed"]

        # Round values
        costs["total"] = round(costs["total"], 4)
        costs["cost_per_document"] = round(costs["cost_per_document"], 4)
        for key in costs["breakdown"]:
            costs["breakdown"][key] = round(costs["breakdown"][key], 4)

        logger.info(f"Daily costs: ${costs['total']:.4f} for {costs['documents_processed']} documents")

        return costs

    # =========================================================================
    # TASK FLOW
    # =========================================================================

    # Health checks (parallel)
    ingestion_health = check_ingestion_health()
    processing_health = check_processing_health()
    vector_store_health = check_vector_store_health()

    # Resource monitoring (parallel)
    pool_metrics = check_pool_utilization()
    dag_metrics = check_dag_run_history()

    # Additional metrics (parallel)
    sla_metrics = check_sla_compliance()
    cost_metrics = calculate_daily_costs()

    # Aggregate all metrics
    report = aggregate_metrics(
        ingestion=ingestion_health,
        processing=processing_health,
        vector_store=vector_store_health,
        pools=pool_metrics,
        dag_history=dag_metrics,
        sla=sla_metrics,
        costs=cost_metrics,
    )

    # Alert routing
    alert_branch = check_alert_conditions(report)

    # Alert paths
    critical_alert = send_critical_alert(report)
    warning_alert = send_warning_alert(report)
    no_alert = store_metrics_only(report)

    # Set dependencies for branching
    alert_branch >> [critical_alert, warning_alert, no_alert]

    # Store final report (runs regardless of alert path)
    store_monitoring_report(
        report=report,
        alert_result=None,  # Simplified: could aggregate from branches
    )


# Instantiate the DAG
monitoring_pipeline()
