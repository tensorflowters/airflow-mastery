"""
Solution 12.3: Automation Script
=================================

Comprehensive automation for Airflow operations including
deployment validation, batch execution, health monitoring,
and operations reporting.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from typing import Optional

from solution_12_1_api_basics import AirflowAPIClient, APIError
from solution_12_2_dag_management import DAGManager

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class ValidationResult:
    """Result of DAG validation."""

    dag_id: str
    exists: bool = False
    healthy: bool = False
    is_paused: bool = True
    task_count: int = 0
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Check if validation passed."""
        return self.exists and self.healthy and not self.errors


@dataclass
class BatchResult:
    """Result of batch DAG execution."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    results: list = field(default_factory=list)
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None

    @property
    def duration(self) -> Optional[timedelta]:
        """Calculate batch duration."""
        if self.end_time:
            return self.end_time - self.start_time
        return None


@dataclass
class HealthCheck:
    """Individual health check result."""

    name: str
    healthy: bool
    message: str = ""
    details: dict = field(default_factory=dict)


@dataclass
class HealthReport:
    """System health report."""

    healthy: bool = False
    timestamp: datetime = field(default_factory=datetime.utcnow)
    checks: list = field(default_factory=list)
    summary: str = ""

    def add_check(self, check: HealthCheck):
        """Add a health check result."""
        self.checks.append(check)
        self._update_status()

    def _update_status(self):
        """Update overall health status."""
        self.healthy = all(c.healthy for c in self.checks)
        passed = sum(1 for c in self.checks if c.healthy)
        self.summary = f"{passed}/{len(self.checks)} checks passed"


# =============================================================================
# PART 1: DEPLOYMENT VALIDATOR
# =============================================================================


class DeploymentValidator:
    """
    Validates DAG deployments.

    Ensures DAGs are properly deployed, healthy, and ready for execution.
    """

    def __init__(self, client: AirflowAPIClient, manager: DAGManager):
        self.client = client
        self.manager = manager

    def validate_dags(self, expected_dags: list) -> list[ValidationResult]:
        """
        Validate that expected DAGs exist and are healthy.

        Args:
            expected_dags: List of dag_ids to validate

        Returns:
            List of ValidationResult for each DAG
        """
        results = []

        for dag_id in expected_dags:
            result = ValidationResult(dag_id=dag_id)

            try:
                # Check if DAG exists
                dag = self.client.get_dag(dag_id)
                if dag is None:
                    result.errors.append("DAG not found")
                    results.append(result)
                    continue

                result.exists = True
                result.is_paused = dag.get("is_paused", True)

                # Check for import errors
                file_token = dag.get("file_token")
                if dag.get("has_import_errors"):
                    result.errors.append("DAG has import errors")

                # Get task count
                try:
                    tasks = self.client.list_tasks(dag_id)
                    result.task_count = len(tasks.get("tasks", []))
                except APIError:
                    result.warnings.append("Could not retrieve tasks")

                # Check if DAG is paused (warning, not error)
                if result.is_paused:
                    result.warnings.append("DAG is paused")

                # Determine overall health
                result.healthy = result.exists and not result.errors

            except Exception as e:
                result.errors.append(f"Validation error: {str(e)}")

            results.append(result)
            logger.info(f"Validated {dag_id}: {'PASS' if result.passed else 'FAIL'}")

        return results

    def validate_with_tasks(
        self,
        dag_id: str,
        expected_tasks: list[str],
    ) -> ValidationResult:
        """
        Validate DAG has expected tasks.

        Args:
            dag_id: DAG to validate
            expected_tasks: List of expected task_ids

        Returns:
            ValidationResult with task validation
        """
        result = self.validate_dags([dag_id])[0]
        if not result.exists:
            return result

        try:
            tasks = self.client.list_tasks(dag_id)
            task_ids = {t["task_id"] for t in tasks.get("tasks", [])}

            missing = set(expected_tasks) - task_ids
            if missing:
                result.errors.append(f"Missing tasks: {missing}")
                result.healthy = False

            extra = task_ids - set(expected_tasks)
            if extra:
                result.warnings.append(f"Unexpected tasks: {extra}")

        except APIError as e:
            result.errors.append(f"Task check failed: {e}")
            result.healthy = False

        return result

    def run_smoke_test(
        self,
        dag_id: str,
        test_conf: dict = None,
        timeout: int = 300,
    ) -> bool:
        """
        Run a smoke test on a DAG.

        Args:
            dag_id: DAG to test
            test_conf: Test configuration
            timeout: Max wait time

        Returns:
            True if smoke test passed
        """
        logger.info(f"Running smoke test for {dag_id}")

        # Use test configuration or default
        conf = test_conf or {"smoke_test": True}

        try:
            # Trigger and wait
            result = self.manager.trigger_and_wait(
                dag_id=dag_id,
                conf=conf,
                timeout=timeout,
                poll_interval=5,
            )

            success = result.get("state") == "success"
            logger.info(f"Smoke test {'PASSED' if success else 'FAILED'}: {dag_id}")
            return success

        except TimeoutError:
            logger.error(f"Smoke test timeout: {dag_id}")
            return False
        except Exception as e:
            logger.error(f"Smoke test error: {dag_id} - {e}")
            return False


# =============================================================================
# PART 2: BATCH ORCHESTRATOR
# =============================================================================


class BatchOrchestrator:
    """
    Orchestrates batch DAG executions.

    Supports sequential and parallel execution with concurrency control.
    """

    def __init__(self, client: AirflowAPIClient, manager: DAGManager):
        self.client = client
        self.manager = manager

    def run_sequence(
        self,
        dag_configs: list[dict],
        stop_on_failure: bool = True,
    ) -> BatchResult:
        """
        Run DAGs in sequence, waiting for each to complete.

        Args:
            dag_configs: List of {"dag_id": str, "conf": dict}
            stop_on_failure: Stop if any DAG fails

        Returns:
            BatchResult with execution summary
        """
        result = BatchResult(total=len(dag_configs))
        logger.info(f"Starting sequential batch: {result.total} DAGs")

        for config in dag_configs:
            dag_id = config.get("dag_id")
            conf = config.get("conf", {})

            try:
                run = self.manager.trigger_and_wait(
                    dag_id=dag_id,
                    conf=conf,
                    timeout=config.get("timeout", 3600),
                )

                state = run.get("state")
                if state == "success":
                    result.succeeded += 1
                    result.results.append({
                        "dag_id": dag_id,
                        "status": "success",
                        "run_id": run.get("dag_run_id"),
                    })
                else:
                    result.failed += 1
                    result.results.append({
                        "dag_id": dag_id,
                        "status": "failed",
                        "state": state,
                    })

                    if stop_on_failure:
                        result.skipped = result.total - result.succeeded - result.failed
                        logger.warning(f"Stopping batch due to failure: {dag_id}")
                        break

            except Exception as e:
                result.failed += 1
                result.results.append({
                    "dag_id": dag_id,
                    "status": "error",
                    "error": str(e),
                })

                if stop_on_failure:
                    result.skipped = result.total - result.succeeded - result.failed
                    break

        result.end_time = datetime.utcnow()
        logger.info(
            f"Batch complete: {result.succeeded} succeeded, "
            f"{result.failed} failed, {result.skipped} skipped"
        )
        return result

    def run_parallel(
        self,
        dag_configs: list[dict],
        max_concurrent: int = 3,
    ) -> BatchResult:
        """
        Run DAGs in parallel with concurrency limit.

        Args:
            dag_configs: List of {"dag_id": str, "conf": dict}
            max_concurrent: Maximum concurrent runs

        Returns:
            BatchResult with execution summary
        """
        result = BatchResult(total=len(dag_configs))
        logger.info(f"Starting parallel batch: {result.total} DAGs, max {max_concurrent}")

        def execute_dag(config: dict) -> dict:
            dag_id = config.get("dag_id")
            try:
                run = self.manager.trigger_and_wait(
                    dag_id=dag_id,
                    conf=config.get("conf", {}),
                    timeout=config.get("timeout", 3600),
                )
                return {
                    "dag_id": dag_id,
                    "status": "success" if run.get("state") == "success" else "failed",
                    "run_id": run.get("dag_run_id"),
                    "state": run.get("state"),
                }
            except Exception as e:
                return {
                    "dag_id": dag_id,
                    "status": "error",
                    "error": str(e),
                }

        # Execute in parallel with concurrency limit
        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = {
                executor.submit(execute_dag, config): config
                for config in dag_configs
            }

            for future in as_completed(futures):
                run_result = future.result()
                result.results.append(run_result)

                if run_result["status"] == "success":
                    result.succeeded += 1
                else:
                    result.failed += 1

        result.end_time = datetime.utcnow()
        logger.info(
            f"Parallel batch complete: {result.succeeded} succeeded, {result.failed} failed"
        )
        return result


# =============================================================================
# PART 3: HEALTH MONITOR
# =============================================================================


class HealthMonitor:
    """
    Monitors Airflow system health.

    Provides comprehensive health checks and failure analysis.
    """

    def __init__(self, client: AirflowAPIClient):
        self.client = client

    def check_system_health(self) -> HealthReport:
        """
        Perform comprehensive health check.

        Returns:
            HealthReport with all check results
        """
        report = HealthReport()

        # Check 1: API connectivity
        try:
            health = self.client.health_check()
            report.add_check(HealthCheck(
                name="api",
                healthy=True,
                message="API is reachable",
            ))
        except Exception as e:
            report.add_check(HealthCheck(
                name="api",
                healthy=False,
                message=f"API error: {str(e)}",
            ))
            return report  # Can't continue without API

        # Check 2: Scheduler status
        scheduler = health.get("scheduler", {})
        scheduler_healthy = scheduler.get("status") == "healthy"
        report.add_check(HealthCheck(
            name="scheduler",
            healthy=scheduler_healthy,
            message=scheduler.get("status", "unknown"),
            details=scheduler,
        ))

        # Check 3: Database status
        metadb = health.get("metadatabase", {})
        db_healthy = metadb.get("status") == "healthy"
        report.add_check(HealthCheck(
            name="database",
            healthy=db_healthy,
            message=metadb.get("status", "unknown"),
            details=metadb,
        ))

        # Check 4: Triggerer status (if present)
        triggerer = health.get("triggerer", {})
        if triggerer:
            triggerer_healthy = triggerer.get("status") == "healthy"
            report.add_check(HealthCheck(
                name="triggerer",
                healthy=triggerer_healthy,
                message=triggerer.get("status", "unknown"),
                details=triggerer,
            ))

        # Check 5: Recent failure rate
        failure_summary = self.get_failure_summary(hours=1)
        recent_failure_rate = failure_summary.get("overall_failure_rate", 0)
        failure_healthy = recent_failure_rate < 0.5  # Less than 50% failure

        report.add_check(HealthCheck(
            name="recent_runs",
            healthy=failure_healthy,
            message=f"Failure rate: {recent_failure_rate:.1%}",
            details=failure_summary,
        ))

        return report

    def get_failure_summary(self, hours: int = 24) -> dict:
        """
        Get summary of failures in recent hours.

        Args:
            hours: Lookback period

        Returns:
            Dict with failure statistics
        """
        summary = {
            "period_hours": hours,
            "dag_failures": {},
            "total_runs": 0,
            "total_failures": 0,
            "overall_failure_rate": 0.0,
        }

        try:
            dags = self.client.get_all_dags()

            for dag in dags:
                dag_id = dag["dag_id"]

                # Get recent runs
                runs = self.client.list_dag_runs(
                    dag_id=dag_id,
                    limit=50,
                    order_by="-start_date",
                )

                dag_runs = runs.get("dag_runs", [])
                if not dag_runs:
                    continue

                # Filter by time window
                cutoff = datetime.utcnow() - timedelta(hours=hours)
                recent_runs = [
                    r for r in dag_runs
                    if r.get("start_date") and
                    datetime.fromisoformat(r["start_date"].replace("Z", "+00:00")).replace(tzinfo=None) > cutoff
                ]

                if not recent_runs:
                    continue

                # Count failures
                failures = sum(1 for r in recent_runs if r.get("state") == "failed")
                total = len(recent_runs)

                summary["total_runs"] += total
                summary["total_failures"] += failures

                if failures > 0:
                    summary["dag_failures"][dag_id] = {
                        "failures": failures,
                        "total": total,
                        "failure_rate": failures / total,
                    }

            # Calculate overall rate
            if summary["total_runs"] > 0:
                summary["overall_failure_rate"] = (
                    summary["total_failures"] / summary["total_runs"]
                )

        except Exception as e:
            logger.error(f"Error getting failure summary: {e}")
            summary["error"] = str(e)

        return summary


# =============================================================================
# PART 4: OPERATIONS REPORTER
# =============================================================================


class OperationsReporter:
    """
    Generates operations reports.

    Provides summaries and metrics export for operational visibility.
    """

    def __init__(self, client: AirflowAPIClient, manager: DAGManager):
        self.client = client
        self.manager = manager

    def daily_summary(self) -> str:
        """
        Generate daily operations summary.

        Returns:
            Formatted summary string
        """
        lines = [
            "=" * 60,
            "AIRFLOW DAILY OPERATIONS SUMMARY",
            f"Generated: {datetime.utcnow().isoformat()}",
            "=" * 60,
            "",
        ]

        # Get DAG overview
        try:
            dags = self.client.get_all_dags()
            active_dags = sum(1 for d in dags if not d.get("is_paused"))
            paused_dags = len(dags) - active_dags

            lines.append("DAG OVERVIEW")
            lines.append("-" * 40)
            lines.append(f"  Total DAGs: {len(dags)}")
            lines.append(f"  Active: {active_dags}")
            lines.append(f"  Paused: {paused_dags}")
            lines.append("")
        except Exception as e:
            lines.append(f"  Error getting DAG overview: {e}")
            lines.append("")

        # Get run statistics (last 24 hours)
        try:
            state_counts = {"success": 0, "failed": 0, "running": 0}
            cutoff = datetime.utcnow() - timedelta(hours=24)

            for dag in dags[:20]:  # Limit for performance
                runs = self.client.list_dag_runs(
                    dag["dag_id"],
                    limit=20,
                    order_by="-start_date",
                )
                for run in runs.get("dag_runs", []):
                    start = run.get("start_date")
                    if not start:
                        continue
                    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")).replace(tzinfo=None)
                    if start_dt > cutoff:
                        state = run.get("state", "unknown")
                        if state in state_counts:
                            state_counts[state] += 1

            total_runs = sum(state_counts.values())
            lines.append("RUNS (Last 24 Hours)")
            lines.append("-" * 40)
            lines.append(f"  Total: {total_runs}")
            lines.append(f"  Success: {state_counts['success']}")
            lines.append(f"  Failed: {state_counts['failed']}")
            lines.append(f"  Running: {state_counts['running']}")
            if total_runs > 0:
                success_rate = state_counts["success"] / total_runs
                lines.append(f"  Success Rate: {success_rate:.1%}")
            lines.append("")

        except Exception as e:
            lines.append(f"  Error getting run statistics: {e}")
            lines.append("")

        # Health status
        try:
            health = self.client.health_check()
            lines.append("SYSTEM HEALTH")
            lines.append("-" * 40)
            for component in ["scheduler", "metadatabase", "triggerer"]:
                status = health.get(component, {}).get("status", "unknown")
                emoji = "✓" if status == "healthy" else "✗"
                lines.append(f"  {emoji} {component.title()}: {status}")
            lines.append("")
        except Exception as e:
            lines.append(f"  Error getting health: {e}")
            lines.append("")

        lines.append("=" * 60)
        return "\n".join(lines)

    def export_metrics(self, format: str = "json") -> str:
        """
        Export metrics in specified format.

        Args:
            format: Output format ("json" or "csv")

        Returns:
            Formatted metrics string
        """
        metrics = {
            "timestamp": datetime.utcnow().isoformat(),
            "dags": {},
            "system": {},
        }

        # Collect DAG metrics
        try:
            dags = self.client.get_all_dags()
            metrics["dags"]["total"] = len(dags)
            metrics["dags"]["active"] = sum(1 for d in dags if not d.get("is_paused"))
            metrics["dags"]["paused"] = metrics["dags"]["total"] - metrics["dags"]["active"]
        except Exception as e:
            metrics["dags"]["error"] = str(e)

        # Collect system metrics
        try:
            health = self.client.health_check()
            metrics["system"]["scheduler_healthy"] = (
                health.get("scheduler", {}).get("status") == "healthy"
            )
            metrics["system"]["db_healthy"] = (
                health.get("metadatabase", {}).get("status") == "healthy"
            )
        except Exception as e:
            metrics["system"]["error"] = str(e)

        if format == "json":
            return json.dumps(metrics, indent=2)
        elif format == "csv":
            # Simple CSV format
            lines = ["metric,value"]
            lines.append(f"timestamp,{metrics['timestamp']}")
            lines.append(f"dags_total,{metrics['dags'].get('total', 'N/A')}")
            lines.append(f"dags_active,{metrics['dags'].get('active', 'N/A')}")
            lines.append(f"scheduler_healthy,{metrics['system'].get('scheduler_healthy', 'N/A')}")
            return "\n".join(lines)
        else:
            raise ValueError(f"Unknown format: {format}")


# =============================================================================
# MAIN AUTOMATION SCRIPT
# =============================================================================


def main():
    """Run automation script."""
    print("Airflow Automation Script - Solution 12.3")
    print("=" * 60)

    # Initialize client and manager
    client = AirflowAPIClient()
    manager = DAGManager(client)

    # Initialize all components
    validator = DeploymentValidator(client, manager)
    orchestrator = BatchOrchestrator(client, manager)
    monitor = HealthMonitor(client)
    reporter = OperationsReporter(client, manager)

    # Step 1: Health Check
    print("\n1. SYSTEM HEALTH CHECK")
    print("-" * 40)
    try:
        health_report = monitor.check_system_health()
        status = "HEALTHY" if health_report.healthy else "UNHEALTHY"
        print(f"   Overall Status: {status}")
        for check in health_report.checks:
            emoji = "✓" if check.healthy else "✗"
            print(f"   {emoji} {check.name}: {check.message}")
    except Exception as e:
        print(f"   Error: {e}")

    # Step 2: DAG Validation
    print("\n2. DAG VALIDATION")
    print("-" * 40)
    try:
        dags = client.get_all_dags()
        dag_ids = [d["dag_id"] for d in dags[:5]]  # Validate first 5
        results = validator.validate_dags(dag_ids)
        for result in results:
            status = "PASS" if result.passed else "FAIL"
            print(f"   {result.dag_id}: {status}")
            if result.errors:
                for error in result.errors:
                    print(f"      ERROR: {error}")
    except Exception as e:
        print(f"   Error: {e}")

    # Step 3: Failure Summary
    print("\n3. FAILURE SUMMARY (Last 24h)")
    print("-" * 40)
    try:
        summary = monitor.get_failure_summary(hours=24)
        print(f"   Total Runs: {summary['total_runs']}")
        print(f"   Failed: {summary['total_failures']}")
        print(f"   Failure Rate: {summary['overall_failure_rate']:.1%}")
        if summary["dag_failures"]:
            print("   Top Failing DAGs:")
            for dag_id, stats in list(summary["dag_failures"].items())[:3]:
                print(f"      - {dag_id}: {stats['failures']}/{stats['total']}")
    except Exception as e:
        print(f"   Error: {e}")

    # Step 4: Daily Summary
    print("\n4. DAILY SUMMARY")
    print("-" * 40)
    try:
        report = reporter.daily_summary()
        # Print just the key lines
        for line in report.split("\n")[:20]:
            print(f"   {line}")
        print("   ...")
    except Exception as e:
        print(f"   Error: {e}")

    # Step 5: Export Metrics
    print("\n5. METRICS EXPORT")
    print("-" * 40)
    try:
        metrics_json = reporter.export_metrics("json")
        print(f"   JSON metrics generated ({len(metrics_json)} chars)")
        metrics_csv = reporter.export_metrics("csv")
        print(f"   CSV metrics generated ({len(metrics_csv)} chars)")
    except Exception as e:
        print(f"   Error: {e}")

    print("\n" + "=" * 60)
    print("Automation script complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
