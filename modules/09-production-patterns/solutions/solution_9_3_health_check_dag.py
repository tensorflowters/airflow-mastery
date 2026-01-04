"""
Solution 9.3: Health Check DAG
===============================

A monitoring DAG that verifies Airflow system health every 5 minutes.

Checks:
1. Database connectivity
2. Critical connections (defined in variables)
3. Required variables exist
4. External service health
5. Resource availability

This DAG should alert on failures to catch infrastructure issues early.
"""

from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def on_health_check_failure(context):
    """
    Alert when health check fails.

    This indicates a critical infrastructure issue.
    """
    task_id = context["ti"].task_id
    exception = context.get("exception", "Unknown error")

    logger.critical(
        f"HEALTH CHECK FAILED: {task_id}",
        extra={
            "task_id": task_id,
            "error": str(exception),
            "severity": "critical",
        }
    )

    # In production, send PagerDuty/Slack alert
    # from airflow.providers.pagerduty.hooks.pagerduty import PagerdutyHook
    # hook = PagerdutyHook(pagerduty_conn_id="pagerduty")
    # hook.create_event(
    #     summary=f"Airflow health check failed: {task_id}",
    #     severity="critical",
    #     source="airflow-health-check",
    # )


@dag(
    dag_id="solution_9_3_health_check",
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlap
    tags=["monitoring", "health-check", "solution"],
    description="System health monitoring DAG",
    default_args={
        "owner": "platform-team",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": on_health_check_failure,
    },
)
def health_check():
    """
    System health check DAG.

    Runs every 5 minutes to verify:
    - Database connectivity
    - Connection validity
    - Variable availability
    - Scheduler health
    """

    @task(execution_timeout=timedelta(seconds=30))
    def check_database() -> dict:
        """
        Verify database connectivity and basic operations.

        Checks:
        - Can connect to metadata database
        - Can read from DAG table
        - Response time is acceptable
        """
        import time
        from airflow.settings import Session

        start_time = time.time()
        result = {
            "check": "database",
            "status": "unknown",
            "latency_ms": 0,
        }

        try:
            session = Session()

            # Simple query to verify connectivity
            from airflow.models import DagModel
            dag_count = session.query(DagModel).count()

            latency = (time.time() - start_time) * 1000
            result["latency_ms"] = round(latency, 2)
            result["dag_count"] = dag_count

            # Check latency threshold
            if latency > 5000:  # 5 seconds is too slow
                result["status"] = "degraded"
                result["message"] = f"High latency: {latency:.0f}ms"
                logger.warning(f"Database latency high: {latency:.0f}ms")
            else:
                result["status"] = "healthy"
                result["message"] = f"Connected, {dag_count} DAGs"
                logger.info(f"Database OK: {latency:.0f}ms, {dag_count} DAGs")

            session.close()

        except Exception as e:
            result["status"] = "unhealthy"
            result["message"] = str(e)
            logger.error(f"Database check failed: {e}")
            raise AirflowException(f"Database health check failed: {e}")

        return result

    @task(execution_timeout=timedelta(seconds=60))
    def check_connections() -> dict:
        """
        Verify critical Airflow connections are working.

        Connections to check are defined in Variable 'critical_connections'.
        """
        from airflow.models import Connection, Variable
        from airflow.hooks.base import BaseHook

        result = {
            "check": "connections",
            "status": "healthy",
            "connections_checked": 0,
            "failures": [],
        }

        # Get list of critical connections from Variable
        # Default to common connections if variable not set
        try:
            critical_conns = Variable.get(
                "critical_connections",
                default_var="aws_default,postgres_default",
                deserialize_json=False
            )
            conn_ids = [c.strip() for c in critical_conns.split(",")]
        except Exception:
            conn_ids = ["aws_default"]  # Minimal default

        for conn_id in conn_ids:
            try:
                # Verify connection exists
                conn = Connection.get_connection_from_secrets(conn_id)

                # Basic validation
                if conn.host or conn.conn_type:
                    logger.info(f"Connection {conn_id}: OK")
                    result["connections_checked"] += 1
                else:
                    logger.warning(f"Connection {conn_id}: empty")
                    result["failures"].append(f"{conn_id}: empty config")

            except Exception as e:
                logger.warning(f"Connection {conn_id} check failed: {e}")
                result["failures"].append(f"{conn_id}: {str(e)[:50]}")

        if result["failures"]:
            result["status"] = "degraded" if result["connections_checked"] > 0 else "unhealthy"

        return result

    @task(execution_timeout=timedelta(seconds=30))
    def check_variables() -> dict:
        """
        Verify critical Airflow variables exist.

        Variables to check are defined in Variable 'critical_variables'.
        """
        from airflow.models import Variable

        result = {
            "check": "variables",
            "status": "healthy",
            "variables_checked": 0,
            "missing": [],
        }

        # Get list of critical variables
        try:
            critical_vars = Variable.get(
                "critical_variables",
                default_var="environment,alert_email",
                deserialize_json=False
            )
            var_names = [v.strip() for v in critical_vars.split(",")]
        except Exception:
            var_names = ["environment"]  # Minimal default

        for var_name in var_names:
            try:
                value = Variable.get(var_name)
                if value:
                    result["variables_checked"] += 1
                    logger.info(f"Variable {var_name}: present")
                else:
                    result["missing"].append(f"{var_name}: empty")
            except KeyError:
                result["missing"].append(var_name)
                logger.warning(f"Variable {var_name}: missing")

        if result["missing"]:
            result["status"] = "degraded"

        return result

    @task(execution_timeout=timedelta(seconds=30))
    def check_scheduler_health() -> dict:
        """
        Verify scheduler is processing DAGs.

        Checks:
        - Recent DAG runs exist
        - Scheduler heartbeat is recent
        """
        from airflow.models import DagRun
        from airflow.settings import Session
        from datetime import datetime, timedelta

        result = {
            "check": "scheduler",
            "status": "unknown",
            "recent_runs": 0,
        }

        try:
            session = Session()

            # Check for recent DAG runs (last 10 minutes)
            recent_threshold = datetime.utcnow() - timedelta(minutes=10)
            recent_runs = session.query(DagRun).filter(
                DagRun.start_date > recent_threshold
            ).count()

            result["recent_runs"] = recent_runs

            if recent_runs > 0:
                result["status"] = "healthy"
                result["message"] = f"{recent_runs} runs in last 10 min"
                logger.info(f"Scheduler OK: {recent_runs} recent runs")
            else:
                # No recent runs could be normal (no scheduled DAGs)
                # Check if there are active DAGs
                from airflow.models import DagModel
                active_dags = session.query(DagModel).filter(
                    DagModel.is_paused == False,
                    DagModel.is_active == True
                ).count()

                if active_dags > 0:
                    result["status"] = "degraded"
                    result["message"] = "No recent runs but active DAGs exist"
                    logger.warning("Scheduler may be stuck")
                else:
                    result["status"] = "healthy"
                    result["message"] = "No recent runs (no active DAGs)"

            session.close()

        except Exception as e:
            result["status"] = "unhealthy"
            result["message"] = str(e)
            logger.error(f"Scheduler check failed: {e}")

        return result

    @task(execution_timeout=timedelta(seconds=30))
    def check_disk_space() -> dict:
        """
        Check disk space for logs and DAGs directories.
        """
        import shutil
        import os

        result = {
            "check": "disk_space",
            "status": "healthy",
            "paths": {},
        }

        paths_to_check = [
            os.environ.get("AIRFLOW_HOME", "/opt/airflow"),
            "/opt/airflow/logs",
            "/opt/airflow/dags",
            "/tmp",
        ]

        for path in paths_to_check:
            try:
                if os.path.exists(path):
                    usage = shutil.disk_usage(path)
                    used_percent = (usage.used / usage.total) * 100
                    free_gb = usage.free / (1024**3)

                    result["paths"][path] = {
                        "used_percent": round(used_percent, 1),
                        "free_gb": round(free_gb, 2),
                    }

                    if used_percent > 90:
                        result["status"] = "unhealthy"
                        logger.error(f"Disk space critical for {path}: {used_percent:.1f}%")
                    elif used_percent > 80:
                        if result["status"] == "healthy":
                            result["status"] = "degraded"
                        logger.warning(f"Disk space low for {path}: {used_percent:.1f}%")

            except Exception as e:
                logger.warning(f"Could not check {path}: {e}")

        return result

    @task
    def aggregate_results(
        db_result: dict,
        conn_result: dict,
        var_result: dict,
        scheduler_result: dict,
        disk_result: dict,
    ) -> dict:
        """
        Aggregate all health check results and determine overall status.
        """
        all_results = [db_result, conn_result, var_result, scheduler_result, disk_result]

        # Determine overall status
        statuses = [r.get("status", "unknown") for r in all_results]

        if "unhealthy" in statuses:
            overall_status = "unhealthy"
        elif "degraded" in statuses:
            overall_status = "degraded"
        elif all(s == "healthy" for s in statuses):
            overall_status = "healthy"
        else:
            overall_status = "unknown"

        summary = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": overall_status,
            "checks": {
                "database": db_result["status"],
                "connections": conn_result["status"],
                "variables": var_result["status"],
                "scheduler": scheduler_result["status"],
                "disk_space": disk_result["status"],
            },
            "details": {
                "database": db_result,
                "connections": conn_result,
                "variables": var_result,
                "scheduler": scheduler_result,
                "disk_space": disk_result,
            },
        }

        # Log summary
        logger.info(
            f"Health check complete: {overall_status}",
            extra={
                "overall_status": overall_status,
                "checks": summary["checks"],
            }
        )

        # Fail the task if unhealthy (triggers alert)
        if overall_status == "unhealthy":
            raise AirflowException(
                f"Health check FAILED: {[r['check'] for r in all_results if r.get('status') == 'unhealthy']}"
            )

        return summary

    # Execute all checks in parallel, then aggregate
    db = check_database()
    conns = check_connections()
    vars_check = check_variables()
    scheduler = check_scheduler_health()
    disk = check_disk_space()

    aggregate_results(db, conns, vars_check, scheduler, disk)


# Instantiate the DAG
health_check()
