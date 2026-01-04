"""
Exercise 9.3: Health Check DAG (Starter)
=========================================

Your task: Implement a monitoring DAG that verifies Airflow system health.

This DAG should run every 5 minutes and check:
1. Database connectivity
2. Critical connections are working
3. Required variables exist
4. Scheduler is processing DAGs
5. (Bonus) Disk space availability

On failure, it should trigger alerts via callback.

Instructions:
1. Complete the TODO sections below
2. Each check should return a dict with 'check', 'status', and 'message' keys
3. Status should be: 'healthy', 'degraded', or 'unhealthy'
4. Use proper error handling with try/except
5. Add appropriate logging

Success Criteria:
- [ ] All check tasks implemented
- [ ] Proper error handling
- [ ] Aggregate function determines overall status
- [ ] Failure callback logs critical errors
- [ ] DAG runs without errors
"""

from airflow.sdk import dag, task
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def on_health_check_failure(context):
    """
    Alert when health check fails.

    TODO: Implement failure callback
    - Extract task_id and exception from context
    - Log critical error with details
    - (Optional) Send alert to PagerDuty/Slack
    """
    task_id = context["ti"].task_id
    exception = context.get("exception", "Unknown error")

    # TODO: Log the critical failure
    # Hint: Use logger.critical() with extra dict for structured logging

    pass  # Remove this and implement


@dag(
    dag_id="exercise_9_3_health_check",
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlap
    tags=["monitoring", "health-check", "exercise"],
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

    Checks system components and aggregates results.
    """

    @task(execution_timeout=timedelta(seconds=30))
    def check_database() -> dict:
        """
        Verify database connectivity and basic operations.

        TODO: Implement database health check
        - Create a session using airflow.settings.Session
        - Query the DagModel table to count DAGs
        - Measure latency
        - Return status based on latency (> 5000ms = degraded)

        Returns:
            dict with keys: check, status, latency_ms, message
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

            # TODO: Query DagModel to count DAGs
            # Hint: from airflow.models import DagModel
            #       dag_count = session.query(DagModel).count()

            # TODO: Calculate latency in milliseconds

            # TODO: Set status based on latency
            # - > 5000ms: status = "degraded"
            # - else: status = "healthy"

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

        TODO: Implement connection check
        - Get list of connection IDs from Variable 'critical_connections'
        - Default to "aws_default,postgres_default" if not set
        - For each connection, verify it exists
        - Track failures

        Returns:
            dict with keys: check, status, connections_checked, failures
        """
        from airflow.models import Connection, Variable

        result = {
            "check": "connections",
            "status": "healthy",
            "connections_checked": 0,
            "failures": [],
        }

        # TODO: Get critical_connections Variable (comma-separated list)
        # Hint: Use Variable.get("critical_connections", default_var="...")

        # TODO: Loop through each connection ID
        # Hint: conn_ids = [c.strip() for c in critical_conns.split(",")]

        # TODO: For each conn_id, try to get the connection
        # Hint: Connection.get_connection_from_secrets(conn_id)

        # TODO: Track successful checks and failures

        # TODO: Set status based on failures
        # - No failures: "healthy"
        # - Some failures but some success: "degraded"
        # - All failures: "unhealthy"

        return result

    @task(execution_timeout=timedelta(seconds=30))
    def check_variables() -> dict:
        """
        Verify critical Airflow variables exist.

        TODO: Implement variable check
        - Get list from Variable 'critical_variables'
        - Default to "environment,alert_email"
        - Check each variable exists and has a value
        - Track missing variables

        Returns:
            dict with keys: check, status, variables_checked, missing
        """
        from airflow.models import Variable

        result = {
            "check": "variables",
            "status": "healthy",
            "variables_checked": 0,
            "missing": [],
        }

        # TODO: Implement variable checking logic
        # Similar pattern to check_connections()

        return result

    @task(execution_timeout=timedelta(seconds=30))
    def check_scheduler_health() -> dict:
        """
        Verify scheduler is processing DAGs.

        TODO: Implement scheduler health check
        - Query DagRun for runs in last 10 minutes
        - If no recent runs but active DAGs exist, status = "degraded"
        - If no recent runs and no active DAGs, status = "healthy"
        - If recent runs exist, status = "healthy"

        Returns:
            dict with keys: check, status, recent_runs, message
        """
        from airflow.models import DagRun, DagModel
        from airflow.settings import Session

        result = {
            "check": "scheduler",
            "status": "unknown",
            "recent_runs": 0,
        }

        try:
            session = Session()

            # TODO: Query for recent DAG runs (last 10 minutes)
            # Hint: recent_threshold = datetime.utcnow() - timedelta(minutes=10)
            #       session.query(DagRun).filter(DagRun.start_date > recent_threshold).count()

            # TODO: If no recent runs, check if there are active unpaused DAGs

            # TODO: Set status and message appropriately

            session.close()

        except Exception as e:
            result["status"] = "unhealthy"
            result["message"] = str(e)

        return result

    @task
    def aggregate_results(
        db_result: dict,
        conn_result: dict,
        var_result: dict,
        scheduler_result: dict,
    ) -> dict:
        """
        Aggregate all health check results and determine overall status.

        TODO: Implement aggregation logic
        - Collect all statuses
        - Determine overall status:
          - Any "unhealthy" -> overall "unhealthy"
          - Any "degraded" -> overall "degraded"
          - All "healthy" -> overall "healthy"
        - Create summary dict
        - Raise AirflowException if unhealthy

        Returns:
            dict with keys: timestamp, overall_status, checks, details
        """
        all_results = [db_result, conn_result, var_result, scheduler_result]

        # TODO: Extract statuses from all results
        # Hint: statuses = [r.get("status", "unknown") for r in all_results]

        # TODO: Determine overall_status based on priority
        # "unhealthy" > "degraded" > "healthy"

        # TODO: Build summary dict

        summary = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": "unknown",  # TODO: Set this
            "checks": {
                "database": db_result["status"],
                "connections": conn_result["status"],
                "variables": var_result["status"],
                "scheduler": scheduler_result["status"],
            },
        }

        # TODO: Log summary

        # TODO: Raise AirflowException if overall_status is "unhealthy"
        # This triggers the failure callback

        return summary

    # Execute all checks in parallel, then aggregate
    db = check_database()
    conns = check_connections()
    vars_check = check_variables()
    scheduler = check_scheduler_health()

    aggregate_results(db, conns, vars_check, scheduler)


# Instantiate the DAG
health_check()
