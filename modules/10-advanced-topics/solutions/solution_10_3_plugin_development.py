"""
Solution 10.3: Plugin Development
==================================

A complete Airflow plugin demonstrating:
1. Custom Flask view for task duration statistics
2. Custom Jinja macros for date operations
3. Custom operator for Slack summaries

This plugin showcases Airflow's extensibility architecture
and best practices for plugin development.

Installation:
    Copy to $AIRFLOW_HOME/plugins/task_analytics/
    Restart webserver to load the plugin
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

import pendulum
from flask import Blueprint, render_template_string, request
from sqlalchemy import func

from airflow.models import BaseOperator, DagRun, TaskInstance
from airflow.plugins_manager import AirflowPlugin
from airflow.settings import Session
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = logging.getLogger(__name__)


# =============================================================================
# PART 1: CUSTOM VIEW - Task Duration Statistics
# =============================================================================

DURATION_STATS_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Task Duration Statistics - Airflow Analytics</title>
    <style>
        :root {
            --primary-color: #017cee;
            --success-color: #00a352;
            --warning-color: #f0ad4e;
            --danger-color: #d9534f;
            --bg-color: #f5f7fa;
        }
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background-color: var(--bg-color);
            color: #333;
            line-height: 1.6;
        }
        .header {
            background: linear-gradient(135deg, var(--primary-color), #0056b3);
            color: white;
            padding: 20px 40px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .header h1 {
            font-size: 24px;
            font-weight: 600;
        }
        .header p {
            opacity: 0.9;
            font-size: 14px;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        .filters {
            background: white;
            padding: 15px 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            display: flex;
            gap: 20px;
            align-items: center;
        }
        .filters label {
            font-weight: 500;
            color: #666;
        }
        .filters select, .filters input {
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            font-size: 14px;
        }
        .filters button {
            background: var(--primary-color);
            color: white;
            border: none;
            padding: 8px 20px;
            border-radius: 4px;
            cursor: pointer;
            font-weight: 500;
        }
        .filters button:hover {
            background: #0056b3;
        }
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .metric-card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        .metric-value {
            font-size: 32px;
            font-weight: 700;
            color: var(--primary-color);
        }
        .metric-label {
            font-size: 14px;
            color: #666;
            margin-top: 5px;
        }
        .metric-card.success .metric-value { color: var(--success-color); }
        .metric-card.warning .metric-value { color: var(--warning-color); }
        .metric-card.danger .metric-value { color: var(--danger-color); }
        .stats-card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
        }
        .stats-card h2 {
            font-size: 18px;
            color: #333;
            margin-bottom: 15px;
            padding-bottom: 10px;
            border-bottom: 1px solid #eee;
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px 15px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }
        th {
            background-color: #f8f9fa;
            font-weight: 600;
            color: #555;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        tr:hover {
            background-color: #f8f9fa;
        }
        .duration-bar {
            background: #e9ecef;
            border-radius: 4px;
            height: 8px;
            overflow: hidden;
        }
        .duration-bar-fill {
            background: var(--primary-color);
            height: 100%;
            border-radius: 4px;
        }
        .badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-weight: 500;
        }
        .badge-success { background: #d4edda; color: #155724; }
        .badge-warning { background: #fff3cd; color: #856404; }
        .badge-danger { background: #f8d7da; color: #721c24; }
        .footer {
            text-align: center;
            padding: 20px;
            color: #999;
            font-size: 12px;
        }
        .grid-2 {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 20px;
        }
        @media (max-width: 768px) {
            .grid-2 { grid-template-columns: 1fr; }
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Task Duration Statistics</h1>
        <p>Performance insights for your Airflow workflows</p>
    </div>

    <div class="container">
        <div class="filters">
            <form method="get" style="display: flex; gap: 20px; align-items: center; width: 100%;">
                <div>
                    <label>Time Range:</label>
                    <select name="days_back">
                        <option value="1" {{ 'selected' if days_back == 1 else '' }}>Last 24 hours</option>
                        <option value="7" {{ 'selected' if days_back == 7 else '' }}>Last 7 days</option>
                        <option value="30" {{ 'selected' if days_back == 30 else '' }}>Last 30 days</option>
                        <option value="90" {{ 'selected' if days_back == 90 else '' }}>Last 90 days</option>
                    </select>
                </div>
                <button type="submit">Apply Filter</button>
            </form>
        </div>

        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-value">{{ total_tasks }}</div>
                <div class="metric-label">Total Task Executions</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ total_dags }}</div>
                <div class="metric-label">Active DAGs</div>
            </div>
            <div class="metric-card success">
                <div class="metric-value">{{ "%.1f"|format(success_rate) }}%</div>
                <div class="metric-label">Success Rate</div>
            </div>
            <div class="metric-card">
                <div class="metric-value">{{ "%.1f"|format(avg_duration) }}s</div>
                <div class="metric-label">Avg Duration</div>
            </div>
        </div>

        <div class="grid-2">
            <div class="stats-card">
                <h2>📈 Duration by DAG</h2>
                <table>
                    <thead>
                        <tr>
                            <th>DAG ID</th>
                            <th>Avg Duration</th>
                            <th>Tasks</th>
                            <th style="width: 30%;">Distribution</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in dag_stats %}
                        <tr>
                            <td><strong>{{ row.dag_id }}</strong></td>
                            <td>{{ "%.2f"|format(row.avg_duration or 0) }}s</td>
                            <td>{{ row.task_count }}</td>
                            <td>
                                <div class="duration-bar">
                                    <div class="duration-bar-fill" style="width: {{ (row.avg_duration / max_duration * 100) if max_duration > 0 else 0 }}%"></div>
                                </div>
                            </td>
                        </tr>
                        {% endfor %}
                        {% if not dag_stats %}
                        <tr><td colspan="4" style="text-align: center; color: #999;">No data available</td></tr>
                        {% endif %}
                    </tbody>
                </table>
            </div>

            <div class="stats-card">
                <h2>🐢 Slowest Tasks</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Task</th>
                            <th>DAG</th>
                            <th>Duration</th>
                            <th>Date</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in slowest_tasks %}
                        <tr>
                            <td><strong>{{ row.task_id }}</strong></td>
                            <td>{{ row.dag_id }}</td>
                            <td>
                                <span class="badge {% if row.duration > 300 %}badge-danger{% elif row.duration > 60 %}badge-warning{% else %}badge-success{% endif %}">
                                    {{ "%.1f"|format(row.duration or 0) }}s
                                </span>
                            </td>
                            <td>{{ row.execution_date }}</td>
                        </tr>
                        {% endfor %}
                        {% if not slowest_tasks %}
                        <tr><td colspan="4" style="text-align: center; color: #999;">No data available</td></tr>
                        {% endif %}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="stats-card">
            <h2>📋 Task State Distribution</h2>
            <table>
                <thead>
                    <tr>
                        <th>State</th>
                        <th>Count</th>
                        <th>Percentage</th>
                        <th style="width: 50%;">Distribution</th>
                    </tr>
                </thead>
                <tbody>
                    {% for state, count in state_distribution.items() %}
                    <tr>
                        <td>
                            <span class="badge {% if state == 'success' %}badge-success{% elif state == 'failed' %}badge-danger{% else %}badge-warning{% endif %}">
                                {{ state }}
                            </span>
                        </td>
                        <td>{{ count }}</td>
                        <td>{{ "%.1f"|format(count / total_tasks * 100 if total_tasks > 0 else 0) }}%</td>
                        <td>
                            <div class="duration-bar">
                                <div class="duration-bar-fill" style="width: {{ count / total_tasks * 100 if total_tasks > 0 else 0 }}%; background: {% if state == 'success' %}var(--success-color){% elif state == 'failed' %}var(--danger-color){% else %}var(--warning-color){% endif %}"></div>
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>

    <div class="footer">
        <p>Task Analytics Plugin | Generated: {{ generated_at }}</p>
    </div>
</body>
</html>
"""

# Create Flask Blueprint
task_analytics_bp = Blueprint(
    "task_analytics",
    __name__,
    url_prefix="/task-analytics",
)


def get_duration_stats(days_back: int = 7) -> dict:
    """
    Query comprehensive task instance statistics from the database.

    This function performs several database queries to collect:
    - Per-DAG duration statistics
    - Slowest individual task executions
    - Task state distribution
    - Overall metrics

    Args:
        days_back: Number of days to look back

    Returns:
        Dictionary containing all statistics for the view
    """
    session = Session()
    cutoff = datetime.utcnow() - timedelta(days=days_back)

    try:
        # Query 1: Average duration by DAG
        dag_stats_query = (
            session.query(
                TaskInstance.dag_id,
                func.avg(TaskInstance.duration).label("avg_duration"),
                func.max(TaskInstance.duration).label("max_duration"),
                func.count(TaskInstance.task_id).label("task_count"),
            )
            .filter(TaskInstance.start_date > cutoff)
            .filter(TaskInstance.duration.isnot(None))
            .filter(TaskInstance.state == TaskInstanceState.SUCCESS)
            .group_by(TaskInstance.dag_id)
            .order_by(func.avg(TaskInstance.duration).desc())
            .limit(20)
            .all()
        )

        dag_stats = [
            {
                "dag_id": row.dag_id,
                "avg_duration": float(row.avg_duration) if row.avg_duration else 0,
                "max_duration": float(row.max_duration) if row.max_duration else 0,
                "task_count": row.task_count,
            }
            for row in dag_stats_query
        ]

        # Query 2: Slowest individual tasks
        slowest_tasks_query = (
            session.query(
                TaskInstance.dag_id,
                TaskInstance.task_id,
                TaskInstance.duration,
                TaskInstance.start_date,
            )
            .filter(TaskInstance.start_date > cutoff)
            .filter(TaskInstance.duration.isnot(None))
            .filter(TaskInstance.state == TaskInstanceState.SUCCESS)
            .order_by(TaskInstance.duration.desc())
            .limit(10)
            .all()
        )

        slowest_tasks = [
            {
                "dag_id": row.dag_id,
                "task_id": row.task_id,
                "duration": float(row.duration) if row.duration else 0,
                "execution_date": row.start_date.strftime("%Y-%m-%d %H:%M") if row.start_date else "N/A",
            }
            for row in slowest_tasks_query
        ]

        # Query 3: Task state distribution
        state_counts_query = (
            session.query(
                TaskInstance.state,
                func.count(TaskInstance.task_id).label("count"),
            )
            .filter(TaskInstance.start_date > cutoff)
            .group_by(TaskInstance.state)
            .all()
        )

        state_distribution = {
            str(row.state) if row.state else "unknown": row.count
            for row in state_counts_query
        }

        # Query 4: Overall statistics
        total_tasks = sum(state_distribution.values())
        total_dags = len(dag_stats)
        successful_tasks = state_distribution.get("success", 0)
        success_rate = (successful_tasks / total_tasks * 100) if total_tasks > 0 else 0

        # Calculate overall average duration
        avg_duration_query = (
            session.query(func.avg(TaskInstance.duration))
            .filter(TaskInstance.start_date > cutoff)
            .filter(TaskInstance.duration.isnot(None))
            .filter(TaskInstance.state == TaskInstanceState.SUCCESS)
            .scalar()
        )
        avg_duration = float(avg_duration_query) if avg_duration_query else 0

        # Find max duration for bar chart scaling
        max_duration = max((d["avg_duration"] for d in dag_stats), default=1)

        return {
            "dag_stats": dag_stats,
            "slowest_tasks": slowest_tasks,
            "state_distribution": state_distribution,
            "total_tasks": total_tasks,
            "total_dags": total_dags,
            "success_rate": success_rate,
            "avg_duration": avg_duration,
            "max_duration": max_duration,
        }

    except Exception as e:
        logger.error(f"Error querying duration stats: {e}")
        return {
            "dag_stats": [],
            "slowest_tasks": [],
            "state_distribution": {},
            "total_tasks": 0,
            "total_dags": 0,
            "success_rate": 0,
            "avg_duration": 0,
            "max_duration": 1,
        }
    finally:
        session.close()


@task_analytics_bp.route("/")
def duration_stats_view():
    """
    Render the task duration statistics dashboard.

    Supports query parameter 'days_back' for filtering time range.
    """
    # Get filter from query params
    days_back = request.args.get("days_back", default=7, type=int)
    days_back = max(1, min(365, days_back))  # Clamp to reasonable range

    # Get statistics
    stats = get_duration_stats(days_back)

    # Render template
    return render_template_string(
        DURATION_STATS_TEMPLATE,
        days_back=days_back,
        **stats,
        generated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
    )


@task_analytics_bp.route("/api/stats")
def stats_api():
    """
    JSON API endpoint for task statistics.

    Useful for integration with external dashboards.
    """
    from flask import jsonify

    days_back = request.args.get("days_back", default=7, type=int)
    stats = get_duration_stats(days_back)
    stats["generated_at"] = datetime.utcnow().isoformat()
    return jsonify(stats)


# =============================================================================
# PART 2: CUSTOM MACROS
# =============================================================================


def format_business_date(ds: str) -> str:
    """
    Format a date string as a business-friendly format.

    This macro can be used in templated fields to format dates
    for human-readable reports or notifications.

    Args:
        ds: Date string in YYYY-MM-DD format (Airflow's standard format)

    Returns:
        Formatted string like "Monday, January 15, 2024"

    Example Usage in DAG:
        BashOperator(
            task_id="print_date",
            bash_command="echo 'Report for {{ format_business_date(ds) }}'",
        )
    """
    try:
        dt = pendulum.parse(ds)
        return dt.format("dddd, MMMM D, YYYY")
    except Exception as e:
        logger.warning(f"Error formatting date {ds}: {e}")
        return ds


def days_until_month_end(ds: str) -> int:
    """
    Calculate the number of days until the end of the month.

    Useful for conditional logic in DAGs that need to behave
    differently near month-end (e.g., closing processes).

    Args:
        ds: Date string in YYYY-MM-DD format

    Returns:
        Number of days remaining in the month (0 on last day)

    Example Usage in DAG:
        @task.branch
        def check_month_end(**context):
            days_left = days_until_month_end(context['ds'])
            if days_left <= 2:
                return 'month_end_processing'
            return 'normal_processing'
    """
    try:
        dt = pendulum.parse(ds)
        end_of_month = dt.end_of("month")
        return (end_of_month - dt).days
    except Exception as e:
        logger.warning(f"Error calculating days until month end for {ds}: {e}")
        return 0


def is_business_day(ds: str) -> bool:
    """
    Check if a date is a business day (Monday-Friday).

    Does not account for holidays - use with holiday calendar
    for complete business day logic.

    Args:
        ds: Date string in YYYY-MM-DD format

    Returns:
        True if Monday-Friday, False for Saturday/Sunday

    Example Usage in DAG:
        {% if is_business_day(ds) %}
            RUN business day logic
        {% endif %}
    """
    try:
        dt = pendulum.parse(ds)
        # weekday() returns 0=Monday, 6=Sunday
        return dt.weekday() < 5
    except Exception as e:
        logger.warning(f"Error checking business day for {ds}: {e}")
        return True  # Default to treating as business day


def quarter_of_year(ds: str) -> int:
    """
    Get the quarter of the year (1-4) for a given date.

    Args:
        ds: Date string in YYYY-MM-DD format

    Returns:
        Quarter number: 1 (Jan-Mar), 2 (Apr-Jun), 3 (Jul-Sep), 4 (Oct-Dec)

    Example Usage in DAG:
        PythonOperator(
            task_id="quarterly_report",
            python_callable=generate_report,
            op_kwargs={"quarter": "{{ quarter_of_year(ds) }}"},
        )
    """
    try:
        dt = pendulum.parse(ds)
        return (dt.month - 1) // 3 + 1
    except Exception as e:
        logger.warning(f"Error calculating quarter for {ds}: {e}")
        return 1


def fiscal_year(ds: str, fiscal_start_month: int = 7) -> int:
    """
    Get the fiscal year for a given date.

    Many organizations have fiscal years that don't align with
    calendar years. This macro handles that conversion.

    Args:
        ds: Date string in YYYY-MM-DD format
        fiscal_start_month: Month when fiscal year starts (default: July=7)

    Returns:
        Fiscal year number

    Example:
        If fiscal year starts in July:
        - June 2024 → FY 2024
        - July 2024 → FY 2025
    """
    try:
        dt = pendulum.parse(ds)
        if dt.month >= fiscal_start_month:
            return dt.year + 1
        return dt.year
    except Exception as e:
        logger.warning(f"Error calculating fiscal year for {ds}: {e}")
        return datetime.now().year


def format_duration(seconds: float) -> str:
    """
    Format a duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string like "2h 30m 15s" or "45s"

    Example Usage in DAG:
        {{ format_duration(task_instance.duration) }}
    """
    try:
        seconds = float(seconds)
        if seconds < 60:
            return f"{seconds:.1f}s"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}m {secs}s"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours}h {minutes}m"
    except Exception as e:
        logger.warning(f"Error formatting duration {seconds}: {e}")
        return str(seconds)


# =============================================================================
# PART 3: CUSTOM OPERATOR
# =============================================================================


class SlackSummaryOperator(BaseOperator):
    """
    Operator that collects DAG run statistics and sends a summary to Slack.

    This operator demonstrates:
    - Custom operator development patterns
    - Database querying for statistics
    - External API integration (Slack webhooks)
    - Template field support

    Attributes:
        template_fields: Fields that support Jinja templating
        template_ext: File extensions for template files
        ui_color: Color displayed in Airflow UI
        ui_fgcolor: Text color in Airflow UI

    Example:
        SlackSummaryOperator(
            task_id="daily_summary",
            webhook_url="{{ var.value.slack_webhook }}",
            dag_ids=["etl_pipeline", "reporting"],
            lookback_hours=24,
        )
    """

    template_fields = ("webhook_url", "message_prefix", "dag_ids")
    template_ext = ()
    ui_color = "#FFBA00"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        *,
        webhook_url: str,
        dag_ids: list[str] | None = None,
        lookback_hours: int = 24,
        message_prefix: str = "",
        include_task_details: bool = False,
        dry_run: bool = False,
        **kwargs,
    ):
        """
        Initialize the SlackSummaryOperator.

        Args:
            webhook_url: Slack incoming webhook URL
            dag_ids: List of DAG IDs to include (None = all DAGs)
            lookback_hours: Hours to look back for statistics
            message_prefix: Optional prefix for the message
            include_task_details: Include task-level statistics
            dry_run: If True, log message but don't send to Slack
        """
        super().__init__(**kwargs)
        self.webhook_url = webhook_url
        self.dag_ids = dag_ids
        self.lookback_hours = lookback_hours
        self.message_prefix = message_prefix
        self.include_task_details = include_task_details
        self.dry_run = dry_run

    def _collect_stats(self) -> dict[str, Any]:
        """
        Collect DAG run statistics from the Airflow database.

        Queries the DagRun table to gather:
        - Total runs in the lookback period
        - Success/failure/running counts
        - Average duration
        - Per-DAG breakdown

        Returns:
            Dictionary with all collected statistics
        """
        session = Session()
        cutoff = datetime.utcnow() - timedelta(hours=self.lookback_hours)

        try:
            # Base query for DAG runs
            base_query = session.query(DagRun).filter(
                DagRun.execution_date > cutoff
            )

            # Filter to specific DAGs if provided
            if self.dag_ids:
                base_query = base_query.filter(DagRun.dag_id.in_(self.dag_ids))

            # Get all runs
            all_runs = base_query.all()

            # Calculate statistics
            total_runs = len(all_runs)
            successful = sum(1 for r in all_runs if r.state == DagRunState.SUCCESS)
            failed = sum(1 for r in all_runs if r.state == DagRunState.FAILED)
            running = sum(1 for r in all_runs if r.state == DagRunState.RUNNING)

            success_rate = (successful / total_runs * 100) if total_runs > 0 else 0

            # Calculate average duration for completed runs
            completed_runs = [
                r for r in all_runs
                if r.state in (DagRunState.SUCCESS, DagRunState.FAILED)
                and r.start_date and r.end_date
            ]

            if completed_runs:
                total_duration = sum(
                    (r.end_date - r.start_date).total_seconds()
                    for r in completed_runs
                )
                avg_duration = total_duration / len(completed_runs)
            else:
                avg_duration = 0

            # Per-DAG breakdown
            dag_breakdown = {}
            for run in all_runs:
                if run.dag_id not in dag_breakdown:
                    dag_breakdown[run.dag_id] = {
                        "total": 0,
                        "success": 0,
                        "failed": 0,
                    }
                dag_breakdown[run.dag_id]["total"] += 1
                if run.state == DagRunState.SUCCESS:
                    dag_breakdown[run.dag_id]["success"] += 1
                elif run.state == DagRunState.FAILED:
                    dag_breakdown[run.dag_id]["failed"] += 1

            return {
                "total_runs": total_runs,
                "successful": successful,
                "failed": failed,
                "running": running,
                "success_rate": success_rate,
                "avg_duration_seconds": avg_duration,
                "dag_breakdown": [
                    {"dag_id": dag_id, **stats}
                    for dag_id, stats in dag_breakdown.items()
                ],
                "lookback_hours": self.lookback_hours,
                "collection_time": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"Error collecting stats: {e}")
            raise
        finally:
            session.close()

    def _format_message(self, stats: dict[str, Any]) -> str:
        """
        Format statistics into a Slack message using mrkdwn.

        Creates a well-structured message with sections for:
        - Overview metrics
        - Per-DAG breakdown
        - Status indicators

        Args:
            stats: Statistics dictionary from _collect_stats()

        Returns:
            Formatted Slack message string
        """
        # Status emoji based on success rate
        if stats["success_rate"] >= 95:
            status_emoji = "✅"
            status_text = "Healthy"
        elif stats["success_rate"] >= 80:
            status_emoji = "⚠️"
            status_text = "Degraded"
        else:
            status_emoji = "🚨"
            status_text = "Critical"

        # Format duration
        duration_str = format_duration(stats["avg_duration_seconds"])

        # Build message
        lines = []

        if self.message_prefix:
            lines.append(self.message_prefix)
            lines.append("")

        lines.extend([
            f"*{status_emoji} Airflow Status Report* | {status_text}",
            f"_Last {stats['lookback_hours']} hours_",
            "",
            "*📊 Overview*",
            f"• Total Runs: *{stats['total_runs']}*",
            f"• ✅ Successful: {stats['successful']}",
            f"• ❌ Failed: {stats['failed']}",
            f"• 🔄 Running: {stats['running']}",
            f"• Success Rate: *{stats['success_rate']:.1f}%*",
            f"• Avg Duration: {duration_str}",
        ])

        # Add per-DAG breakdown if there are failures
        if stats["failed"] > 0 and stats["dag_breakdown"]:
            lines.extend(["", "*📋 DAGs with Failures*"])
            for dag in stats["dag_breakdown"]:
                if dag["failed"] > 0:
                    lines.append(
                        f"• `{dag['dag_id']}`: {dag['failed']} failed / {dag['total']} total"
                    )

        # Timestamp
        lines.extend([
            "",
            f"_Generated: {stats['collection_time']}_",
        ])

        return "\n".join(lines)

    def _send_to_slack(self, message: str) -> dict:
        """
        Send message to Slack via incoming webhook.

        Args:
            message: Formatted message to send

        Returns:
            Response data from Slack API

        Raises:
            AirflowException: If Slack API returns an error
        """
        import json

        if self.dry_run:
            logger.info(f"DRY RUN - Would send to Slack:\n{message}")
            return {"ok": True, "dry_run": True}

        try:
            import requests

            payload = {
                "text": message,
                "mrkdwn": True,
            }

            response = requests.post(
                self.webhook_url,
                data=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                timeout=30,
            )

            if response.status_code != 200:
                raise Exception(
                    f"Slack API error: {response.status_code} - {response.text}"
                )

            logger.info("Successfully sent message to Slack")
            return {"ok": True, "status_code": response.status_code}

        except ImportError:
            logger.warning("requests library not available, using mock send")
            return {"ok": True, "mock": True}
        except Exception as e:
            logger.error(f"Error sending to Slack: {e}")
            raise

    def execute(self, context: Context) -> dict[str, Any]:
        """
        Execute the operator.

        This is the main entry point called by Airflow when
        the task runs.

        Args:
            context: Airflow task context with run information

        Returns:
            Statistics dictionary (pushed to XCom)
        """
        logger.info(
            f"Collecting Airflow stats for last {self.lookback_hours} hours"
        )
        if self.dag_ids:
            logger.info(f"Filtering to DAGs: {self.dag_ids}")

        # Collect statistics
        stats = self._collect_stats()
        logger.info(f"Collected stats: {stats['total_runs']} runs")

        # Format and send message
        message = self._format_message(stats)
        logger.info(f"Formatted message ({len(message)} chars)")

        response = self._send_to_slack(message)
        stats["slack_response"] = response

        return stats


# =============================================================================
# PLUGIN REGISTRATION
# =============================================================================


class TaskAnalyticsPlugin(AirflowPlugin):
    """
    Airflow Plugin for Task Analytics and Reporting.

    This plugin extends Airflow with:

    1. **Custom View** (`/task-analytics/`):
       Dashboard showing task duration statistics,
       slowest tasks, and state distribution.

    2. **Custom Macros**:
       - format_business_date: Human-friendly date format
       - days_until_month_end: Days remaining in month
       - is_business_day: Check if weekday
       - quarter_of_year: Get fiscal quarter
       - fiscal_year: Get fiscal year
       - format_duration: Human-readable duration

    3. **Custom Operator** (SlackSummaryOperator):
       Sends DAG run summaries to Slack webhooks.

    Usage:
        Place this file in $AIRFLOW_HOME/plugins/
        Restart webserver to load
    """

    name = "task_analytics"

    # Flask blueprints for custom views
    flask_blueprints = [task_analytics_bp]

    # Custom operators
    operators = [SlackSummaryOperator]

    # Custom hooks (none for this plugin)
    hooks = []

    # Jinja template macros
    macros = [
        format_business_date,
        days_until_month_end,
        is_business_day,
        quarter_of_year,
        fiscal_year,
        format_duration,
    ]

    # Menu items in Airflow UI
    appbuilder_menu_items = [
        {
            "name": "Task Analytics",
            "category": "Analytics",
            "category_icon": "fa-chart-bar",
            "href": "/task-analytics/",
        }
    ]


# =============================================================================
# EXAMPLE DAG USING THE PLUGIN
# =============================================================================


from airflow.sdk import dag, task


@dag(
    dag_id="solution_10_3_plugin_demo",
    schedule="0 8 * * *",  # Daily at 8 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "plugin", "demo"],
    doc_md="""
    ## Plugin Demo DAG

    Demonstrates usage of the Task Analytics plugin:
    - Custom macros in templates
    - SlackSummaryOperator for notifications
    """,
)
def plugin_demo():
    """Demo DAG showcasing the Task Analytics plugin."""

    @task
    def use_custom_macros(**context) -> dict:
        """
        Demonstrate custom macro usage.

        Note: Macros are available in templated fields.
        For Python code, call the functions directly.
        """
        ds = context["ds"]

        return {
            "business_date": format_business_date(ds),
            "days_to_month_end": days_until_month_end(ds),
            "is_business_day": is_business_day(ds),
            "quarter": quarter_of_year(ds),
            "fiscal_year": fiscal_year(ds),
        }

    @task
    def log_macro_results(results: dict) -> None:
        """Log the macro results."""
        logger.info("Custom Macro Results:")
        for key, value in results.items():
            logger.info(f"  {key}: {value}")

    # Example of using SlackSummaryOperator
    # (In production, use actual webhook URL from Variable)
    send_summary = SlackSummaryOperator(
        task_id="send_slack_summary",
        webhook_url="https://hooks.slack.com/services/FAKE/WEBHOOK/URL",
        dag_ids=["solution_10_3_plugin_demo"],
        lookback_hours=24,
        message_prefix="🤖 *Daily Airflow Report*",
        dry_run=True,  # Set to False with real webhook
    )

    # Build DAG
    macro_results = use_custom_macros()
    log_macro_results(macro_results) >> send_summary


# Instantiate
plugin_demo()


# =============================================================================
# TESTING
# =============================================================================


def test_macros():
    """Test all custom macros."""
    print("=" * 60)
    print("Testing Custom Macros")
    print("=" * 60)

    # Test format_business_date
    result = format_business_date("2024-01-15")
    print(f"format_business_date('2024-01-15') = '{result}'")
    assert "Monday" in result
    assert "January" in result
    assert "15" in result
    assert "2024" in result

    # Test days_until_month_end
    result = days_until_month_end("2024-01-15")
    print(f"days_until_month_end('2024-01-15') = {result}")
    assert result == 16  # Jan has 31 days, 31-15=16

    result = days_until_month_end("2024-01-31")
    print(f"days_until_month_end('2024-01-31') = {result}")
    assert result == 0  # Last day

    # Test is_business_day
    assert is_business_day("2024-01-15") == True  # Monday
    assert is_business_day("2024-01-13") == False  # Saturday
    assert is_business_day("2024-01-14") == False  # Sunday
    print("✓ is_business_day tests passed")

    # Test quarter_of_year
    assert quarter_of_year("2024-01-15") == 1
    assert quarter_of_year("2024-04-15") == 2
    assert quarter_of_year("2024-07-15") == 3
    assert quarter_of_year("2024-10-15") == 4
    print("✓ quarter_of_year tests passed")

    # Test fiscal_year (July start)
    assert fiscal_year("2024-06-30", 7) == 2024
    assert fiscal_year("2024-07-01", 7) == 2025
    print("✓ fiscal_year tests passed")

    # Test format_duration
    assert format_duration(45) == "45.0s"
    assert format_duration(90) == "1m 30s"
    assert format_duration(3700) == "1h 1m"
    print("✓ format_duration tests passed")

    print("\nAll macro tests passed! ✓")


def test_operator():
    """Test the SlackSummaryOperator."""
    print("\n" + "=" * 60)
    print("Testing SlackSummaryOperator")
    print("=" * 60)

    op = SlackSummaryOperator(
        task_id="test_summary",
        webhook_url="https://hooks.slack.com/test",
        lookback_hours=24,
        message_prefix="🧪 *Test Summary*",
        dry_run=True,
    )

    # Test stat collection (will use database if available)
    print("\nCollecting stats...")
    try:
        stats = op._collect_stats()
        print(f"✓ Stats collected: {stats['total_runs']} runs")
    except Exception as e:
        print(f"⚠ Stats collection failed (expected if no DB): {e}")
        stats = {
            "total_runs": 10,
            "successful": 8,
            "failed": 1,
            "running": 1,
            "success_rate": 80.0,
            "avg_duration_seconds": 120,
            "dag_breakdown": [
                {"dag_id": "test_dag", "total": 10, "success": 8, "failed": 1}
            ],
            "lookback_hours": 24,
            "collection_time": datetime.utcnow().isoformat(),
        }

    # Test message formatting
    print("\nFormatting message...")
    message = op._format_message(stats)
    print("Generated message:")
    print("-" * 40)
    print(message)
    print("-" * 40)

    # Test dry run
    print("\nTesting dry run send...")
    response = op._send_to_slack(message)
    print(f"Response: {response}")
    assert response.get("ok") == True
    assert response.get("dry_run") == True

    print("\nOperator tests passed! ✓")


if __name__ == "__main__":
    test_macros()
    test_operator()
