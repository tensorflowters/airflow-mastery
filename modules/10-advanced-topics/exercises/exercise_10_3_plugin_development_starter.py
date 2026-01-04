"""
Exercise 10.3: Plugin Development (Starter)
=============================================

Build an Airflow plugin with:
1. Custom view for task duration statistics
2. Custom Jinja macros for date formatting
3. Utility operator for Slack summaries

This file contains the skeleton - implement the TODO sections.

To use this plugin:
1. Place in $AIRFLOW_HOME/plugins/task_analytics/
2. Restart the Airflow webserver
3. Access at http://localhost:8080/task-analytics/
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from flask import Blueprint, render_template_string

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

if TYPE_CHECKING:
    from airflow.utils.context import Context


# =============================================================================
# PART 1: CUSTOM VIEW
# =============================================================================

# HTML template for the duration statistics view
DURATION_STATS_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Task Duration Statistics</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        h1 {
            color: #017cee;
        }
        .stats-card {
            background: white;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #017cee;
            color: white;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .metric {
            font-size: 24px;
            font-weight: bold;
            color: #017cee;
        }
        .label {
            color: #666;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Task Duration Statistics</h1>

        <div class="stats-card">
            <h2>Summary (Last {{ days_back }} Days)</h2>
            <div style="display: flex; gap: 40px;">
                <div>
                    <div class="metric">{{ total_tasks }}</div>
                    <div class="label">Total Tasks</div>
                </div>
                <div>
                    <div class="metric">{{ total_dags }}</div>
                    <div class="label">Active DAGs</div>
                </div>
                <div>
                    <div class="metric">{{ "%.1f"|format(avg_duration) }}s</div>
                    <div class="label">Avg Duration</div>
                </div>
            </div>
        </div>

        <div class="stats-card">
            <h2>Duration by DAG</h2>
            <table>
                <thead>
                    <tr>
                        <th>DAG ID</th>
                        <th>Avg Duration (s)</th>
                        <th>Task Count</th>
                        <th>Max Duration (s)</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in dag_stats %}
                    <tr>
                        <td>{{ row.dag_id }}</td>
                        <td>{{ "%.2f"|format(row.avg_duration or 0) }}</td>
                        <td>{{ row.task_count }}</td>
                        <td>{{ "%.2f"|format(row.max_duration or 0) }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <div class="stats-card">
            <h2>Slowest Tasks</h2>
            <table>
                <thead>
                    <tr>
                        <th>DAG ID</th>
                        <th>Task ID</th>
                        <th>Duration (s)</th>
                        <th>Execution Date</th>
                    </tr>
                </thead>
                <tbody>
                    {% for row in slowest_tasks %}
                    <tr>
                        <td>{{ row.dag_id }}</td>
                        <td>{{ row.task_id }}</td>
                        <td>{{ "%.2f"|format(row.duration or 0) }}</td>
                        <td>{{ row.execution_date }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        <p style="color: #999; font-size: 12px;">
            Generated: {{ generated_at }}
        </p>
    </div>
</body>
</html>
"""

# Create Flask Blueprint for the view
task_analytics_bp = Blueprint(
    "task_analytics",
    __name__,
    url_prefix="/task-analytics",
)


def get_duration_stats(days_back: int = 7) -> dict:
    """
    Query task instance statistics from the database.

    TODO: Implement this function to return:
    - dag_stats: List of dicts with dag_id, avg_duration, task_count, max_duration
    - slowest_tasks: List of dicts with dag_id, task_id, duration, execution_date
    - total_tasks: Total number of task instances
    - total_dags: Number of unique DAGs
    - avg_duration: Overall average duration

    Args:
        days_back: Number of days to look back

    Returns:
        Dictionary with all statistics
    """
    # TODO: Implement database queries
    # Hint: Use airflow.settings.Session and airflow.models.TaskInstance

    # Placeholder return - replace with actual implementation
    return {
        "dag_stats": [
            {"dag_id": "example_dag", "avg_duration": 10.5, "task_count": 100, "max_duration": 45.2},
        ],
        "slowest_tasks": [
            {"dag_id": "example_dag", "task_id": "slow_task", "duration": 45.2, "execution_date": "2024-01-15"},
        ],
        "total_tasks": 100,
        "total_dags": 5,
        "avg_duration": 10.5,
    }


@task_analytics_bp.route("/")
def duration_stats_view():
    """
    Render the task duration statistics page.

    TODO: Complete this view function.
    """
    days_back = 7  # Could be made configurable via query param

    # Get statistics
    stats = get_duration_stats(days_back)

    # Render template
    return render_template_string(
        DURATION_STATS_TEMPLATE,
        days_back=days_back,
        dag_stats=stats["dag_stats"],
        slowest_tasks=stats["slowest_tasks"],
        total_tasks=stats["total_tasks"],
        total_dags=stats["total_dags"],
        avg_duration=stats["avg_duration"],
        generated_at=datetime.utcnow().isoformat(),
    )


# =============================================================================
# PART 2: CUSTOM MACROS
# =============================================================================


def format_business_date(ds: str) -> str:
    """
    Format a date string as a business-friendly format.

    Example:
        {{ format_business_date(ds) }}
        "2024-01-15" -> "Monday, January 15, 2024"

    TODO: Implement this macro.

    Args:
        ds: Date string in YYYY-MM-DD format

    Returns:
        Formatted date string like "Monday, January 15, 2024"
    """
    # TODO: Implement using pendulum
    # Hint: pendulum.parse(ds).format("dddd, MMMM D, YYYY")
    pass


def days_until_month_end(ds: str) -> int:
    """
    Calculate the number of days until the end of the month.

    Example:
        {{ days_until_month_end(ds) }}
        "2024-01-15" -> 16

    TODO: Implement this macro.

    Args:
        ds: Date string in YYYY-MM-DD format

    Returns:
        Number of days remaining in the month
    """
    # TODO: Implement using pendulum
    # Hint: Use .end_of("month") to get last day
    pass


def is_business_day(ds: str) -> bool:
    """
    Check if a date is a business day (Monday-Friday).

    Example:
        {% if is_business_day(ds) %}
            Business day processing
        {% endif %}

    TODO: Implement this macro.

    Args:
        ds: Date string in YYYY-MM-DD format

    Returns:
        True if Monday-Friday, False otherwise
    """
    # TODO: Implement using pendulum
    # Hint: weekday() returns 0-6 where 0=Monday
    pass


def quarter_of_year(ds: str) -> int:
    """
    Get the quarter of the year (1-4).

    Example:
        {{ quarter_of_year(ds) }}
        "2024-01-15" -> 1
        "2024-04-15" -> 2

    TODO: Implement this macro.

    Args:
        ds: Date string in YYYY-MM-DD format

    Returns:
        Quarter number (1-4)
    """
    # TODO: Implement
    # Hint: (month - 1) // 3 + 1
    pass


# =============================================================================
# PART 3: CUSTOM OPERATOR
# =============================================================================


class SlackSummaryOperator(BaseOperator):
    """
    Operator that collects DAG run statistics and sends a summary to Slack.

    This operator:
    1. Queries recent DAG runs
    2. Calculates success/failure rates
    3. Formats a summary message
    4. Sends to a Slack webhook

    TODO: Implement all methods marked with TODO.

    Example usage:
        SlackSummaryOperator(
            task_id="send_daily_summary",
            webhook_url="https://hooks.slack.com/services/...",
            dag_ids=["etl_pipeline", "reporting_dag"],
            lookback_hours=24,
        )
    """

    # Fields that support Jinja templating
    template_fields = ("webhook_url", "message_prefix")

    def __init__(
        self,
        *,
        webhook_url: str,
        dag_ids: list[str] | None = None,
        lookback_hours: int = 24,
        message_prefix: str = "",
        include_task_details: bool = False,
        **kwargs,
    ):
        """
        Initialize the SlackSummaryOperator.

        Args:
            webhook_url: Slack webhook URL for sending messages
            dag_ids: Optional list of DAG IDs to include (None = all DAGs)
            lookback_hours: How many hours back to collect statistics
            message_prefix: Optional prefix for the message
            include_task_details: Whether to include task-level details
        """
        super().__init__(**kwargs)
        self.webhook_url = webhook_url
        self.dag_ids = dag_ids
        self.lookback_hours = lookback_hours
        self.message_prefix = message_prefix
        self.include_task_details = include_task_details

    def _collect_stats(self) -> dict[str, Any]:
        """
        Collect DAG run statistics from the database.

        TODO: Implement this method to query:
        - Total DAG runs in the lookback period
        - Successful runs count
        - Failed runs count
        - Running runs count
        - Average duration
        - Per-DAG breakdown

        Returns:
            Dictionary with collected statistics
        """
        # TODO: Implement database queries
        # Hint: Use airflow.models.DagRun and filter by execution_date

        # Placeholder return
        return {
            "total_runs": 0,
            "successful": 0,
            "failed": 0,
            "running": 0,
            "success_rate": 0.0,
            "avg_duration_seconds": 0,
            "dag_breakdown": [],
        }

    def _format_message(self, stats: dict[str, Any]) -> str:
        """
        Format statistics into a Slack message.

        TODO: Implement this method to create a well-formatted message.

        Args:
            stats: Dictionary from _collect_stats()

        Returns:
            Formatted message string with Slack mrkdwn
        """
        # TODO: Implement message formatting
        # Hint: Use Slack mrkdwn syntax for formatting
        # *bold*, _italic_, `code`, ```code block```

        message = f"""
{self.message_prefix}
*Airflow DAG Summary* (Last {self.lookback_hours} hours)

📊 *Overview*
• Total Runs: {stats['total_runs']}
• ✅ Successful: {stats['successful']}
• ❌ Failed: {stats['failed']}
• 🔄 Running: {stats['running']}
• Success Rate: {stats['success_rate']:.1f}%

⏱️ *Average Duration*: {stats['avg_duration_seconds']:.1f}s
"""
        return message.strip()

    def _send_to_slack(self, message: str) -> dict:
        """
        Send message to Slack webhook.

        TODO: Implement this method.

        Args:
            message: Formatted message to send

        Returns:
            Response from Slack API
        """
        # TODO: Implement Slack webhook call
        # Hint: Use requests library to POST to webhook_url
        # For the exercise, you can mock this

        import logging

        logger = logging.getLogger(__name__)
        logger.info(f"Would send to Slack: {message}")

        # Mock response for exercise
        return {"ok": True, "message": "Mock send successful"}

    def execute(self, context: Context) -> dict[str, Any]:
        """
        Execute the operator.

        This is the main method called by Airflow.

        Args:
            context: Airflow task context

        Returns:
            Statistics dictionary for XCom
        """
        import logging

        logger = logging.getLogger(__name__)

        logger.info(f"Collecting stats for last {self.lookback_hours} hours")
        if self.dag_ids:
            logger.info(f"Filtering to DAGs: {self.dag_ids}")

        # Collect statistics
        stats = self._collect_stats()
        logger.info(f"Collected stats: {stats}")

        # Format message
        message = self._format_message(stats)
        logger.info(f"Formatted message:\n{message}")

        # Send to Slack
        response = self._send_to_slack(message)
        logger.info(f"Slack response: {response}")

        # Return stats for XCom
        return stats


# =============================================================================
# PLUGIN REGISTRATION
# =============================================================================


class TaskAnalyticsPlugin(AirflowPlugin):
    """
    Airflow Plugin for Task Analytics.

    This plugin provides:
    - A custom view for task duration statistics
    - Custom Jinja macros for date formatting
    - A Slack summary operator

    TODO: Complete the plugin registration.
    """

    # Plugin name (must be unique)
    name = "task_analytics"

    # Flask blueprints for custom views
    flask_blueprints = [task_analytics_bp]

    # Custom operators (available via airflow.operators.task_analytics)
    operators = [SlackSummaryOperator]

    # Custom hooks (none for this exercise)
    hooks = []

    # Jinja template macros
    # TODO: Add your macro functions here
    macros = [
        # format_business_date,
        # days_until_month_end,
        # is_business_day,
        # quarter_of_year,
    ]

    # Menu links in Airflow UI
    appbuilder_menu_items = [
        {
            "name": "Task Analytics",
            "category": "Analytics",
            "category_icon": "fa-chart-bar",
            "href": "/task-analytics/",
        }
    ]


# =============================================================================
# TESTING
# =============================================================================


def test_macros():
    """Test the custom macros."""
    print("Testing custom macros...")

    # Test format_business_date
    result = format_business_date("2024-01-15")
    print(f"format_business_date('2024-01-15') = {result}")
    # Expected: "Monday, January 15, 2024"

    # Test days_until_month_end
    result = days_until_month_end("2024-01-15")
    print(f"days_until_month_end('2024-01-15') = {result}")
    # Expected: 16

    # Test is_business_day
    result = is_business_day("2024-01-15")  # Monday
    print(f"is_business_day('2024-01-15') = {result}")
    # Expected: True

    result = is_business_day("2024-01-13")  # Saturday
    print(f"is_business_day('2024-01-13') = {result}")
    # Expected: False

    # Test quarter_of_year
    for month in [1, 4, 7, 10]:
        ds = f"2024-{month:02d}-15"
        result = quarter_of_year(ds)
        print(f"quarter_of_year('{ds}') = {result}")

    print("Macro testing complete!")


def test_operator():
    """Test the SlackSummaryOperator."""
    print("\nTesting SlackSummaryOperator...")

    op = SlackSummaryOperator(
        task_id="test_summary",
        webhook_url="https://hooks.slack.com/test",
        dag_ids=["example_dag"],
        lookback_hours=24,
        message_prefix="🤖 *Test Message*",
    )

    # Test stat collection
    stats = op._collect_stats()
    print(f"Collected stats: {stats}")

    # Test message formatting
    message = op._format_message(stats)
    print(f"Formatted message:\n{message}")

    print("Operator testing complete!")


if __name__ == "__main__":
    test_macros()
    test_operator()
