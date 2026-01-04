# Exercise 10.3: Plugin Development

## Objective

Build an Airflow plugin that extends the platform with custom views, macros, and operators to demonstrate the plugin architecture.

## Background

Airflow's plugin system allows extending the platform with:
- **Custom Views**: Flask blueprints adding web UI pages
- **Custom Operators**: Reusable task types
- **Custom Hooks**: Connection handlers
- **Custom Macros**: Jinja template functions
- **Menu Links**: Navigation items in the Airflow UI

### Plugin Architecture

```python
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

class MyPlugin(AirflowPlugin):
    name = "my_plugin"

    # Flask blueprints for custom views
    flask_blueprints = [my_blueprint]

    # Custom operators
    operators = [MyOperator]

    # Custom hooks
    hooks = [MyHook]

    # Jinja template macros
    macros = [my_macro_function]

    # Menu items
    appbuilder_menu_items = [
        {
            "name": "My View",
            "category": "Custom",
            "href": "/myview",
        }
    ]
```

## Requirements

Create a plugin with three components:

### Part 1: Task Duration Statistics View

Build a custom view that displays:
1. Average task duration by DAG
2. Slowest tasks across all DAGs
3. Duration trends over time
4. Filterable by date range

### Part 2: Custom Date Formatting Macro

Create a Jinja macro for date formatting:
1. `{{ format_business_date(ds) }}` → "Monday, January 15, 2024"
2. `{{ days_until_month_end(ds) }}` → Number of days remaining
3. `{{ is_business_day(ds) }}` → True/False

### Part 3: Utility Operator

Build a `SlackSummaryOperator` that:
1. Collects DAG run statistics
2. Formats a summary message
3. Sends to a Slack webhook (mock for exercise)

## Starter Code

See `exercise_10_3_plugin_development_starter.py`

## Expected Plugin Structure

```
plugins/
└── task_analytics/
    ├── __init__.py
    ├── views/
    │   ├── __init__.py
    │   └── duration_view.py
    ├── operators/
    │   ├── __init__.py
    │   └── slack_summary.py
    ├── macros/
    │   ├── __init__.py
    │   └── date_macros.py
    └── plugin.py
```

## Hints

<details>
<summary>Hint 1: Flask Blueprint for Views</summary>

```python
from flask import Blueprint, render_template_string
from airflow.www.app import csrf

bp = Blueprint(
    "task_analytics",
    __name__,
    template_folder="templates",
    static_folder="static",
    url_prefix="/task-analytics",
)

@bp.route("/")
@csrf.exempt  # For GET requests
def duration_stats():
    # Query task instances
    from airflow.models import TaskInstance
    from airflow.settings import Session

    session = Session()
    # Your query logic here
    session.close()

    return render_template_string(TEMPLATE, data=data)
```

</details>

<details>
<summary>Hint 2: Querying Task Duration Statistics</summary>

```python
from sqlalchemy import func
from airflow.models import TaskInstance, DagRun
from airflow.utils.state import State

def get_duration_stats(days_back=7):
    from airflow.settings import Session
    from datetime import datetime, timedelta

    session = Session()
    cutoff = datetime.utcnow() - timedelta(days=days_back)

    # Average duration by DAG
    results = (
        session.query(
            TaskInstance.dag_id,
            func.avg(TaskInstance.duration).label("avg_duration"),
            func.count(TaskInstance.task_id).label("task_count"),
        )
        .filter(TaskInstance.start_date > cutoff)
        .filter(TaskInstance.state == State.SUCCESS)
        .group_by(TaskInstance.dag_id)
        .all()
    )

    session.close()
    return results
```

</details>

<details>
<summary>Hint 3: Custom Macro Registration</summary>

```python
from pendulum import parse

def format_business_date(ds: str) -> str:
    """Format date as business-friendly string."""
    dt = parse(ds)
    return dt.format("dddd, MMMM D, YYYY")

def days_until_month_end(ds: str) -> int:
    """Calculate days remaining in month."""
    dt = parse(ds)
    end_of_month = dt.end_of("month")
    return (end_of_month - dt).days

def is_business_day(ds: str) -> bool:
    """Check if date is a business day (Mon-Fri)."""
    dt = parse(ds)
    return dt.weekday() < 5  # 0-4 are Mon-Fri

# Register in plugin
class MyPlugin(AirflowPlugin):
    name = "my_plugin"
    macros = [format_business_date, days_until_month_end, is_business_day]
```

</details>

<details>
<summary>Hint 4: Custom Operator Structure</summary>

```python
from airflow.models import BaseOperator
from airflow.utils.context import Context

class SlackSummaryOperator(BaseOperator):
    template_fields = ("message", "webhook_url")

    def __init__(
        self,
        *,
        webhook_url: str,
        dag_ids: list[str] | None = None,
        lookback_hours: int = 24,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.webhook_url = webhook_url
        self.dag_ids = dag_ids
        self.lookback_hours = lookback_hours

    def execute(self, context: Context):
        stats = self._collect_stats()
        message = self._format_message(stats)
        self._send_to_slack(message)
        return stats

    def _collect_stats(self) -> dict:
        """Collect DAG run statistics."""
        # Query logic here
        pass
```

</details>

## Verification

### Test Your Plugin

```python
# Test macros
from your_plugin.macros.date_macros import (
    format_business_date,
    days_until_month_end,
    is_business_day,
)

assert format_business_date("2024-01-15") == "Monday, January 15, 2024"
assert is_business_day("2024-01-15") == True  # Monday
assert is_business_day("2024-01-13") == False  # Saturday
```

### Test View

```bash
# Start Airflow webserver and navigate to:
http://localhost:8080/task-analytics/
```

### Test Operator

```python
from your_plugin.operators.slack_summary import SlackSummaryOperator

op = SlackSummaryOperator(
    task_id="send_summary",
    webhook_url="https://hooks.slack.com/...",
    dag_ids=["my_dag"],
    lookback_hours=24,
)

# Dry run
stats = op._collect_stats()
print(op._format_message(stats))
```

## Success Criteria

- [ ] Plugin loads without errors
- [ ] Custom view accessible at `/task-analytics/`
- [ ] View displays task duration statistics
- [ ] All three macros work correctly
- [ ] `SlackSummaryOperator` collects and formats stats
- [ ] Plugin appears in Airflow plugin list
- [ ] Menu item appears in Airflow UI
- [ ] Code follows Airflow plugin best practices

## Bonus Challenges

1. **Add Charts**: Use Chart.js or similar to visualize duration trends
2. **Add Filters**: Allow filtering by DAG, date range, task state
3. **Export Feature**: Add CSV/JSON export for statistics
4. **Real Slack Integration**: Connect to actual Slack webhook
5. **Unit Tests**: Write pytest tests for all plugin components

---

## References

- [Airflow Plugins Documentation](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/plugins.html)
- [Flask Blueprints](https://flask.palletsprojects.com/en/2.3.x/blueprints/)
- [Creating Custom Operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html)

---

Congratulations! You've completed the Advanced Topics module!
