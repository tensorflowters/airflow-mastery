# Exercise 9.1: Production-Ready DAG

## Objective

Transform a basic DAG into a production-ready pipeline with proper error handling, retries, timeouts, alerting, and logging best practices.

## Background

Production DAGs require additional considerations beyond basic functionality:

| Aspect | Development | Production |
|--------|-------------|------------|
| Retries | None or minimal | Configured with backoff |
| Timeouts | None | Task and DAG timeouts |
| Alerts | Console logs | Slack/PagerDuty/Email |
| Logging | Print statements | Structured logging |
| Error Handling | Basic try/except | Comprehensive with context |

## Requirements

Take the following basic DAG and transform it into a production-ready pipeline:

### Starting DAG (to transform)

```python
"""Basic ETL DAG - needs production hardening"""

from airflow.sdk import dag, task
from datetime import datetime
import requests

@dag(
    dag_id="basic_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
)
def basic_etl():
    @task
    def extract():
        response = requests.get("https://api.example.com/data")
        return response.json()

    @task
    def transform(data):
        return [item["value"] * 2 for item in data]

    @task
    def load(transformed_data):
        print(f"Loading {len(transformed_data)} records")
        # Simulate database insert
        return {"records_loaded": len(transformed_data)}

    data = extract()
    transformed = transform(data)
    load(transformed)

basic_etl()
```

### Production Requirements

1. **Retry Configuration**
   - Add retries with exponential backoff for the extract task
   - Configure `retries=3`, `retry_delay=timedelta(minutes=1)`
   - Add `retry_exponential_backoff=True`
   - Set `max_retry_delay=timedelta(minutes=30)`

2. **Timeouts**
   - Add `execution_timeout` to each task (appropriate for task type)
   - Add `dagrun_timeout` to the DAG

3. **Failure Callbacks**
   - Implement `on_failure_callback` that sends a Slack notification
   - Include DAG ID, task ID, execution date, and error message
   - Provide a link to the task logs

4. **Success Callback**
   - Implement `on_success_callback` at DAG level
   - Report pipeline completion time and records processed

5. **Logging Best Practices**
   - Use proper logging instead of print statements
   - Include structured information (record counts, timing)
   - Log at appropriate levels (INFO, WARNING, ERROR)

6. **Error Handling**
   - Add proper exception handling with context
   - Distinguish between retryable and non-retryable errors
   - Include meaningful error messages

## Starter Code

See `exercise_9_1_production_dag_starter.py`

## Deliverables

1. **`solution_9_1_production_dag.py`** - Complete production DAG
2. **`alerting.py`** - Reusable callback functions

## Hints

<details>
<summary>Hint 1: Slack callback function</summary>

```python
def send_slack_alert(context):
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

    ti = context.get('ti')
    dag_id = context.get('dag').dag_id
    task_id = ti.task_id
    execution_date = context.get('logical_date')
    log_url = ti.log_url
    exception = context.get('exception')

    message = f"""
    :x: *Task Failed*
    *DAG:* {dag_id}
    *Task:* {task_id}
    *Execution Date:* {execution_date}
    *Error:* {str(exception)[:200]}
    <{log_url}|View Logs>
    """

    hook = SlackWebhookHook(slack_webhook_conn_id='slack_webhook')
    hook.send(text=message)
```

</details>

<details>
<summary>Hint 2: Structured logging</summary>

```python
import logging

logger = logging.getLogger(__name__)

@task
def my_task():
    logger.info("Starting task", extra={
        "record_count": 100,
        "source": "api"
    })
```

</details>

<details>
<summary>Hint 3: Error categorization</summary>

```python
from airflow.exceptions import AirflowException, AirflowSkipException

@task(retries=3)
def smart_task():
    try:
        result = api_call()
    except RateLimitError as e:
        # Retryable - let Airflow retry mechanism handle it
        raise
    except AuthenticationError as e:
        # Non-retryable - fail immediately
        raise AirflowException(f"Auth failed: {e}") from None
    except DataNotFoundError:
        # Skip this run
        raise AirflowSkipException("No data available")
```

</details>

## Success Criteria

- [ ] DAG has appropriate retry configuration with backoff
- [ ] All tasks have execution timeouts
- [ ] DAG has overall timeout
- [ ] Failure callback sends Slack notification with context
- [ ] Success callback reports completion
- [ ] Uses proper logging instead of print
- [ ] Error handling distinguishes error types
- [ ] Code is clean and well-documented

---

Next: [Exercise 9.2: CI/CD Pipeline →](exercise_9_2_cicd_pipeline.md)
