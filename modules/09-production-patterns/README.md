# Module 09: Production Patterns

## 🎯 Learning Objectives

By the end of this module, you will:

- Design DAGs for reliability and maintainability
- Implement proper error handling and alerting
- Set up CI/CD pipelines for DAG deployment
- Monitor Airflow performance and health
- Apply organizational patterns for large-scale Airflow

## ⏱️ Estimated Time: 5-6 hours

---

## 1. DAG Design Patterns

### Idempotent Tasks

Tasks should produce the same result when run multiple times:

```python
@task
def load_data(date: str):
    """Idempotent: overwrites partition for the date"""
    # ✅ Good: Delete before insert
    db.execute(f"DELETE FROM table WHERE date = '{date}'")
    db.execute(f"INSERT INTO table SELECT * FROM source WHERE date = '{date}'")

    # ❌ Bad: Append without checking
    # db.execute(f"INSERT INTO table SELECT * FROM source WHERE date = '{date}'")
```

### Atomic Operations

All-or-nothing execution:

```python
@task
def atomic_operation():
    """Use transactions for atomicity"""
    with db.begin_transaction() as txn:
        try:
            txn.execute("INSERT INTO table_a ...")
            txn.execute("UPDATE table_b ...")
            txn.execute("DELETE FROM table_c ...")
            txn.commit()
        except Exception:
            txn.rollback()
            raise
```

### Small, Focused Tasks

```python
# ✅ Good: Single responsibility
@task
def extract_users():
    return fetch_users()


@task
def validate_users(users):
    return [u for u in users if u["email"]]


@task
def transform_users(valid_users):
    return [enrich(u) for u in valid_users]


# ❌ Bad: Monolithic task
@task
def do_everything():
    users = fetch_users()
    valid = [u for u in users if u["email"]]
    enriched = [enrich(u) for u in valid]
    save(enriched)
    notify()
```

---

## 2. Error Handling & Retries

### Retry Configuration

```python
from datetime import timedelta


@task(
    retries=3,
    retry_delay=timedelta(minutes=5),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(hours=1),
)
def flaky_api_call():
    """Retries with exponential backoff"""
    response = requests.get("https://api.example.com/data")
    response.raise_for_status()
    return response.json()
```

### Custom Retry Logic

```python
from airflow.exceptions import AirflowException


@task(retries=3)
def smart_retry():
    try:
        result = call_api()
    except RateLimitError:
        # Don't retry rate limits - wait longer
        raise AirflowException("Rate limited, will retry with backoff")
    except AuthenticationError:
        # Don't retry auth failures
        raise AirflowException("Auth failed - check credentials")
    except ConnectionError:
        # Retry connection issues
        raise
    return result
```

### Callbacks for Notifications

```python
from airflow.sdk import DAG
from datetime import datetime

def on_failure(context):
    """Called when a task fails"""
    task_instance = context['ti']
    dag_id = context['dag'].dag_id

    send_slack_alert(
        channel="#airflow-alerts",
        message=f"Task {task_instance.task_id} failed in {dag_id}",
        details=str(context.get('exception'))
    )

def on_success(context):
    """Called when DAG succeeds"""
    send_slack_message("#airflow-status", "Pipeline completed successfully")

with DAG(
    dag_id="production_pipeline",
    default_args={
        "on_failure_callback": on_failure,
    },
    on_success_callback=on_success,  # DAG-level callback
    ...
):
    pass
```

---

## 3. Alerting & Notifications

### Slack Integration

```python
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook


def send_slack_alert(context):
    hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")

    ti = context["ti"]
    log_url = ti.log_url

    message = {
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f":x: *Task Failed*\n*DAG:* {ti.dag_id}\n*Task:* {ti.task_id}"},
            },
            {
                "type": "actions",
                "elements": [{"type": "button", "text": {"type": "plain_text", "text": "View Logs"}, "url": log_url}],
            },
        ]
    }

    hook.send_dict(message)
```

### Deadline Alerts (Airflow 3)

```python
from datetime import timedelta

with DAG(
    dag_id="sla_dag",
    dagrun_timeout=timedelta(hours=2),  # Entire DAG timeout
    ...
):
    @task(execution_timeout=timedelta(minutes=30))
    def time_bounded_task():
        """Fails if exceeds 30 minutes"""
        pass
```

---

## 4. CI/CD for DAGs

### Deployment Strategies

| Strategy     | Description              | Use Case                  |
| ------------ | ------------------------ | ------------------------- |
| Git-sync     | Continuous pull from Git | Simple, real-time updates |
| Docker Image | Bake DAGs into image     | Immutable, versioned      |
| CI/CD Push   | Deploy on merge          | Controlled releases       |

### GitHub Actions Pipeline

```yaml
# .github/workflows/deploy-dags.yml
name: Deploy DAGs

on:
  push:
    branches: [main]
    paths: ["dags/**"]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install apache-airflow ruff pytest

      - name: Lint
        run: ruff check dags/ --select AIR

      - name: Test DAG integrity
        run: |
          export AIRFLOW_HOME=$(pwd)/.airflow
          airflow db init
          python -c "from airflow.models import DagBag; db=DagBag('dags/'); assert not db.import_errors"

      - name: Run unit tests
        run: pytest tests/ -v

  deploy:
    needs: validate
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: actions/checkout@v4

      # Option 1: Sync to S3 (if using S3 for DAG storage)
      - name: Sync to S3
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-region: us-east-1
      - run: aws s3 sync dags/ s3://my-airflow-dags/dags/

      # Option 2: Trigger Helm upgrade with new image
      # - name: Deploy to K8s
      #   run: |
      #     helm upgrade airflow apache-airflow/airflow \
      #       --set images.airflow.tag=${{ github.sha }} \
      #       -n airflow
```

### Blue-Green Deployments

```yaml
# Deploy to staging first
staging:
  runs-on: ubuntu-latest
  steps:
    - name: Deploy to staging
      run: |
        kubectl config use-context staging
        helm upgrade airflow-staging ...

# After validation, deploy to production
production:
  needs: staging
  runs-on: ubuntu-latest
  environment: production # Requires approval
  steps:
    - name: Deploy to production
      run: |
        kubectl config use-context production
        helm upgrade airflow-production ...
```

---

## 5. Monitoring & Observability

### Key Metrics to Monitor

| Metric                            | Meaning            | Alert Threshold |
| --------------------------------- | ------------------ | --------------- |
| `scheduler_heartbeat`             | Scheduler is alive | Missing for 60s |
| `dag_processing.total_parse_time` | DAG parsing time   | > 30s           |
| `executor.queued_tasks`           | Tasks waiting      | > 100 for 10m   |
| `pool_running_slots`              | Pool usage         | > 90% capacity  |
| `task_instance_failures`          | Failed tasks       | > 5/hour        |

### Prometheus + Grafana

```yaml
# values.yaml additions
statsd:
  enabled: true

extraEnv:
  - name: AIRFLOW__METRICS__STATSD_ON
    value: "True"
  - name: AIRFLOW__METRICS__STATSD_HOST
    value: "prometheus-statsd-exporter"
```

### Health Checks

```python
# Custom health check DAG
from datetime import datetime

from airflow.sdk import DAG, task

with DAG(
    dag_id="health_check",
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    max_active_runs=1,
    catchup=False,
):

    @task
    def check_database():
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="warehouse")
        hook.run("SELECT 1")
        return "db_ok"

    @task
    def check_s3():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="aws_default")
        hook.check_for_bucket("my-bucket")
        return "s3_ok"

    @task
    def report_health(results: list):
        if all(r.endswith("_ok") for r in results):
            print("All systems healthy")
        else:
            raise Exception("Health check failed")

    report_health([check_database(), check_s3()])
```

---

## 6. Organizational Patterns

### DAG Naming Conventions

```
{domain}_{action}_{target}[_{version}]

Examples:
- analytics_load_daily_sales
- marketing_sync_hubspot_contacts
- finance_generate_monthly_report_v2
- infra_cleanup_old_logs
```

### Folder Structure for Large Teams

```
dags/
├── analytics/
│   ├── __init__.py
│   ├── daily_metrics.py
│   └── weekly_reports.py
├── marketing/
│   ├── __init__.py
│   └── campaign_sync.py
├── shared/
│   ├── __init__.py
│   ├── operators/
│   │   └── custom_operator.py
│   ├── hooks/
│   │   └── custom_hook.py
│   └── utils/
│       └── helpers.py
└── config/
    ├── dev.yaml
    ├── staging.yaml
    └── prod.yaml
```

### Configuration Management

```python
# dags/shared/config.py
import os

import yaml


def load_config():
    env = os.getenv("AIRFLOW_ENV", "dev")
    config_path = f"/opt/airflow/dags/config/{env}.yaml"

    with open(config_path) as f:
        return yaml.safe_load(f)


CONFIG = load_config()

# Usage in DAG
from shared.config import CONFIG


@task
def process():
    database = CONFIG["database"]["host"]
    api_key = CONFIG["api"]["key"]
```

---

## 7. Performance Optimization

### DAG Parsing Performance

```python
# ✅ Fast: Import inside functions
@task
def process():
    pass
    # ...


# ❌ Slow: Top-level imports of heavy libraries
```

### Reduce DAG Count

```python
# ❌ Bad: One DAG per table (100 DAGs)
for table in get_tables():
    with DAG(f"sync_{table}", ...):
        ...

# ✅ Better: One DAG with dynamic tasks
with DAG("sync_all_tables", ...):

    @task
    def get_tables():
        return ["table1", "table2", ...]

    @task
    def sync_table(table: str): ...

    sync_table.expand(table=get_tables())
```

### Pool Management

```python
# Create pools via UI or API
# Admin → Pools → Add

# Limit concurrent tasks
@task(pool="api_pool")  # Max concurrent API calls
def call_rate_limited_api():
    pass
```

---

## 📝 Exercises

### Exercise 9.1: Production-Ready DAG

Take any DAG from previous modules and add:

- Proper retry configuration
- Failure callbacks with Slack notification
- Execution timeouts
- Logging best practices

### Exercise 9.2: CI/CD Pipeline

Create a GitHub Actions workflow that:

- Runs on PR to validate DAGs
- Runs on merge to main to deploy
- Includes DAG integrity tests

### Exercise 9.3: Monitoring Dashboard

Design (and optionally implement):

- Key metrics to track
- Alert thresholds
- A Grafana dashboard layout

---

## ✅ Checkpoint

Before moving to Module 10, ensure you can:

- [ ] Design idempotent and atomic tasks
- [ ] Configure retries with exponential backoff
- [ ] Set up failure callbacks and alerts
- [ ] Create a CI/CD pipeline for DAG deployment
- [ ] Identify key monitoring metrics
- [ ] Apply organizational patterns for DAG management

---

## 🏭 Industry Spotlight: Spotify

**How Spotify Orchestrates Production ML Pipelines**

Spotify runs thousands of ML models powering personalized recommendations, podcast discovery, and audio analysis. Production patterns ensure reliability at scale:

| Challenge             | Production Pattern Solution                              |
| --------------------- | -------------------------------------------------------- |
| **API rate limits**   | Exponential backoff with jitter prevents thundering herd |
| **Model freshness**   | Scheduled retraining with staleness alerts               |
| **Cost visibility**   | Callbacks track compute costs per pipeline               |
| **Failure isolation** | Circuit breakers prevent cascade failures                |

**Pattern in Use**: Spotify-style production ML orchestration:

```python
from datetime import timedelta

from airflow.sdk import task


def cost_tracking_callback(context):
    """Track pipeline execution costs."""
    task_instance = context["ti"]
    duration = task_instance.duration

    # Calculate and log costs
    compute_cost = calculate_compute_cost(duration, context["params"]["instance_type"])
    log_to_cost_dashboard(dag_id=context["dag"].dag_id, cost=compute_cost, timestamp=context["logical_date"])


@task(
    retries=5,
    retry_delay=timedelta(seconds=30),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
    on_success_callback=cost_tracking_callback,
    execution_timeout=timedelta(hours=2),
)
def train_recommendation_model(user_segment: str):
    """Production ML training with full observability."""
    with circuit_breaker("ml-training-service"):
        model = train_model(user_segment)
        validate_model_quality(model, min_accuracy=0.85)
        return deploy_model(model)
```

**Key Insight**: Spotify's production patterns reduced incident response time by 70% and prevented $2M+ in potential revenue loss through early anomaly detection.

📖 **Related Exercise**: [Exercise 9.4: LLM Retry Patterns](exercises/exercise_9_4_llm_retry_patterns.md) - Apply production patterns to AI/ML workloads

---

## 📚 Further Reading

- [Best Practices Documentation](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Monitoring & Observability](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/index.html)
- [Case Study: Spotify Recommendations](../../docs/case-studies/spotify-recommendations.md)

---

Next: [Module 10: Advanced Topics →](../10-advanced-topics/README.md)
