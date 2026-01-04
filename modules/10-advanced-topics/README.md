# Module 10: Advanced Topics

## 🎯 Learning Objectives

By the end of this module, you will:
- Understand the Task SDK and Task Execution Interface
- Use the Edge Executor for hybrid deployments
- Configure multiple executors
- Build plugins and custom extensions
- Work with the new Airflow 3 API

## ⏱️ Estimated Time: 4-6 hours

---

## 1. Task SDK Deep Dive

Airflow 3's Task SDK is the new runtime for task execution.

### Why Task SDK?

| Old Model (Airflow 2) | New Model (Airflow 3) |
|-----------------------|-----------------------|
| Workers access database | Workers use REST API |
| Full Airflow install on workers | Lightweight Task SDK |
| Shared credentials | Scoped tokens |
| Python only | Multi-language potential |

### Task SDK Usage

```python
# The SDK is used automatically with @task
from airflow.sdk import task

@task
def my_task():
    # Inside here, you're running in the Task SDK runtime
    # No direct database access - use provided APIs
    pass
```

### Accessing Metadata via SDK

```python
from airflow.sdk import task
from airflow.sdk.execution_time.context import get_current_context

@task
def access_metadata():
    context = get_current_context()
    
    # Get task instance info
    ti = context["ti"]
    
    # Pull XCom (goes through Task Execution API)
    value = ti.xcom_pull(task_ids="upstream", key="result")
    
    # Push XCom
    ti.xcom_push(key="my_result", value={"status": "done"})
```

### Task SDK Versioning

The Task SDK is versioned separately from Airflow:
- Airflow 3.0.x includes Task SDK 1.0.x
- Allows independent SDK updates

```bash
# Check installed version
pip show apache-airflow-task-sdk
```

---

## 2. Task Execution Interface (AIP-72)

The API that workers use to communicate with the scheduler.

### Architecture

```
┌─────────────┐         ┌─────────────────┐
│   Worker    │ ◄─────► │   API Server    │
│  (Task SDK) │  REST   │ (Task Execution │
│             │   API   │   Interface)    │
└─────────────┘         └────────┬────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │    Database     │
                        └─────────────────┘
```

### Key Endpoints

| Endpoint | Purpose |
|----------|---------|
| `GET /task/{id}/state` | Get task state |
| `PATCH /task/{id}/state` | Update task state |
| `POST /task/{id}/xcom` | Push XCom |
| `GET /task/{id}/xcom/{key}` | Pull XCom |
| `GET /task/{id}/connection/{conn_id}` | Get connection |

### Benefits

1. **Security**: Workers don't need DB credentials
2. **Scalability**: API can be scaled independently
3. **Isolation**: Tasks can run in untrusted environments
4. **Multi-language**: Any language can implement the SDK

---

## 3. Edge Executor (AIP-69)

Run tasks on edge devices, remote data centers, or hybrid clouds.

### Use Cases

- Process data locally before sending to cloud
- Run tasks in air-gapped environments
- Utilize on-premise GPU resources
- Comply with data locality requirements

### Setup

```bash
# Install edge provider
pip install apache-airflow-providers-edge3
```

### Configuration

```python
# In airflow.cfg or environment
[edge]
api_url = https://airflow.example.com/api/v2
job_poll_interval = 5
heartbeat_interval = 30
```

### Edge Worker Registration

```bash
# On the edge device
airflow edge worker \
  --edge-hostname "edge-worker-1" \
  --api-url "https://airflow.example.com/api/v2" \
  --api-token "$EDGE_TOKEN"
```

### Targeting Edge Workers

```python
from airflow.sdk import DAG, task

with DAG(...):
    @task(queue="edge-datacenter-1")  # Route to specific edge
    def process_on_edge():
        """Runs on edge worker"""
        pass
```

---

## 4. Multiple Executor Configuration

Airflow 3 deprecates hybrid executors in favor of multiple executor config.

### Configuration

```ini
# airflow.cfg
[core]
executor = airflow.executors.executor_loader.ExecutorLoader

[executors]
default = KubernetesExecutor
celery = CeleryExecutor
local = LocalExecutor
```

### Task-Level Executor Selection

```python
@task(executor="celery")
def high_throughput_task():
    """Uses CeleryExecutor for speed"""
    pass

@task(executor="default")  # KubernetesExecutor
def isolated_task():
    """Uses K8s for isolation"""
    pass
```

---

## 5. Building Plugins

Extend Airflow functionality with plugins.

### Plugin Structure

```python
# plugins/my_plugin.py
from airflow.plugins_manager import AirflowPlugin
from flask import Blueprint

# Custom view
my_views_blueprint = Blueprint(
    "my_views",
    __name__,
    url_prefix="/myplugin",
)

@my_views_blueprint.route("/")
def my_view():
    return "Hello from my plugin!"

# Custom operator (if not using providers)
class MyCustomOperator(BaseOperator):
    def execute(self, context):
        pass

# Register plugin
class MyPlugin(AirflowPlugin):
    name = "my_plugin"
    flask_blueprints = [my_views_blueprint]
    operators = [MyCustomOperator]
```

### Plugin Location

```
$AIRFLOW_HOME/
└── plugins/
    ├── __init__.py
    └── my_plugin.py
```

### Custom Macros

```python
# plugins/macros.py
from airflow.plugins_manager import AirflowPlugin

def custom_macro(date_str):
    """Custom Jinja macro available in templates"""
    return date_str.replace("-", "")

class MacroPlugin(AirflowPlugin):
    name = "macro_plugin"
    macros = [custom_macro]
```

Usage in DAG:
```python
bash_command="echo {{ custom_macro(ds) }}"  # 20240101
```

---

## 6. REST API v2

Airflow 3 uses FastAPI-based REST API v2.

### Authentication

```python
import requests

# Get auth token (depends on auth backend)
headers = {"Authorization": "Bearer <token>"}

# Or basic auth
from requests.auth import HTTPBasicAuth
auth = HTTPBasicAuth("username", "password")
```

### Common Operations

```python
import requests

BASE = "https://airflow.example.com/api/v2"

# List DAGs
r = requests.get(f"{BASE}/dags", auth=auth)
dags = r.json()["dags"]

# Trigger DAG run
r = requests.post(
    f"{BASE}/dags/my_dag/dagRuns",
    json={"conf": {"key": "value"}},
    auth=auth
)

# Get task instances
r = requests.get(
    f"{BASE}/dags/my_dag/dagRuns/run_id/taskInstances",
    auth=auth
)

# Get XCom
r = requests.get(
    f"{BASE}/dags/my_dag/dagRuns/run_id/taskInstances/task_id/xcomEntries/key",
    auth=auth
)
```

### Using the Python Client

```python
from airflow.api_connexion.client import Client

client = Client(
    host="https://airflow.example.com",
    username="admin",
    password="admin"
)

# List DAGs
dags = client.dag_api.get_dags()

# Trigger run
client.dag_run_api.post_dag_run(
    dag_id="my_dag",
    body={"conf": {}}
)
```

---

## 7. DAG Bundles & Versioning

### DAG Bundles

Bundle configuration for DAG deployment:

```python
# In airflow.cfg
[dag_bundles]
my_bundle = {
    "type": "git",
    "repo": "git@github.com:org/dags.git",
    "branch": "main",
    "subdir": "dags/"
}
```

### Version-Aware Execution

```python
from airflow.sdk import DAG, task

with DAG(
    dag_id="versioned_dag",
    version="1.0.0",  # Explicit versioning
    ...
):
    @task
    def my_task():
        pass
```

Key behavior:
- DAG runs use the version at trigger time
- Mid-execution changes don't affect running DAGs
- UI shows version for each run

---

## 8. Custom Timetables

For schedules cron can't express:

```python
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from pendulum import DateTime

class TradingDaysTimetable(Timetable):
    """Run only on trading days (excludes weekends and holidays)"""
    
    HOLIDAYS = {
        DateTime(2024, 12, 25),
        DateTime(2024, 1, 1),
        # ... more holidays
    }
    
    def is_trading_day(self, dt: DateTime) -> bool:
        return dt.weekday() < 5 and dt not in self.HOLIDAYS
    
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval,
        restriction,
    ):
        if last_automated_data_interval is None:
            next_start = restriction.earliest
        else:
            next_start = last_automated_data_interval.end
        
        # Find next trading day
        while not self.is_trading_day(next_start):
            next_start = next_start.add(days=1)
        
        next_end = next_start.add(days=1)
        
        return DagRunInfo(
            run_after=next_end,
            data_interval=DataInterval(next_start, next_end)
        )
```

---

## 9. Deferrable Operators (Advanced Sensors)

Operators that release worker slots while waiting:

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger

class DeferrableSensor(BaseSensorOperator):
    def execute(self, context):
        if not self.poke(context):
            # Defer instead of sleeping
            self.defer(
                trigger=TimeDeltaTrigger(timedelta(minutes=5)),
                method_name="execute_complete"
            )
    
    def execute_complete(self, context, event=None):
        # Called when trigger fires
        if self.poke(context):
            return True
        # Defer again
        self.defer(...)
```

---

## 📝 Exercises

### Exercise 10.1: API Automation
Create a Python script that:
1. Lists all DAGs via API
2. Finds DAGs that haven't run in 7 days
3. Triggers each one with a maintenance flag

### Exercise 10.2: Custom Timetable
Create a timetable for:
- Every weekday at 9 AM
- But skip the first Monday of each month
- And run an extra time on the last Friday

### Exercise 10.3: Plugin Development
Build a plugin that:
1. Adds a custom view showing task duration statistics
2. Provides a custom macro for date formatting
3. Includes a utility operator

---

## ✅ Final Checkpoint

Congratulations on completing the curriculum! Verify you can:

- [ ] Explain the Task SDK architecture
- [ ] Describe when to use Edge Executor
- [ ] Configure multiple executors
- [ ] Build Airflow plugins
- [ ] Use the REST API v2
- [ ] Understand DAG versioning

---

## 🎓 What's Next?

1. **Build a portfolio project**: Create a production-grade pipeline
2. **Contribute to Airflow**: Submit bug fixes or documentation
3. **Explore providers**: Deep-dive into cloud-specific operators
4. **Join the community**: Slack, mailing lists, Airflow Summit

---

## 📚 Additional Resources

- [Airflow 3 Release Blog](https://airflow.apache.org/blog/airflow-three-point-oh-is-here/)
- [AIP Proposals](https://cwiki.apache.org/confluence/display/AIRFLOW/Airflow+Improvement+Proposals)
- [Provider Packages](https://airflow.apache.org/docs/apache-airflow-providers/)
- [Airflow Summit Talks](https://airflowsummit.org/)

---

**You've completed the Airflow Mastery curriculum!** 🎉
