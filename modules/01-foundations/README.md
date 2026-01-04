# Module 01: Foundations

## 🎯 Learning Objectives

By the end of this module, you will:
- Understand Airflow's core architecture and how components interact
- Grasp the key differences between Airflow 2.x and Airflow 3
- Know what a DAG is and how it represents a workflow
- Understand Tasks, Operators, and the execution model
- Be able to write and run your first DAG

## ⏱️ Estimated Time: 3-4 hours

---

## 1. What is Apache Airflow?

Airflow is a **workflow orchestration platform** — it doesn't process data itself, but coordinates when and how data processing tasks run. Think of it as a conductor for your data orchestra.

### Key Insight
> Airflow is about **when** and **in what order**, not **what**. The actual data processing happens in external systems (databases, Spark clusters, APIs, etc.)

### Core Use Cases
- ETL/ELT pipelines
- ML model training and deployment
- Report generation and distribution
- System monitoring and alerting
- Data quality checks
- Cross-system data synchronization

---

## 2. Architecture Overview

### Airflow 3 Component Model

```
┌─────────────────────────────────────────────────────────────┐
│                       USER INTERFACE                         │
│                    (React-based Web UI)                      │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        API SERVER                            │
│              (FastAPI - serves UI + REST API)                │
│                    airflow api-server                        │
└─────────────────────────────┬───────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│    SCHEDULER    │ │  DAG PROCESSOR  │ │    TRIGGERER    │
│ Decides when    │ │ Parses DAG      │ │ Handles async   │
│ tasks run       │ │ files           │ │ sensors/deferra │
└────────┬────────┘ └─────────────────┘ └─────────────────┘
         │
         │ Task Execution Interface (REST API)
         ▼
┌─────────────────────────────────────────────────────────────┐
│                         EXECUTOR                             │
│   (KubernetesExecutor / CeleryExecutor / LocalExecutor)     │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                         WORKERS                              │
│              (Execute actual task code)                      │
│              Uses Task SDK for communication                 │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    METADATA DATABASE                         │
│              (PostgreSQL recommended)                        │
│    Stores DAG definitions, run history, task states          │
└─────────────────────────────────────────────────────────────┘
```

### 🆕 What Changed in Airflow 3

| Component | Airflow 2.x | Airflow 3 |
|-----------|-------------|-----------|
| Web Server | `airflow webserver` | `airflow api-server` |
| DAG Parsing | Part of scheduler | Separate `airflow dag-processor` |
| Worker DB Access | Direct connection | Via Task Execution API only |
| API | `/api/v1` (Flask) | `/api/v2` (FastAPI) |

**Why this matters**: Workers no longer need database credentials, improving security. The Task SDK handles all communication with the scheduler.

---

## 3. Core Concepts

### DAG (Directed Acyclic Graph)

A DAG defines:
- **What tasks exist**
- **Dependencies between tasks** (which runs before which)
- **Schedule** (when to run)
- **Configuration** (retries, timeouts, etc.)

```python
from airflow.sdk import DAG
from datetime import datetime

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # or cron: "0 0 * * *"
    catchup=False,
    tags=["example", "learning"]
):
    # Tasks defined here
    pass
```

**Key Properties:**
- `dag_id`: Unique identifier (must be unique across all DAGs)
- `start_date`: When the DAG becomes active
- `schedule`: How often to run (None = manual only)
- `catchup`: Whether to backfill missed runs

### Tasks

A task is a single unit of work. In Airflow 3, tasks are defined using:

1. **TaskFlow API** (recommended) — Python functions with `@task` decorator
2. **Operators** — Pre-built task templates

```python
from airflow.sdk import task

@task
def extract_data():
    """This is a task defined with TaskFlow API"""
    return {"users": 100, "events": 5000}

@task
def transform_data(raw_data: dict):
    """Tasks can receive data from upstream tasks"""
    return {"processed_users": raw_data["users"] * 2}
```

### Dependencies

Dependencies define execution order:

```python
# TaskFlow: Implicit dependencies via function calls
result = extract_data()
transformed = transform_data(result)

# Explicit operators: Use >> or <<
task_a >> task_b >> task_c      # A then B then C
task_a >> [task_b, task_c]      # A then B and C in parallel
[task_a, task_b] >> task_c      # A and B complete, then C
```

### XCom (Cross-Communication)

XComs allow tasks to share small amounts of data:
- TaskFlow handles this automatically via return values
- Operators use `xcom_push()` and `xcom_pull()`

⚠️ **Warning**: XComs are stored in the database. Keep them small (< 48KB recommended).

---

## 4. The Execution Model

### Key Terms

| Term | Definition |
|------|------------|
| **DAG Run** | A single execution of a DAG |
| **Task Instance** | A single execution of a task within a DAG Run |
| **Logical Date** | The scheduled time for a run (formerly `execution_date`) |
| **Data Interval** | The time period the run covers `[start, end)` |

### Task Lifecycle

```
none → scheduled → queued → running → success/failed/skipped
                      ↓
              up_for_retry (if retries configured)
```

### Understanding Data Intervals

For a daily DAG with `start_date=2024-01-01`:

| Run # | Logical Date | Data Interval Start | Data Interval End |
|-------|--------------|---------------------|-------------------|
| 1 | 2024-01-01 | 2024-01-01 00:00 | 2024-01-02 00:00 |
| 2 | 2024-01-02 | 2024-01-02 00:00 | 2024-01-03 00:00 |

**Key Insight**: The run with `logical_date=2024-01-01` actually runs at the END of that day (2024-01-02 00:00), processing data FROM 2024-01-01.

---

## 5. Your First DAG

Let's create a complete, working DAG:

```python
"""
My First Airflow 3 DAG

This DAG demonstrates:
- Basic DAG structure
- TaskFlow API
- Task dependencies
- XCom data passing
"""

from airflow.sdk import DAG, task
from datetime import datetime, timedelta

# Default arguments applied to all tasks
default_args = {
    "owner": "learner",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="01_hello_airflow",
    default_args=default_args,
    description="My first Airflow 3 DAG",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["module-01", "foundations"],
):
    
    @task
    def greet():
        """First task: Print a greeting"""
        print("Hello from Airflow 3!")
        return "Hello"
    
    @task
    def get_current_time():
        """Second task: Get current timestamp"""
        from datetime import datetime
        now = datetime.now().isoformat()
        print(f"Current time: {now}")
        return now
    
    @task
    def combine_messages(greeting: str, timestamp: str):
        """Third task: Combine data from previous tasks"""
        message = f"{greeting}! The time is {timestamp}"
        print(f"Combined message: {message}")
        return message
    
    @task
    def final_log(message: str):
        """Fourth task: Final logging"""
        print(f"Workflow complete. Final message: {message}")
    
    # Define the DAG structure
    greeting = greet()
    current_time = get_current_time()
    combined = combine_messages(greeting, current_time)
    final_log(combined)
```

### Running Your DAG

1. **Copy to playground**:
   ```bash
   cp exercises/01_hello_airflow.py ../../dags/playground/
   ```

2. **Verify it loads**:
   ```bash
   airflow dags list | grep 01_hello
   ```

3. **Test without scheduling**:
   ```bash
   airflow dags test 01_hello_airflow 2024-01-01
   ```

4. **Trigger via UI**:
   - Open http://localhost:8080
   - Find `01_hello_airflow`
   - Click the play button (▶)

---

## 6. Airflow 3 Import Patterns

### ⚠️ Critical: New Import Paths

Airflow 3 uses the `airflow.sdk` namespace:

```python
# ✅ Airflow 3 way
from airflow.sdk import DAG, task
from airflow.sdk import Asset

# ❌ Old Airflow 2.x (still works but deprecated)
from airflow import DAG
from airflow.decorators import task
```

### Standard Provider Package

Common operators moved to `apache-airflow-providers-standard`:

```python
# Install first: pip install apache-airflow-providers-standard

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
```

---

## 📝 Exercises

### Exercise 1.1: Conceptual Understanding
Answer these questions in your own words:
1. What's the difference between a DAG and a Task?
2. Why do workers in Airflow 3 not connect directly to the database?
3. What happens if you set `catchup=True` on a DAG with `start_date` from 6 months ago?

### Exercise 1.2: Build a Simple DAG
Create a DAG that:
- Has 4 tasks
- Task A and Task B run in parallel
- Task C depends on both A and B
- Task D depends on C
- Uses the TaskFlow API

Save as `exercises/solutions/ex_1_2_parallel.py`

### Exercise 1.3: Explore the UI
With your local environment running:
1. Navigate to DAGs view
2. Find your DAG and trigger it manually
3. View the Graph view — understand the visual representation
4. Click on a task instance and view its logs
5. Explore the Grid view (new in Airflow 3)

---

## ✅ Checkpoint

Before moving to Module 02, ensure you can:

- [ ] Explain the role of each Airflow component
- [ ] Describe why Airflow 3's architecture is more secure
- [ ] Write a basic DAG with multiple tasks
- [ ] Understand task dependencies using `>>`
- [ ] Run a DAG using `airflow dags test`
- [ ] Navigate the Airflow UI

---

## 📚 Further Reading

- [Airflow Concepts Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html)
- [Upgrading to Airflow 3](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html)
- [Architecture Overview](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/overview.html)

---

Next: [Module 02: TaskFlow API →](../02-taskflow-api/README.md)
