# Exercise 14.3: Concurrency Limits

## Objective

Implement and manage concurrency controls at DAG and task levels to prevent system overload.

## Background

Concurrency limits prevent resource exhaustion:
- **DAG-level**: Limit active DAG runs
- **Task-level**: Limit active task instances
- **Global**: System-wide parallelism

## Requirements

### Part 1: Concurrency Configuration

Create a concurrency management system:

```python
class ConcurrencyConfig:
    def get_dag_concurrency(self, dag_id: str) -> dict:
        """Get DAG concurrency settings."""
        pass

    def set_dag_concurrency(self, dag_id: str, max_active_runs: int) -> bool:
        """Update DAG concurrency."""
        pass

    def get_system_concurrency(self) -> dict:
        """Get global concurrency settings."""
        pass
```

### Part 2: Concurrency-Controlled DAG

Create a DAG with various concurrency settings:

```python
@dag(
    max_active_runs=3,
    max_active_tasks=10,
    ...
)
def controlled_pipeline():
    # Tasks with max_active_tis_per_dag
    # Tasks with max_active_tis_per_dagrun
```

### Part 3: Concurrency Monitor

Create a monitoring utility:

```python
class ConcurrencyMonitor:
    def get_running_dags(self) -> dict:
        """Get currently running DAG runs."""
        pass

    def get_running_tasks(self) -> dict:
        """Get currently running task instances."""
        pass

    def check_capacity(self) -> dict:
        """Check available capacity."""
        pass
```

## Starter Code

See `exercise_14_3_concurrency_limits_starter.py`

## Hints

<details>
<summary>Hint 1: DAG concurrency parameters</summary>

```python
@dag(
    max_active_runs=3,        # Max 3 concurrent DAG runs
    max_active_tasks=10,      # Max 10 tasks running at once
)
def my_dag():
    @task(
        max_active_tis_per_dag=5,     # Max 5 across all runs
        max_active_tis_per_dagrun=2,  # Max 2 per single run
    )
    def my_task():
        pass
```

</details>

<details>
<summary>Hint 2: Checking running instances</summary>

```python
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState

# Count running DAG runs
running_runs = DagRun.find(state=DagRunState.RUNNING)

# Count running tasks
running_tasks = TaskInstance.get_task_instances(
    state=TaskInstanceState.RUNNING
)
```

</details>

## Success Criteria

- [ ] Concurrency config is manageable
- [ ] DAG respects concurrency limits
- [ ] Monitor tracks running instances
- [ ] Capacity checks work correctly

---

Congratulations! You've completed Module 14: Resource Management.
