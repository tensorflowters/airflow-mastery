# Solution: Exercise 1.1

```python
"""
Exercise 1.1 Solution: Hello World DAG

This is your first Airflow 3 DAG demonstrating:
- The new airflow.sdk imports
- Basic DAG definition
- Using the @task decorator
"""

from datetime import datetime
from airflow.sdk import DAG, task


@task
def say_hello():
    """A simple task that prints a greeting."""
    print("Hello, Airflow 3!")


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["module-01", "exercise"],
):
    say_hello()
```

## Key Learning Points

1. **New imports**: We use `airflow.sdk` instead of the old import paths
2. **DAG as context manager**: The `with DAG(...):` pattern automatically associates tasks with the DAG
3. **@task decorator**: Creates a Python function into an Airflow task
4. **schedule=None**: The DAG won't run automatically; you trigger it manually
5. **catchup=False**: Prevents backfilling of past dates (good practice for development)

## Common Mistakes

1. **Forgetting to call the task**: The `say_hello()` line is crucial - it creates the task instance
2. **Wrong import path**: Using old `from airflow import DAG` still works but is deprecated
3. **Missing datetime import**: The `start_date` requires a datetime object
