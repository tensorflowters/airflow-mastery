# Exercise 1.1: Hello World DAG

## Objective

Create your first Airflow DAG that prints "Hello, Airflow 3!" to the logs.

## Requirements

Your DAG should:
1. Be named `hello_airflow`
2. Have a start date of January 1, 2024
3. Not be scheduled (manual trigger only)
4. Contain a single task that prints the message

## Starter Code

Create a file `dags/playground/hello_airflow.py`:

```python
# TODO: Import DAG and task from airflow.sdk
# Hint: from airflow.sdk import ...

# TODO: Define your DAG using the context manager
# Hint: with DAG(...) as dag:

# TODO: Create a task using the @task decorator that prints "Hello, Airflow 3!"
```

## Verification

1. After saving, wait ~30 seconds for the DAG to appear in the UI
2. Toggle the DAG "ON" 
3. Trigger a manual run (play button)
4. Check the task logs - you should see your message

## Hints

<details>
<summary>Hint 1: Import statement</summary>

```python
from airflow.sdk import DAG, task
```

</details>

<details>
<summary>Hint 2: DAG definition</summary>

```python
with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
):
    # tasks go here
```

</details>

<details>
<summary>Hint 3: Task decorator</summary>

```python
@task
def say_hello():
    print("Hello, Airflow 3!")

say_hello()  # Don't forget to call the task!
```

</details>

## Success Criteria

- [ ] DAG appears in Airflow UI
- [ ] DAG runs successfully (green status)
- [ ] Log output shows "Hello, Airflow 3!"
