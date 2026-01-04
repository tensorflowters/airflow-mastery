# Exercise 1.3: Sequential Task Dependencies

## Objective

Create a DAG with three tasks that execute in sequence, simulating a simple ETL pipeline.

## Requirements

Your DAG should:
1. Be named `simple_etl_pipeline`
2. Have three tasks: `extract`, `transform`, `load`
3. Tasks must run in order: extract → transform → load
4. Each task should print what it's doing and return some data

## Starter Code

```python
from datetime import datetime
from airflow.sdk import DAG, task

# TODO: Create three tasks using @task decorator
# Each task should:
# - Print what stage it's executing
# - Return some data (extract returns raw data, transform returns processed, etc.)

# TODO: Define task dependencies
# Hint: Use the >> operator to set dependencies
# Example: task1 >> task2 >> task3

with DAG(
    dag_id="simple_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):
    # Your code here
    pass
```

## Expected Behavior

When you run this DAG:
1. The Airflow UI should show 3 tasks in sequence (Graph view)
2. Tasks execute one after another
3. Each task log shows its print statement

## Verification Steps

1. Open the DAG in the Airflow UI
2. Go to Graph view - verify the dependencies look correct
3. Trigger the DAG
4. Verify tasks run in order (check timestamps in task details)
5. Check logs for each task

## Hints

<details>
<summary>Hint 1: Passing data between tasks</summary>

Tasks can return values and receive them as parameters:

```python
@task
def extract():
    data = [1, 2, 3]
    print(f"Extracted: {data}")
    return data

@task  
def transform(raw_data):
    result = [x * 2 for x in raw_data]
    print(f"Transformed: {result}")
    return result
```

</details>

<details>
<summary>Hint 2: Setting dependencies with return values</summary>

When using TaskFlow, dependencies are implicit through data passing:

```python
raw = extract()
transformed = transform(raw)  # This creates the dependency automatically!
load(transformed)
```

</details>

## Bonus Challenge

After completing the basic exercise, try:
1. Add a fourth task that runs after `load`
2. Make `extract` return a dictionary with metadata
3. Add error handling with try/except in one task

## Success Criteria

- [ ] DAG has 3 tasks visible in UI
- [ ] Graph view shows correct sequential dependencies
- [ ] All tasks complete successfully
- [ ] Logs show data flowing between tasks
