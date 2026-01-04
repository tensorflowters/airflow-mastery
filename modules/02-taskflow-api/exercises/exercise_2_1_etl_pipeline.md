# Exercise 2.1: ETL Pipeline with TaskFlow

## Objective

Build a complete ETL (Extract, Transform, Load) pipeline using the TaskFlow API that processes data from two different sources, transforms them separately, combines the results, and loads to a destination.

## Requirements

Your DAG should:
1. Be named `exercise_2_1_etl_pipeline`
2. Have a start date of January 1, 2024
3. Not be scheduled (manual trigger only)
4. Include proper tags: `["exercise", "module-02", "etl"]`

### Task Requirements

1. **Extract Tasks** (2 tasks):
   - `extract_users`: Returns mock user data as a dictionary
   - `extract_orders`: Returns mock order data as a dictionary
   - Both extract tasks should have `retries=2` and `retry_delay=timedelta(seconds=30)`

2. **Transform Tasks** (2 tasks):
   - `transform_users`: Takes user data, adds a `processed` field
   - `transform_orders`: Takes order data, calculates order totals
   - Use `multiple_outputs=True` where appropriate

3. **Combine Task**:
   - `combine_data`: Merges the transformed user and order data
   - Returns a combined summary dictionary

4. **Load Task**:
   - `load_to_destination`: Receives combined data and prints it
   - Returns metadata about the load operation

### Type Hints

All tasks must include proper type hints for parameters and return values.

## Starter Code

Create a file `dags/playground/exercise_2_1_etl_pipeline.py`:

```python
"""
Exercise 2.1: ETL Pipeline with TaskFlow API
============================================
Build a multi-source ETL pipeline using TaskFlow patterns.
"""

from datetime import datetime, timedelta
# TODO: Import DAG and task from airflow.sdk


# TODO: Define the DAG with the specified configuration
# Hint: Use the @dag decorator or context manager


    # TODO: Define extract_users task
    # - Returns: {"users": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]}
    # - Add retries=2, retry_delay=timedelta(seconds=30)


    # TODO: Define extract_orders task
    # - Returns: {"orders": [{"id": 101, "user_id": 1, "amount": 99.99}, ...]}
    # - Add retries=2, retry_delay=timedelta(seconds=30)


    # TODO: Define transform_users task
    # - Takes user data dict as input
    # - Adds "processed": True to each user
    # - Consider using multiple_outputs=True


    # TODO: Define transform_orders task
    # - Takes order data dict as input
    # - Calculates total of all orders
    # - Returns dict with orders and total


    # TODO: Define combine_data task
    # - Takes transformed users and orders
    # - Creates a summary combining both


    # TODO: Define load_to_destination task
    # - Prints the combined data
    # - Returns metadata about the operation


    # TODO: Wire up the tasks
    # users_raw = extract_users()
    # orders_raw = extract_orders()
    # ...


# Don't forget to instantiate the DAG!
```

## Verification

1. Save the file and wait for Airflow to parse it (~30 seconds)
2. Check the DAG appears in the UI without errors
3. Trigger a manual run
4. Verify in the Graph view:
   - `extract_users` and `extract_orders` run in parallel
   - Both feed into their respective transform tasks
   - `combine_data` waits for both transforms
   - `load_to_destination` runs last
5. Check the logs for each task to see the data flow

## Hints

<details>
<summary>Hint 1: Import statement</summary>

```python
from airflow.sdk import dag, task
```

</details>

<details>
<summary>Hint 2: Using @dag decorator</summary>

```python
@dag(
    dag_id="exercise_2_1_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "module-02", "etl"],
)
def etl_pipeline():
    # Define tasks inside
    pass

etl_pipeline()  # Instantiate!
```

</details>

<details>
<summary>Hint 3: Task with retries</summary>

```python
@task(retries=2, retry_delay=timedelta(seconds=30))
def extract_users() -> dict:
    return {"users": [...]}
```

</details>

<details>
<summary>Hint 4: Multiple outputs pattern</summary>

```python
@task(multiple_outputs=True)
def transform_orders(data: dict) -> dict:
    orders = data["orders"]
    total = sum(o["amount"] for o in orders)
    return {
        "orders": orders,
        "total": total
    }

# Access individual outputs:
result = transform_orders(raw_data)
result["orders"]  # Just orders
result["total"]   # Just total
```

</details>

<details>
<summary>Hint 5: Wiring parallel tasks</summary>

```python
# These run in parallel (no dependency between them)
users_raw = extract_users()
orders_raw = extract_orders()

# These wait for their respective upstream tasks
users_transformed = transform_users(users_raw)
orders_transformed = transform_orders(orders_raw)

# This waits for both transforms
combined = combine_data(users_transformed, orders_transformed)
```

</details>

## Success Criteria

- [ ] DAG appears in UI without import errors
- [ ] DAG has correct tags and description
- [ ] Extract tasks have retry configuration
- [ ] Extract tasks run in parallel (check Graph view)
- [ ] Transform tasks use type hints
- [ ] Multiple outputs used where appropriate
- [ ] Load task prints the combined summary
- [ ] All tasks complete successfully (green)

## Expected Output

In the `load_to_destination` task logs, you should see something like:

```
Loading data summary:
{
    "user_count": 2,
    "order_count": 3,
    "total_revenue": 299.97,
    "processed_at": "2024-01-01T00:00:00"
}
```
