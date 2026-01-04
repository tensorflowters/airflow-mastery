# Solution: Exercise 1.3

```python
"""
Exercise 1.3 Solution: Sequential Task Dependencies

This DAG demonstrates:
- Creating multiple TaskFlow tasks
- Automatic dependency creation through data passing
- Data flow from extract → transform → load
"""

from datetime import datetime
from airflow.sdk import DAG, task


@task
def extract() -> dict:
    """
    Extract stage: Simulate fetching raw data.

    In a real scenario, this might:
    - Query a database
    - Read from an API
    - Load files from storage
    """
    print("Executing EXTRACT stage...")

    raw_data = {
        "records": [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
            {"id": 3, "value": 300},
        ],
        "source": "sample_db",
        "extracted_at": datetime.now().isoformat(),
    }

    print(f"Extracted {len(raw_data['records'])} records")
    return raw_data


@task
def transform(raw_data: dict) -> dict:
    """
    Transform stage: Process and clean the raw data.

    In a real scenario, this might:
    - Apply business logic
    - Clean and validate data
    - Aggregate or reshape data
    """
    print("Executing TRANSFORM stage...")
    print(f"Received data from: {raw_data['source']}")

    # Transform the data - double all values
    transformed_records = [
        {"id": r["id"], "value": r["value"] * 2, "status": "transformed"}
        for r in raw_data["records"]
    ]

    transformed_data = {
        "records": transformed_records,
        "original_count": len(raw_data["records"]),
        "transformed_at": datetime.now().isoformat(),
    }

    print(f"Transformed {len(transformed_records)} records")
    return transformed_data


@task
def load(transformed_data: dict) -> dict:
    """
    Load stage: Save the transformed data.

    In a real scenario, this might:
    - Write to a data warehouse
    - Update a database
    - Export to files
    """
    print("Executing LOAD stage...")

    # Simulate loading data
    for record in transformed_data["records"]:
        print(f"  Loading record {record['id']}: value={record['value']}")

    result = {
        "loaded_count": len(transformed_data["records"]),
        "status": "success",
        "loaded_at": datetime.now().isoformat(),
    }

    print(f"Loaded {result['loaded_count']} records successfully")
    return result


with DAG(
    dag_id="simple_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["module-01", "exercise", "etl"],
):
    # Create task instances with automatic dependencies
    # When you pass the return value of one task to another,
    # Airflow automatically creates the dependency!

    raw = extract()
    transformed = transform(raw)  # Depends on extract
    load(transformed)              # Depends on transform
```

## Key Learning Points

1. **Automatic Dependencies**: When you pass the output of one task as input to another,
   Airflow automatically creates the dependency chain. No need for `>>` operators!

2. **Data Passing via XCom**: Under the hood, TaskFlow uses XCom to pass data between tasks.
   The `raw_data` and `transformed_data` are serialized and stored in the metadata database.

3. **Type Hints**: Using `raw_data: dict` helps with code readability and IDE support.

4. **Return Values**: Each task returns data that the next task can use. This is the
   recommended pattern for TaskFlow.

## Alternative: Explicit Dependencies

You can also set dependencies explicitly if needed:

```python
# Without data passing
raw = extract()
transformed = transform()  # Note: no argument
loaded = load()

# Set dependencies manually
raw >> transformed >> loaded
```

However, the data-passing pattern is preferred because:
- It's more readable
- It enforces data contracts between tasks
- It makes the data flow explicit

## The Graph View

In the Airflow UI Graph view, you should see:

```
[extract] → [transform] → [load]
```

Each box represents a task, and the arrows show the dependencies.

## Common Patterns

### Adding Validation

```python
@task
def validate(data: dict) -> bool:
    """Validate data before loading."""
    return len(data["records"]) > 0

# Insert between transform and load
validated = validate(transformed)
# Then use a branch to handle validation failure
```

### Parallel Branches

```python
# Multiple transformations
transformed_a = transform_a(raw)
transformed_b = transform_b(raw)

# Both feed into load
load([transformed_a, transformed_b])
```

## Bonus: Error Handling

```python
@task
def safe_extract():
    """Extract with error handling."""
    try:
        # Attempt extraction
        data = fetch_from_source()
        return {"status": "success", "data": data}
    except Exception as e:
        print(f"Extraction failed: {e}")
        return {"status": "error", "error": str(e)}
```
