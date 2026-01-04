# Exercise 5.3: @asset Decorator Pattern

## Objective

Learn to use the `@asset` decorator introduced in Airflow 3.x for a more Pythonic, asset-centric approach to data pipelines.

## Background

The `@asset` decorator combines Asset definition and task implementation in a single, self-documenting unit. This pattern:

- Defines the Asset and its producer in one place
- Shows data dependencies through function parameters
- Automatically manages scheduling based on upstream Assets
- Creates cleaner, more maintainable code

### Traditional vs @asset Pattern

**Traditional Pattern:**
```python
# Asset defined separately
my_asset = Asset("s3://bucket/data")

@dag(...)
def my_dag():
    @task(outlets=[my_asset])
    def produce():
        pass
```

**@asset Pattern:**
```python
@asset(
    uri="s3://bucket/data",
    schedule="@daily",
)
def my_asset():
    # This function IS both the asset and its producer
    pass
```

## Requirements

Create an ETL pipeline using the `@asset` decorator pattern:

### Asset A: raw_data

- URI: `s3://data-lake/bronze/raw_data`
- Schedule: `@daily`
- Simulates extracting data from a source API
- Returns raw data dict

### Asset B: transformed_data

- URI: `s3://data-lake/silver/transformed_data`
- Schedule: Depends on `raw_data`
- Receives raw_data as input parameter
- Returns cleaned/transformed data

### Asset C: warehouse_data

- URI: `postgres://warehouse/analytics_table`
- Schedule: Depends on `transformed_data`
- Receives transformed_data as input
- Simulates loading to warehouse

## Key Concepts

### Basic @asset Decorator

```python
from airflow.sdk import asset

@asset(
    uri="s3://data-lake/my_data",
    schedule="@daily",
)
def my_data():
    """This function produces the Asset."""
    return process_data()
```

### Asset with Dependencies

```python
@asset(
    uri="s3://data-lake/downstream",
    schedule=[my_data],  # Triggered by my_data
)
def downstream_data(my_data):  # Receives upstream Asset as parameter
    """Process the upstream data."""
    return transform(my_data)
```

### Accessing Asset Definition

```python
# The decorated function can be used as an Asset reference
@asset(uri="...", schedule="@daily")
def source_data():
    return data

@asset(uri="...", schedule=[source_data])  # Reference the Asset
def consumer(source_data):  # Parameter receives the data
    pass
```

## Starter Code

See `exercise_5_3_asset_decorator_starter.py`

## Testing Your Assets

```bash
# 1. List all DAGs to see the asset-based DAGs
airflow dags list | grep exercise_5_3

# 2. Test the pipeline
airflow dags test exercise_5_3_raw_data 2024-01-15

# 3. View Asset dependencies in UI
# Navigate to: DAGs → Assets
```

## Hints

<details>
<summary>Hint 1: Creating the raw_data Asset</summary>

```python
from airflow.sdk import asset

@asset(
    uri="s3://data-lake/bronze/raw_data",
    schedule="@daily",
)
def raw_data():
    """Extract raw data from source."""
    print("Extracting raw data...")
    return {
        "records": [...],
        "extracted_at": datetime.now().isoformat(),
    }
```

</details>

<details>
<summary>Hint 2: Creating dependent Asset</summary>

```python
@asset(
    uri="s3://data-lake/silver/transformed_data",
    schedule=[raw_data],  # Depends on raw_data Asset
)
def transformed_data(raw_data):  # Receives raw_data output
    """Transform the raw data."""
    print(f"Transforming {len(raw_data['records'])} records...")
    return {
        "records": transformed_records,
        "transformations": ["clean", "normalize"],
    }
```

</details>

<details>
<summary>Hint 3: Chain of dependencies</summary>

```python
# raw_data → transformed_data → warehouse_data

@asset(uri="...", schedule="@daily")
def raw_data():
    return extract()

@asset(uri="...", schedule=[raw_data])
def transformed_data(raw_data):
    return transform(raw_data)

@asset(uri="...", schedule=[transformed_data])
def warehouse_data(transformed_data):
    return load(transformed_data)
```

</details>

## Success Criteria

- [ ] Three Assets are created using @asset decorator
- [ ] raw_data runs on daily schedule
- [ ] transformed_data triggers when raw_data updates
- [ ] warehouse_data triggers when transformed_data updates
- [ ] Data flows correctly through the pipeline
- [ ] You understand the benefits of @asset pattern

## Benefits of @asset Pattern

| Benefit | Description |
|---------|-------------|
| **Colocation** | Asset and producer defined together |
| **Self-documenting** | Dependencies visible in function signature |
| **Type hints** | IDE support for data flow |
| **Simpler code** | Less boilerplate than traditional pattern |
| **Clear lineage** | Data flow obvious from code structure |

## Comparison

### Traditional Pattern
```python
raw_asset = Asset("s3://bronze/raw")
transformed_asset = Asset("s3://silver/transformed")

@dag(schedule="@daily", ...)
def extract_dag():
    @task(outlets=[raw_asset])
    def extract():
        pass

@dag(schedule=[raw_asset], ...)
def transform_dag():
    @task(outlets=[transformed_asset])
    def transform():
        pass
```

### @asset Pattern
```python
@asset(uri="s3://bronze/raw", schedule="@daily")
def raw_data():
    pass

@asset(uri="s3://silver/transformed", schedule=[raw_data])
def transformed_data(raw_data):
    pass
```

The @asset pattern reduces boilerplate and makes data lineage immediately visible.
