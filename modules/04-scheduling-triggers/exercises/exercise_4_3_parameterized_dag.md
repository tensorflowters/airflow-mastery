# Exercise 4.3: Parameterized DAG

## Objective

Create a data processing DAG that accepts runtime parameters, allowing users to customize pipeline behavior without modifying code.

## Background

Airflow 3.x provides powerful parameterization through the `Param` class, enabling:
- Type-safe parameter validation
- Default values and required parameters
- Enum constraints for limited choices
- Parameter descriptions for documentation
- UI-friendly parameter forms

### Param Class

```python
from airflow.sdk.definitions.param import Param

@dag(
    params={
        "my_param": Param(
            default="value",
            type="string",
            description="Parameter description",
        ),
    }
)
```

### Accessing Parameters

```python
@task
def my_task(**context):
    params = context["params"]
    value = params["my_param"]
```

Or with Jinja templating:
```python
BashOperator(
    task_id="bash_task",
    bash_command="echo {{ params.my_param }}",
)
```

## Requirements

Your DAG should:
1. Be named `exercise_4_3_parameterized_dag`
2. Have a start date of January 1, 2024
3. Use `schedule=None` (manual trigger only)
4. Include tags: `["exercise", "module-04", "params"]`

### Required Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `source_table` | string | Yes | - | Source table to read from |
| `target_table` | string | Yes | - | Target table to write to |
| `mode` | enum | No | "append" | Write mode: "append" or "overwrite" |
| `limit` | integer | No | null | Row limit (null = no limit) |

### Task Structure

1. **validate_params**: Validate all parameters and print configuration
2. **extract_data**: Simulate reading from source table with optional limit
3. **transform_data**: Process the extracted data
4. **load_data**: Simulate writing to target table with specified mode
5. **log_completion**: Log the completed operation details

## Key Concepts

### Defining Parameters

```python
@dag(
    params={
        "required_param": Param(
            type="string",
            description="This parameter is required",
        ),
        "optional_param": Param(
            default="default_value",
            type="string",
            description="This parameter has a default",
        ),
        "enum_param": Param(
            default="option1",
            type="string",
            enum=["option1", "option2", "option3"],
            description="Choose from predefined options",
        ),
        "nullable_int": Param(
            default=None,
            type=["null", "integer"],
            description="Optional integer (can be null)",
        ),
    }
)
```

### Parameter Types

- `"string"` - Text values
- `"integer"` - Whole numbers
- `"number"` - Decimal numbers
- `"boolean"` - True/False
- `"array"` - Lists
- `"object"` - Dictionaries
- `["null", "type"]` - Nullable values

## Starter Code

See `exercise_4_3_parameterized_dag_starter.py`

## Testing Your DAG

### Via CLI

```bash
# Test with required parameters only
airflow dags test exercise_4_3_parameterized_dag 2024-01-15 \
  -c '{"source_table": "raw_events", "target_table": "processed_events"}'

# Test with all parameters
airflow dags test exercise_4_3_parameterized_dag 2024-01-15 \
  -c '{"source_table": "raw_events", "target_table": "processed_events", "mode": "overwrite", "limit": 1000}'
```

### Via UI

1. Go to the DAG in the Airflow UI
2. Click "Trigger DAG" button
3. Fill in the parameter form
4. Click "Trigger"

## Hints

<details>
<summary>Hint 1: Defining required string parameter</summary>

```python
"source_table": Param(
    type="string",
    description="Source table name to read data from",
    minLength=1,  # Ensures non-empty string
),
```

</details>

<details>
<summary>Hint 2: Defining enum parameter</summary>

```python
"mode": Param(
    default="append",
    type="string",
    enum=["append", "overwrite"],
    description="Write mode for target table",
),
```

</details>

<details>
<summary>Hint 3: Defining nullable integer</summary>

```python
"limit": Param(
    default=None,
    type=["null", "integer"],
    minimum=1,  # If provided, must be at least 1
    description="Row limit (null for no limit)",
),
```

</details>

<details>
<summary>Hint 4: Accessing params in task</summary>

```python
@task
def extract_data(**context):
    params = context["params"]
    source = params["source_table"]
    limit = params.get("limit")  # May be None

    if limit:
        print(f"Reading {limit} rows from {source}")
    else:
        print(f"Reading all rows from {source}")
```

</details>

## Success Criteria

- [ ] DAG accepts `source_table` and `target_table` as required parameters
- [ ] DAG accepts `mode` with enum validation (append/overwrite)
- [ ] DAG accepts `limit` as optional nullable integer
- [ ] Parameters are validated before processing begins
- [ ] Tasks correctly use parameter values
- [ ] DAG can be triggered from UI with parameter form
- [ ] You can explain how to add new parameters

## Real-World Use Cases

| Use Case | Parameters |
|----------|------------|
| ETL Pipeline | source_db, target_db, schema, batch_size |
| Report Generation | report_date, format, recipients |
| Data Backfill | start_date, end_date, partitions |
| API Sync | api_endpoint, page_size, incremental |
| ML Training | model_type, hyperparameters, dataset |
