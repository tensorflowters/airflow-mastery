# Exercise 7.2: Unit Test a Pipeline

## Objective

Write comprehensive unit tests for an ETL pipeline, including mocking external dependencies and testing edge cases.

## Background

Unit testing DAG tasks ensures:
- Business logic is correct
- Edge cases are handled
- Regressions are caught early
- Code is maintainable and refactorable

### Testing TaskFlow Functions

TaskFlow functions are regular Python functions, making them easy to test:

```python
# In your DAG file
@task
def transform_data(raw: dict) -> dict:
    return {"processed": raw["value"] * 2}

# In your test file
def test_transform_data():
    result = transform_data.function({"value": 5})
    assert result == {"processed": 10}
```

## Requirements

Create tests for an ETL pipeline with these tasks:

1. **extract_data**: Fetches data from an API
2. **transform_data**: Cleans and transforms data
3. **load_data**: Loads data to a destination

### Test Categories

1. **Happy Path Tests**
   - Normal data processing
   - Expected transformations

2. **Edge Case Tests**
   - Empty data
   - Missing fields
   - Null values

3. **Mocked Dependency Tests**
   - API calls
   - Database connections
   - File system operations

## Key Concepts

### Accessing Task Functions

```python
# TaskFlow decorator wraps the function
# Access the original function with .function
result = my_task.function(input_data)
```

### Mocking External Calls

```python
from unittest.mock import patch, MagicMock

@patch('requests.get')
def test_extract_with_mock(mock_get):
    mock_get.return_value.json.return_value = {"data": [1, 2, 3]}
    mock_get.return_value.status_code = 200

    result = extract_data.function()
    assert len(result["data"]) == 3
```

### Testing with pytest Fixtures

```python
@pytest.fixture
def sample_data():
    return {
        "records": [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
        ]
    }

def test_transform(sample_data):
    result = transform_data.function(sample_data)
    assert result["total"] == 300
```

## Starter Code

See `exercise_7_2_unit_testing_starter.py`

## Testing Your Tests

```bash
# Run the unit tests
pytest modules/07-testing-debugging/exercises/test_etl_pipeline.py -v

# Run with coverage
pytest --cov=dags --cov-report=html
```

## Hints

<details>
<summary>Hint 1: Testing transform logic</summary>

```python
def test_transform_calculates_total():
    input_data = {
        "records": [
            {"id": 1, "amount": 100},
            {"id": 2, "amount": 200},
        ]
    }

    result = transform_data.function(input_data)

    assert result["total_amount"] == 300
    assert result["record_count"] == 2
```

</details>

<details>
<summary>Hint 2: Testing empty data handling</summary>

```python
def test_transform_handles_empty_data():
    result = transform_data.function({"records": []})

    assert result["total_amount"] == 0
    assert result["record_count"] == 0
```

</details>

<details>
<summary>Hint 3: Mocking API calls</summary>

```python
from unittest.mock import patch

@patch('requests.get')
def test_extract_api_call(mock_get):
    # Set up mock response
    mock_get.return_value.json.return_value = {
        "data": [{"id": 1}]
    }
    mock_get.return_value.status_code = 200

    result = extract_data.function()

    # Verify the mock was called
    mock_get.assert_called_once()
    assert len(result["data"]) == 1
```

</details>

## Success Criteria

- [ ] Happy path tests pass
- [ ] Empty data is handled gracefully
- [ ] Missing fields don't cause crashes
- [ ] External calls are properly mocked
- [ ] Tests are fast (no real external calls)
- [ ] Coverage is at least 80%

## Test Structure Best Practices

```
tests/
├── conftest.py          # Shared fixtures
├── test_dag_integrity.py
└── dags/
    ├── test_etl_pipeline.py
    ├── test_data_quality.py
    └── fixtures/
        ├── sample_data.json
        └── expected_output.json
```
