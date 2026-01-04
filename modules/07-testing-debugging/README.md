# Module 07: Testing & Debugging

## 🎯 Learning Objectives

By the end of this module, you will:
- Write unit tests for DAGs and tasks
- Use `dag.test()` for local testing
- Debug DAGs using logs and the UI
- Validate DAG integrity before deployment
- Set up CI/CD testing pipelines

## ⏱️ Estimated Time: 4-5 hours

---

## 1. Types of Airflow Tests

| Test Type | Purpose | Tools |
|-----------|---------|-------|
| **DAG Integrity** | DAG loads without errors | pytest, `airflow dags list` |
| **Unit Tests** | Task logic works correctly | pytest, mocks |
| **Integration Tests** | Tasks work with real services | pytest, test environments |
| **End-to-End** | Full DAG runs successfully | `dag.test()`, staging env |

---

## 2. DAG Integrity Tests

The minimum test every DAG should have:

```python
# tests/test_dag_integrity.py
import pytest
from airflow.models import DagBag

def test_dagbag_loads_without_errors():
    """Verify all DAGs load without import errors"""
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    
    assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"

def test_dags_have_tags():
    """Verify all DAGs have at least one tag"""
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG {dag_id} has no tags"

def test_dags_have_description():
    """Verify all DAGs have descriptions"""
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    
    for dag_id, dag in dagbag.dags.items():
        assert dag.description, f"DAG {dag_id} has no description"

def test_no_cycles():
    """Verify DAGs don't have cycles (should never happen, but check)"""
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    
    for dag_id, dag in dagbag.dags.items():
        # If DAG loaded, it has no cycles (Airflow validates this)
        assert dag is not None
```

---

## 3. Unit Testing Tasks

### Testing TaskFlow Functions

```python
# tests/test_etl_tasks.py
import pytest
from dags.etl_pipeline import extract_data, transform_data, validate_data

class TestExtractData:
    def test_returns_expected_format(self):
        """Test extract returns proper structure"""
        # Call the underlying function directly
        result = extract_data.function()
        
        assert isinstance(result, dict)
        assert "records" in result
        assert "timestamp" in result
    
    def test_handles_empty_source(self, mocker):
        """Test behavior with empty data source"""
        # Mock external calls
        mocker.patch("dags.etl_pipeline.fetch_from_api", return_value=[])
        
        result = extract_data.function()
        
        assert result["records"] == []

class TestTransformData:
    def test_transforms_correctly(self):
        """Test transformation logic"""
        input_data = {"records": [{"name": "test", "value": 10}]}
        
        result = transform_data.function(input_data)
        
        assert result["transformed"][0]["value"] == 20  # Doubled
    
    def test_handles_missing_fields(self):
        """Test graceful handling of missing fields"""
        input_data = {"records": [{"name": "test"}]}  # No 'value'
        
        result = transform_data.function(input_data)
        
        assert result["transformed"][0]["value"] == 0  # Default
```

### Mocking External Dependencies

```python
import pytest
from unittest.mock import Mock, patch

def test_database_task(mocker):
    """Test task that uses database connection"""
    # Mock the hook
    mock_hook = Mock()
    mock_hook.get_records.return_value = [("row1",), ("row2",)]
    
    mocker.patch(
        "dags.my_dag.PostgresHook",
        return_value=mock_hook
    )
    
    from dags.my_dag import query_database
    result = query_database.function()
    
    assert len(result) == 2
    mock_hook.get_records.assert_called_once()

def test_s3_task(mocker):
    """Test task that uses S3"""
    mock_hook = Mock()
    mock_hook.read_key.return_value = '{"data": "test"}'
    
    mocker.patch(
        "dags.my_dag.S3Hook",
        return_value=mock_hook
    )
    
    from dags.my_dag import read_from_s3
    result = read_from_s3.function("bucket", "key")
    
    assert result == {"data": "test"}
```

---

## 4. Using dag.test()

Airflow 2.5+ provides `dag.test()` for local testing:

```python
# Run from command line
airflow dags test my_dag 2024-01-01

# Or in Python
from dags.my_dag import dag

if __name__ == "__main__":
    dag.test()
```

### Testing with Parameters

```bash
airflow dags test my_dag 2024-01-01 \
    --conf '{"param1": "value1", "param2": 42}'
```

### Testing Specific Tasks

```bash
# Test a single task
airflow tasks test my_dag my_task 2024-01-01

# Test with verbose output
airflow tasks test my_dag my_task 2024-01-01 -v
```

---

## 5. Debugging Techniques

### Reading Logs

```bash
# Scheduler logs
airflow scheduler --log-file /var/log/airflow/scheduler.log

# Task logs location (default)
# $AIRFLOW_HOME/logs/dag_id/task_id/execution_date/

# Via CLI
airflow tasks logs my_dag my_task 2024-01-01 --local
```

### Adding Debug Logging

```python
from airflow.sdk import task
import logging

@task
def my_task():
    logger = logging.getLogger("airflow.task")
    
    logger.debug("Debug details: %s", some_var)  # Won't show by default
    logger.info("Processing started")
    logger.warning("Something unexpected but not fatal")
    logger.error("Something went wrong: %s", error_details)
    
    # Or use print (also captured in logs)
    print("This appears in task logs too")
```

### Setting Log Levels

```bash
# Increase verbosity
export AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG

# Or in airflow.cfg
[logging]
logging_level = DEBUG
```

### Using the UI Debugger

1. **Grid View**: See task states and history
2. **Graph View**: Visualize dependencies
3. **Task Instance Details**: Click any task → "Log" tab
4. **XCom Tab**: Inspect passed data
5. **Rendered Template**: See actual values after Jinja rendering

### Common Issues and Solutions

| Issue | Symptom | Solution |
|-------|---------|----------|
| Import Error | DAG not appearing | Check `airflow dags list` for errors |
| Task Stuck | Always "running" | Check worker logs, increase timeout |
| XCom Too Large | Serialization error | Pass references, not data |
| Dependency Cycle | DAG won't load | Check task dependencies |
| Template Error | Task fails immediately | Check "Rendered Template" in UI |

---

## 6. Testing Configuration

### pytest Configuration

```ini
# pytest.ini
[pytest]
testpaths = tests
python_files = test_*.py
python_functions = test_*
addopts = -v --tb=short
filterwarnings =
    ignore::DeprecationWarning

# Environment variables for tests
env =
    AIRFLOW_HOME=./tests/airflow_home
    AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### Fixtures

```python
# tests/conftest.py
import pytest
import os
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dagbag():
    """Shared DagBag for all tests"""
    return DagBag(dag_folder="dags/", include_examples=False)

@pytest.fixture
def mock_context():
    """Mock Airflow context for testing"""
    from datetime import datetime
    from pendulum import DateTime
    from unittest.mock import Mock
    
    ti = Mock()
    ti.xcom_pull = Mock(return_value=None)
    ti.xcom_push = Mock()
    
    return {
        "ti": ti,
        "ds": "2024-01-01",
        "logical_date": DateTime(2024, 1, 1),
        "data_interval_start": DateTime(2024, 1, 1),
        "data_interval_end": DateTime(2024, 1, 2),
        "params": {},
    }

@pytest.fixture
def temp_dag_folder(tmp_path):
    """Temporary folder for testing DAG files"""
    dag_folder = tmp_path / "dags"
    dag_folder.mkdir()
    return dag_folder
```

---

## 7. CI/CD Integration

### GitHub Actions Example

```yaml
# .github/workflows/airflow-tests.yml
name: Airflow Tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-mock
      
      - name: Lint DAGs with Ruff
        run: |
          pip install ruff
          ruff check dags/ --select AIR
      
      - name: Run DAG integrity tests
        run: |
          export AIRFLOW_HOME=$(pwd)/tests/airflow_home
          airflow db init
          pytest tests/test_dag_integrity.py -v
      
      - name: Run unit tests
        run: |
          pytest tests/ -v --ignore=tests/test_dag_integrity.py
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.1.5
    hooks:
      - id: ruff
        args: [--select, AIR, --fix]
      - id: ruff-format

  - repo: local
    hooks:
      - id: dag-integrity
        name: DAG Integrity Check
        entry: python -c "from airflow.models import DagBag; db=DagBag('dags/'); assert not db.import_errors, db.import_errors"
        language: system
        pass_filenames: false
        files: ^dags/.*\.py$
```

---

## 📝 Exercises

### Exercise 7.1: Write Integrity Tests
Create a test suite that validates:
- All DAGs load without errors
- All DAGs have an owner in default_args
- No DAG has schedule=None without explicit documentation
- All task IDs follow naming convention (snake_case)

### Exercise 7.2: Unit Test a Pipeline
Take the ETL DAG from Module 02 and write:
- Unit tests for each task function
- Tests with mocked external dependencies
- Edge case tests (empty data, malformed data)

### Exercise 7.3: Debug a Broken DAG
A deliberately broken DAG is provided in `exercises/broken_dag.py`. Use debugging techniques to:
- Identify all issues
- Fix them
- Add tests to prevent regression

---

## ✅ Checkpoint

Before moving to Module 08, ensure you can:

- [ ] Write DAG integrity tests with pytest
- [ ] Unit test TaskFlow functions
- [ ] Mock external dependencies in tests
- [ ] Use `dag.test()` and `tasks test` commands
- [ ] Find and read task logs
- [ ] Set up basic CI/CD for DAG validation

---

Next: [Module 08: Kubernetes Executor →](../08-kubernetes-executor/README.md)
