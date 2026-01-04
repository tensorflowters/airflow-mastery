# Exercise 7.1: DAG Integrity Tests

## Objective

Create a comprehensive test suite that validates DAG structure, configuration, and coding standards.

## Background

DAG integrity tests run automatically in CI/CD pipelines to catch issues before deployment:
- Import errors that prevent DAGs from loading
- Missing required configurations (owner, tags)
- Naming convention violations
- Potentially problematic configurations

### Why Integrity Tests Matter

| Issue | Cost to Fix |
|-------|------------|
| Caught in PR | Minutes |
| Caught in staging | Hours |
| Caught in production | Days + incidents |

## Requirements

Create a test file `test_dag_integrity.py` that validates:

1. **All DAGs Load Successfully**
   - No import errors
   - No syntax errors
   - All dependencies available

2. **Owner Configuration**
   - All DAGs have an `owner` in default_args or tags
   - Owner is not the default "airflow"

3. **Schedule Documentation**
   - DAGs with `schedule=None` must have a description
   - Undocumented manual-trigger DAGs are flagged

4. **Task ID Naming**
   - All task IDs use snake_case
   - No special characters (except underscore)

## Key Concepts

### Using DagBag for Testing

```python
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    assert len(dag_bag.import_errors) == 0, dag_bag.import_errors
```

### Iterating Over DAGs

```python
dag_bag = DagBag(...)
for dag_id, dag in dag_bag.dags.items():
    # Access DAG properties
    owner = dag.default_args.get("owner")
    schedule = dag.schedule_interval
    tasks = dag.tasks
```

### Checking Task IDs

```python
import re

def is_snake_case(name: str) -> bool:
    return bool(re.match(r'^[a-z][a-z0-9_]*$', name))

for task in dag.tasks:
    assert is_snake_case(task.task_id)
```

## Starter Code

See `exercise_7_1_integrity_tests_starter.py`

## Testing Your Tests

```bash
# Run the integrity tests
pytest modules/07-testing-debugging/exercises/test_dag_integrity.py -v

# Or run against your dags folder
pytest tests/test_dag_integrity.py -v
```

## Hints

<details>
<summary>Hint 1: Setting up DagBag</summary>

```python
import os
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dag_bag():
    """Load all DAGs for testing."""
    dag_folder = os.path.join(os.path.dirname(__file__), "..", "..", "..", "dags")
    return DagBag(dag_folder=dag_folder, include_examples=False)
```

</details>

<details>
<summary>Hint 2: Parametrized tests for each DAG</summary>

```python
@pytest.mark.parametrize("dag_id", dag_bag.dags.keys())
def test_dag_has_owner(dag_bag, dag_id):
    dag = dag_bag.dags[dag_id]
    owner = dag.default_args.get("owner")
    assert owner is not None, f"DAG {dag_id} missing owner"
    assert owner != "airflow", f"DAG {dag_id} uses default owner"
```

</details>

<details>
<summary>Hint 3: Checking schedule documentation</summary>

```python
def test_manual_dags_documented(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        if dag.schedule_interval is None:
            assert dag.description, (
                f"DAG {dag_id} has schedule=None but no description"
            )
```

</details>

## Success Criteria

- [ ] Test suite catches import errors
- [ ] Test verifies owner is set and not default
- [ ] Test flags undocumented manual-trigger DAGs
- [ ] Test validates snake_case task IDs
- [ ] Tests can be run in CI/CD
- [ ] Clear error messages indicate what to fix

## Real-World Test Patterns

| Test | Purpose |
|------|---------|
| Import errors | Catch syntax and dependency issues |
| Required tags | Ensure DAGs are categorized |
| Email on failure | Verify alerting is configured |
| Retries configured | Production resilience |
| Pool assignments | Resource management validation |
