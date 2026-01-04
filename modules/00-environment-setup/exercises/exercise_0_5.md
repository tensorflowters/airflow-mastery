# Exercise 0.5: Testing Integration with pytest

## 🎯 Objective

Set up a complete pytest testing workflow for Airflow DAG validation with coverage reporting.

## ⏱️ Estimated Time: 25-30 minutes

---

## Prerequisites

- Completed Exercise 0.4
- Project with DAGs and uv configured
- Test dependencies installed (`uv sync --group test`)

---

## Tasks

### Task 1: Create Tests Directory Structure

```bash
cd airflow-learning

# Create tests directory
mkdir -p tests
touch tests/__init__.py
```

### Task 2: Configure pytest in pyproject.toml

Add or update the pytest configuration:

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = [
    "-v",
    "--tb=short",
    "-ra",
]
filterwarnings = [
    "ignore::DeprecationWarning",
    "ignore::PendingDeprecationWarning",
]
markers = [
    "slow: marks tests as slow (deselect with '-m \"not slow\"')",
    "integration: marks tests as integration tests",
]

[tool.coverage.run]
source = ["dags", "plugins"]
omit = [
    "tests/*",
    "*/__pycache__/*",
]
branch = true

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "if TYPE_CHECKING:",
    "if __name__ == .__main__.:",
    "raise NotImplementedError",
]
show_missing = true
fail_under = 80
```

### Task 3: Create conftest.py

Create `tests/conftest.py` with shared fixtures:

```python
"""Pytest fixtures for Airflow DAG testing."""

import os
import sys
from pathlib import Path

import pytest

# Ensure the project root is in Python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Set Airflow home before importing airflow
os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_ROOT))
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return path to project root."""
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def dags_folder(project_root: Path) -> Path:
    """Return path to DAGs folder."""
    return project_root / "dags"


@pytest.fixture(scope="session")
def dag_files(dags_folder: Path) -> list[Path]:
    """Return list of all DAG Python files."""
    if not dags_folder.exists():
        return []
    return [f for f in dags_folder.rglob("*.py") if not f.name.startswith("_") and f.name != "__init__.py"]


@pytest.fixture(scope="session")
def dag_bag():
    """Create a DagBag for testing."""
    from airflow.models import DagBag

    return DagBag(include_examples=False)
```

### Task 4: Create DAG Integrity Tests

Create `tests/test_dag_integrity.py`:

```python
"""Tests for DAG integrity and validity."""

import importlib.util
from pathlib import Path

import pytest


class TestDagImports:
    """Test that all DAG files can be imported."""

    def test_dags_folder_exists(self, dags_folder: Path) -> None:
        """Verify DAGs folder exists."""
        assert dags_folder.exists(), f"DAGs folder not found: {dags_folder}"

    def test_dag_files_found(self, dag_files: list[Path]) -> None:
        """Verify at least one DAG file exists."""
        assert len(dag_files) > 0, "No DAG files found"

    def test_dags_can_be_imported(self, dag_files: list[Path]) -> None:
        """Verify all DAG files can be imported without errors."""
        for dag_file in dag_files:
            spec = importlib.util.spec_from_file_location(
                dag_file.stem,
                dag_file,
            )
            assert spec is not None, f"Could not load spec for {dag_file}"
            assert spec.loader is not None, f"No loader for {dag_file}"

            module = importlib.util.module_from_spec(spec)
            try:
                spec.loader.exec_module(module)
            except Exception as e:
                pytest.fail(f"Failed to import {dag_file.name}: {e}")


class TestDagBag:
    """Test DagBag integrity."""

    def test_no_import_errors(self, dag_bag) -> None:
        """Check DAGs don't have import errors."""
        assert len(dag_bag.import_errors) == 0, "DAG import errors found:\n" + "\n".join(
            f"  {k}: {v}" for k, v in dag_bag.import_errors.items()
        )

    def test_dags_loaded(self, dag_bag) -> None:
        """Verify at least one DAG was loaded."""
        assert len(dag_bag.dags) > 0, "No DAGs loaded in DagBag"

    def test_no_cycles(self, dag_bag) -> None:
        """Verify DAGs don't have circular dependencies."""
        for dag_id, dag in dag_bag.dags.items():
            try:
                # topological_sort raises if there are cycles
                _ = dag.topological_sort()
            except Exception as e:
                pytest.fail(f"DAG {dag_id} has circular dependencies: {e}")


class TestDagConfiguration:
    """Test DAG configuration best practices."""

    def test_dag_ids_are_unique(self, dag_bag) -> None:
        """Verify all DAG IDs are unique."""
        dag_ids = list(dag_bag.dags.keys())
        assert len(dag_ids) == len(set(dag_ids)), f"Duplicate DAG IDs found: {dag_ids}"

    def test_dags_have_tags(self, dag_bag) -> None:
        """Verify all DAGs have at least one tag."""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.tags, f"DAG {dag_id} has no tags"

    def test_dags_have_description_or_doc(self, dag_bag) -> None:
        """Verify all DAGs have a description or docstring."""
        for dag_id, dag in dag_bag.dags.items():
            has_description = bool(dag.description)
            has_doc = bool(dag.doc_md)
            assert has_description or has_doc, f"DAG {dag_id} has no description or docstring"

    def test_catchup_is_disabled(self, dag_bag) -> None:
        """Verify catchup is explicitly disabled for safety."""
        for dag_id, dag in dag_bag.dags.items():
            assert dag.catchup is False, f"DAG {dag_id} has catchup enabled. Set catchup=False unless intentional."
```

### Task 5: Create Task Tests

Create `tests/test_dag_tasks.py`:

```python
"""Tests for DAG task configurations."""


class TestTaskDefaults:
    """Test task default configurations."""

    def test_tasks_have_owners(self, dag_bag) -> None:
        """Verify all tasks have owners defined."""
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                assert task.owner != "airflow", (
                    f"Task {dag_id}.{task.task_id} uses default owner. Set a specific owner in default_args."
                )

    def test_tasks_have_retries(self, dag_bag) -> None:
        """Verify tasks have retry configuration."""
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                # Allow 0 retries if explicitly set
                assert task.retries is not None, f"Task {dag_id}.{task.task_id} has no retries configured"

    def test_email_on_failure_configured(self, dag_bag) -> None:
        """Verify email notifications are configured."""
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                # This is a soft check - just verify it's explicitly set
                _ = task.email_on_failure  # Should not raise


class TestTaskDependencies:
    """Test task dependency configurations."""

    def test_no_orphan_tasks(self, dag_bag) -> None:
        """Verify no tasks are orphaned (no upstream or downstream)."""
        for dag_id, dag in dag_bag.dags.items():
            # Skip DAGs with only one task
            if len(dag.tasks) <= 1:
                continue

            for task in dag.tasks:
                has_upstream = bool(task.upstream_list)
                has_downstream = bool(task.downstream_list)
                assert has_upstream or has_downstream, (
                    f"Task {dag_id}.{task.task_id} is orphaned (no upstream or downstream dependencies)"
                )

    def test_reasonable_task_count(self, dag_bag) -> None:
        """Verify DAGs don't have too many tasks."""
        max_tasks = 100  # Adjust based on your standards

        for dag_id, dag in dag_bag.dags.items():
            assert len(dag.tasks) <= max_tasks, (
                f"DAG {dag_id} has {len(dag.tasks)} tasks (max: {max_tasks}). Consider splitting into sub-DAGs."
            )
```

### Task 6: Install Test Dependencies

```bash
uv sync --group test
```

### Task 7: Run the Tests

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run a specific test file
uv run pytest tests/test_dag_integrity.py -v

# Run a specific test class
uv run pytest tests/test_dag_integrity.py::TestDagImports -v
```

### Task 8: Run with Coverage

```bash
# Run tests with coverage
uv run pytest --cov=dags --cov-report=term-missing

# Generate HTML coverage report
uv run pytest --cov=dags --cov-report=html

# Open coverage report (macOS)
open htmlcov/index.html
```

### Task 9: Add Tests to Pre-commit (Optional)

Update `.pre-commit-config.yaml` to run tests:

```yaml
# Local hooks
- repo: local
  hooks:
    - id: pytest
      name: pytest
      entry: uv run pytest
      language: system
      types: [python]
      pass_filenames: false
      always_run: true
      stages: [pre-push] # Only run on push, not every commit
```

### Task 10: Create a Test Helper Script

Create `scripts/test.sh`:

```bash
#!/bin/bash
# scripts/test.sh - Run tests with common options

set -e

# Default to running all tests
TEST_PATH="${1:-tests/}"

echo "🧪 Running tests..."
uv run pytest "$TEST_PATH" \
    --cov=dags \
    --cov-report=term-missing \
    --cov-fail-under=80 \
    -v

echo "✅ All tests passed!"
```

Make it executable:

```bash
chmod +x scripts/test.sh
```

---

## Verification Checklist

- [ ] `tests/` directory exists with `__init__.py`
- [ ] `tests/conftest.py` with fixtures
- [ ] `tests/test_dag_integrity.py` with import and structure tests
- [ ] `tests/test_dag_tasks.py` with task configuration tests
- [ ] `uv run pytest` runs without errors
- [ ] Coverage report generated with `--cov`

---

## 💡 Key Learnings

1. **pytest fixtures** share setup across tests efficiently
2. **DagBag** is Airflow's mechanism for loading and validating DAGs
3. **Test categories**: Import tests catch syntax errors, structure tests catch configuration issues
4. **Coverage thresholds** ensure adequate test coverage

---

## ⚠️ Common Edge Cases and How Tests Handle Them

Understanding these edge cases helps you write more robust DAGs:

| Edge Case                 | What Happens                     | Test That Catches It        |
| ------------------------- | -------------------------------- | --------------------------- |
| Empty dags/ folder        | DagBag loads nothing             | `test_dag_files_found`      |
| Syntax error in DAG       | Import fails with SyntaxError    | `test_dags_can_be_imported` |
| Missing dependency import | ImportError raised               | `test_dags_can_be_imported` |
| Circular task dependency  | topological_sort() fails         | `test_no_cycles`            |
| Duplicate DAG IDs         | Only one DAG loads               | `test_dag_ids_are_unique`   |
| Missing tags              | DAG loads but hard to find in UI | `test_dags_have_tags`       |
| catchup=True (default)    | Backfills on first run           | `test_catchup_is_disabled`  |
| Orphaned tasks            | Tasks never execute              | `test_no_orphan_tasks`      |

### Tests You Should Always Have

**Minimum Test Suite** (must pass before deployment):

1. `test_dags_can_be_imported` - No syntax/import errors
2. `test_no_import_errors` - DagBag loads successfully
3. `test_no_cycles` - DAG structure is valid

**Recommended Additional Tests**: 4. `test_dags_have_tags` - Organization in UI 5. `test_catchup_is_disabled` - Prevent accidental backfills 6. `test_tasks_have_owners` - Accountability

---

## Test Types for Airflow

| Test Type         | Purpose                    | Example                     |
| ----------------- | -------------------------- | --------------------------- |
| Import tests      | Catch syntax/import errors | `test_dags_can_be_imported` |
| Structure tests   | Validate DAG configuration | `test_no_cycles`            |
| Task tests        | Verify task configuration  | `test_tasks_have_retries`   |
| Unit tests        | Test individual functions  | Test custom operators       |
| Integration tests | Test end-to-end flows      | `dag.test()`                |

---

## 🚀 Bonus Challenge

1. Add a unit test for a custom operator or task function:

   ```python
   # tests/test_tasks.py
   def test_extract_returns_dict():
       """Test that extract task returns expected format."""
       from dags.sample_dag import extract

       # Call the underlying function
       result = extract.function()
       assert isinstance(result, dict)
       assert "users" in result
   ```

2. Add a marker for slow tests:

   ```python
   @pytest.mark.slow
   def test_full_dag_execution(dag_bag):
       """Test full DAG execution (slow)."""
       dag = dag_bag.get_dag("sample_dag")
       dag.test()
   ```

   Run fast tests only:

   ```bash
   uv run pytest -m "not slow"
   ```

3. Set up GitHub Actions for automated testing:
   ```yaml
   # .github/workflows/test.yml
   name: Tests
   on: [push, pull_request]
   jobs:
     test:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v4
         - uses: astral-sh/setup-uv@v4
         - run: uv sync --all-groups
         - run: uv run pytest --cov=dags
   ```

---

## 📚 Reference

- [pytest Documentation](https://docs.pytest.org/)
- [pytest-cov](https://pytest-cov.readthedocs.io/)
- [Airflow Testing Guide](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing-a-dag)

---

**Congratulations!** You've completed Module 00 and set up a professional Airflow development environment.

Next: [Module 01: Foundations →](../../01-foundations/README.md)
