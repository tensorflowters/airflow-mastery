"""Solution 0.5: Pytest fixtures for Airflow DAG testing.

This conftest.py provides shared fixtures for testing Airflow DAGs.
Place this file in your tests/ directory.

Requires Python 3.9+ (uses modern type hints with __future__ annotations).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from airflow.models import DagBag

# ============================================
# Path Setup
# ============================================

# Get the project root directory (parent of tests/)
PROJECT_ROOT = Path(__file__).parent.parent

# Add project root to Python path so we can import from dags/ and plugins/
sys.path.insert(0, str(PROJECT_ROOT))

# ============================================
# Environment Setup
# ============================================

# Set Airflow home before importing airflow modules
os.environ.setdefault("AIRFLOW_HOME", str(PROJECT_ROOT))

# Enable unit test mode (disables some Airflow features for faster tests)
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")

# Disable loading example DAGs
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")


# ============================================
# Fixtures
# ============================================


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Return path to project root directory.

    Returns:
        Path: Absolute path to the project root.
    """
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def dags_folder(project_root: Path) -> Path:
    """Return path to DAGs folder.

    Args:
        project_root: Path to the project root directory.

    Returns:
        Path: Absolute path to the dags/ directory.
    """
    return project_root / "dags"


@pytest.fixture(scope="session")
def plugins_folder(project_root: Path) -> Path:
    """Return path to plugins folder.

    Args:
        project_root: Path to the project root directory.

    Returns:
        Path: Absolute path to the plugins/ directory.
    """
    return project_root / "plugins"


@pytest.fixture(scope="session")
def dag_files(dags_folder: Path) -> list[Path]:
    """Return list of all DAG Python files.

    Excludes:
    - Files starting with underscore (_)
    - __init__.py files
    - Files in __pycache__ directories

    Args:
        dags_folder: Path to the dags/ directory.

    Returns:
        list[Path]: List of DAG file paths.
    """
    if not dags_folder.exists():
        return []

    return [
        f
        for f in dags_folder.rglob("*.py")
        if not f.name.startswith("_")
        and f.name != "__init__.py"
        and "__pycache__" not in str(f)
    ]


@pytest.fixture(scope="session")
def dag_bag() -> DagBag:
    """Create a DagBag for testing.

    The DagBag is Airflow's mechanism for loading and managing DAGs.
    Using session scope means the DagBag is created once and reused
    across all tests for better performance.

    Returns:
        DagBag: Loaded DagBag with project DAGs.
    """
    from airflow.models import DagBag

    return DagBag(include_examples=False)


@pytest.fixture
def sample_execution_date() -> datetime:
    """Return a sample execution date for testing.

    Returns a timezone-aware datetime for consistent test results.
    Using UTC ensures tests are deterministic regardless of local timezone.

    Returns:
        datetime: A fixed timezone-aware datetime for testing.
    """
    return datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def mock_context(sample_execution_date: datetime) -> dict:
    """Create a mock Airflow context for testing tasks.

    This fixture provides a comprehensive context dictionary that can be
    used when testing tasks that require context. Updated for Airflow 3.x
    compatibility with modern context fields.

    Note:
        For more complex testing scenarios, consider using Airflow's
        built-in testing utilities or the dag.test() method.

    Args:
        sample_execution_date: The execution date to use in the context.

    Returns:
        dict: Mock context dictionary with Airflow 3.x fields.
    """
    # Calculate data interval (for daily schedule, interval is 24 hours before logical_date)
    data_interval_end = sample_execution_date
    data_interval_start = datetime(2023, 12, 31, 0, 0, 0, tzinfo=timezone.utc)

    return {
        # Legacy fields (for backward compatibility)
        "ds": sample_execution_date.strftime("%Y-%m-%d"),
        "ds_nodash": sample_execution_date.strftime("%Y%m%d"),
        "execution_date": sample_execution_date,  # Deprecated in Airflow 3.x
        # Modern Airflow 3.x fields
        "logical_date": sample_execution_date,
        "data_interval_start": data_interval_start,
        "data_interval_end": data_interval_end,
        "ts": sample_execution_date.isoformat(),
        "ts_nodash": sample_execution_date.strftime("%Y%m%dT%H%M%S"),
        "ts_nodash_with_tz": sample_execution_date.strftime("%Y%m%dT%H%M%S%z"),
        # Run identification
        "run_id": "test_run_id",
        "dag_run": None,  # Can be mocked with MagicMock if needed
        "task_instance": None,  # Can be mocked with MagicMock if needed
        # Additional useful context
        "params": {},
        "var": {"json": None, "value": None},
        "conn": None,
        "task": None,
        "dag": None,
    }
