"""
Pytest Configuration and Fixtures
==================================

This file provides shared fixtures and configuration for all tests.

Markers:
- slow: Tests that take longer to run (deselect with -m "not slow")
- integration: Tests requiring full Airflow environment

Usage:
    pytest tests/ -v                    # Run all tests
    pytest tests/ -v -m "not slow"      # Skip slow tests
    pytest tests/ -v -m "not integration" # Skip integration tests
    pytest tests/ -v --run-slow         # Include slow tests explicitly
"""

import pytest
import os
from pathlib import Path


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="Run slow tests",
    )
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests",
    )


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "airflow: marks tests requiring Airflow installation"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on markers and options."""
    # Skip slow tests unless --run-slow is specified
    if not config.getoption("--run-slow"):
        skip_slow = pytest.mark.skip(reason="need --run-slow option to run")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    # Skip integration tests unless --run-integration is specified
    if not config.getoption("--run-integration"):
        skip_integration = pytest.mark.skip(
            reason="need --run-integration option to run"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)


@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def dags_folder(project_root):
    """Return the DAGs folder path."""
    return project_root / "dags"


@pytest.fixture
def sample_context():
    """Provide a mock Airflow task instance context."""
    from datetime import datetime
    from pendulum import timezone
    
    utc = timezone("UTC")
    logical_date = utc.datetime(2024, 1, 1, 0, 0, 0)
    
    return {
        "ds": "2024-01-01",
        "ds_nodash": "20240101",
        "logical_date": logical_date,
        "data_interval_start": logical_date,
        "data_interval_end": utc.datetime(2024, 1, 2, 0, 0, 0),
        "run_id": "manual__2024-01-01T00:00:00+00:00",
        "dag_run": None,  # Would be a mock DagRun
        "ti": None,  # Would be a mock TaskInstance
    }


@pytest.fixture
def mock_hook():
    """Provide a mock database hook."""
    from unittest.mock import MagicMock

    hook = MagicMock()
    hook.get_records.return_value = [
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
    ]
    return hook


@pytest.fixture(scope="session")
def modules_folder(project_root):
    """Return the modules folder path."""
    return project_root / "modules"


@pytest.fixture(scope="session")
def all_solution_files(modules_folder):
    """Return all solution files across modules."""
    return list(modules_folder.glob("*/solutions/*.py"))


@pytest.fixture(scope="session")
def all_exercise_files(modules_folder):
    """Return all exercise files across modules."""
    return list(modules_folder.glob("*/exercises/*.py"))


@pytest.fixture
def mock_task_instance():
    """Provide a mock TaskInstance for testing."""
    from unittest.mock import MagicMock

    ti = MagicMock()
    ti.task_id = "mock_task"
    ti.dag_id = "mock_dag"
    ti.run_id = "manual__2024-01-01T00:00:00+00:00"
    ti.try_number = 1
    ti.max_tries = 3

    # Mock XCom methods
    ti.xcom_push = MagicMock()
    ti.xcom_pull = MagicMock(return_value={"test": "data"})

    return ti


@pytest.fixture
def mock_dag_run():
    """Provide a mock DagRun for testing."""
    from unittest.mock import MagicMock
    from pendulum import timezone

    utc = timezone("UTC")
    logical_date = utc.datetime(2024, 1, 1, 0, 0, 0)

    dag_run = MagicMock()
    dag_run.dag_id = "mock_dag"
    dag_run.run_id = "manual__2024-01-01T00:00:00+00:00"
    dag_run.logical_date = logical_date
    dag_run.data_interval_start = logical_date
    dag_run.data_interval_end = utc.datetime(2024, 1, 2, 0, 0, 0)
    dag_run.run_type = "manual"

    return dag_run


@pytest.fixture
def full_context(sample_context, mock_task_instance, mock_dag_run):
    """Provide a comprehensive mock context for task testing."""
    from unittest.mock import MagicMock

    context = sample_context.copy()
    context["ti"] = mock_task_instance
    context["dag_run"] = mock_dag_run
    context["task"] = MagicMock()
    context["task"].task_id = "mock_task"

    return context


@pytest.fixture(scope="session")
def airflow_home(tmp_path_factory):
    """Create a temporary Airflow home for testing."""
    home = tmp_path_factory.mktemp("airflow_home")

    # Set environment variable
    os.environ["AIRFLOW_HOME"] = str(home)
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

    # Create minimal config
    (home / "airflow.cfg").write_text("")

    return home
