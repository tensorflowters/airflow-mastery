"""Solution 0.5: DAG integrity and validation tests.

This test module provides comprehensive tests for Airflow DAG validation.
Place this file in your tests/ directory as test_dag_integrity.py or test_dags.py.

Requires Python 3.9+ (uses modern type hints with __future__ annotations).
"""

from __future__ import annotations

import importlib.util
import time
from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from airflow.models import DagBag

# ============================================
# DAG Import Tests
# ============================================


class TestDagImports:
    """Test that all DAG files can be imported without errors."""

    def test_dags_folder_exists(self, dags_folder: Path) -> None:
        """Verify DAGs folder exists."""
        assert dags_folder.exists(), f"DAGs folder not found: {dags_folder}"

    def test_dag_files_found(self, dag_files: list[Path]) -> None:
        """Verify at least one DAG file exists."""
        assert len(dag_files) > 0, "No DAG files found in dags/ directory"

    def test_dags_can_be_imported(self, dag_files: list[Path]) -> None:
        """Verify all DAG files can be imported without syntax/import errors.

        This test catches:
        - SyntaxError: Invalid Python syntax
        - ImportError: Missing dependencies
        - ModuleNotFoundError: Missing modules
        """
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
            except (SyntaxError, ImportError, ModuleNotFoundError) as e:
                pytest.fail(
                    f"Failed to import {dag_file.name}: {type(e).__name__}: {e}"
                )


# ============================================
# DagBag Integrity Tests
# ============================================


class TestDagBag:
    """Test DagBag integrity and DAG loading."""

    def test_no_import_errors(self, dag_bag: DagBag) -> None:
        """Check that no DAGs have import errors."""
        if dag_bag.import_errors:
            error_messages = "\n".join(
                f"  {dag_id}: {error}"
                for dag_id, error in dag_bag.import_errors.items()
            )
            pytest.fail(f"DAG import errors found:\n{error_messages}")

    def test_dags_loaded(self, dag_bag: DagBag) -> None:
        """Verify at least one DAG was loaded successfully."""
        assert len(dag_bag.dags) > 0, (
            "No DAGs loaded in DagBag. Check that DAG files define DAGs correctly."
        )

    def test_no_cycles(self, dag_bag: DagBag) -> None:
        """Verify DAGs don't have circular dependencies.

        Cycles in DAG dependencies prevent proper task scheduling.
        The topological_sort() method raises AirflowDagCycleException if cycles exist.
        """
        from airflow.exceptions import AirflowDagCycleException

        for dag_id, dag in dag_bag.dags.items():
            try:
                # topological_sort raises AirflowDagCycleException if cycles exist
                _ = dag.topological_sort()
            except AirflowDagCycleException as e:
                pytest.fail(f"DAG '{dag_id}' has circular dependencies: {e}")


# ============================================
# DAG Configuration Tests
# ============================================


class TestDagConfiguration:
    """Test DAG configuration best practices."""

    def test_dag_ids_are_unique(self, dag_bag: DagBag) -> None:
        """Verify all DAG IDs are unique.

        Uses Counter for O(n) complexity instead of nested loops.
        """
        dag_ids = list(dag_bag.dags.keys())
        dag_id_counts = Counter(dag_ids)

        # Find any DAG IDs that appear more than once
        duplicates = [dag_id for dag_id, count in dag_id_counts.items() if count > 1]

        if duplicates:
            pytest.fail(f"Duplicate DAG IDs found: {duplicates}")

    def test_dags_have_tags(self, dag_bag: DagBag) -> None:
        """Verify all DAGs have at least one tag for organization.

        Tags help organize DAGs in the Airflow UI and enable filtering.
        """
        for dag_id, dag in dag_bag.dags.items():
            assert dag.tags and len(dag.tags) > 0, (
                f"DAG '{dag_id}' has no tags. Add tags for better organization in the UI."
            )

    def test_dags_have_description(self, dag_bag: DagBag) -> None:
        """Verify all DAGs have a description or docstring.

        Checks both `description` parameter and `doc_md` (markdown documentation).
        Either one satisfies this requirement.
        """
        for dag_id, dag in dag_bag.dags.items():
            has_description = bool(dag.description)
            has_doc = bool(dag.doc_md)
            assert has_description or has_doc, (
                f"DAG '{dag_id}' has no description. Add a description for documentation."
            )

    def test_catchup_is_disabled(self, dag_bag: DagBag) -> None:
        """Verify catchup is explicitly disabled for safety.

        Catchup=True can trigger many DAG runs if start_date is in the past.
        This should be explicitly enabled only when needed.
        """
        for dag_id, dag in dag_bag.dags.items():
            assert dag.catchup is False, (
                f"DAG '{dag_id}' has catchup enabled. Set catchup=False unless historical runs are intentional."
            )

    def test_start_date_not_in_future(self, dag_bag: DagBag) -> None:
        """Verify DAG start_date is not set in the future.

        DAGs with future start_date won't run until that date, which may be unintentional.
        """
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)

        for dag_id, dag in dag_bag.dags.items():
            if dag.start_date:
                # Make start_date timezone-aware if it isn't
                start_date = dag.start_date
                if start_date.tzinfo is None:
                    start_date = start_date.replace(tzinfo=timezone.utc)

                assert start_date <= now, (
                    f"DAG '{dag_id}' has start_date in the future: {start_date}. This may be unintentional."
                )


# ============================================
# Task Configuration Tests
# ============================================


class TestTaskConfiguration:
    """Test task configuration best practices."""

    def test_tasks_have_owners(self, dag_bag: DagBag) -> None:
        """Verify all tasks have non-default owners.

        Task owners are used for alerting and accountability.
        The default 'airflow' owner should be replaced with actual team/person.
        """
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                # Check for both default 'airflow' and empty/None owners
                assert task.owner and task.owner != "airflow", (
                    f"Task '{dag_id}.{task.task_id}' uses default owner 'airflow'. "
                    "Set a specific owner in default_args."
                )

    def test_tasks_have_retries(self, dag_bag: DagBag) -> None:
        """Verify tasks have retry configuration.

        Retries help recover from transient failures. Even if set to 0,
        it should be an explicit choice.
        """
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                # retries should be explicitly set (can be 0)
                assert task.retries is not None, (
                    f"Task '{dag_id}.{task.task_id}' has no retries configured. "
                    "Set retries in default_args or task definition."
                )

    def test_no_orphan_tasks(self, dag_bag: DagBag) -> None:
        """Verify no tasks are orphaned (must have upstream or downstream).

        Single-task DAGs are allowed (valid use case for simple workflows).
        """
        for dag_id, dag in dag_bag.dags.items():
            # Skip single-task DAGs (valid use case)
            if len(dag.tasks) <= 1:
                continue

            for task in dag.tasks:
                has_upstream = bool(task.upstream_list)
                has_downstream = bool(task.downstream_list)
                assert has_upstream or has_downstream, (
                    f"Task '{dag_id}.{task.task_id}' is orphaned "
                    "(no upstream or downstream). All tasks should be connected."
                )

    def test_reasonable_task_count(self, dag_bag: DagBag) -> None:
        """Verify DAGs don't have too many tasks (maintainability).

        Large DAGs can be difficult to maintain and debug.
        Consider splitting into multiple DAGs or using dynamic task mapping.
        """
        max_tasks = 100  # Adjust based on your team's standards

        for dag_id, dag in dag_bag.dags.items():
            assert len(dag.tasks) <= max_tasks, (
                f"DAG '{dag_id}' has {len(dag.tasks)} tasks (max: {max_tasks}). "
                "Consider splitting into sub-DAGs or using dynamic task mapping."
            )


# ============================================
# Optional: Performance Tests
# ============================================


class TestDagPerformance:
    """Test DAG parsing performance.

    These tests are marked as 'slow' and can be skipped with:
        pytest -m "not slow"
    Or run exclusively with:
        pytest -m slow
    """

    @pytest.mark.slow
    def test_dag_parsing_time(self, dag_files: list[Path]) -> None:
        """Verify DAGs parse within acceptable time limits.

        Slow DAG parsing can impact scheduler performance and UI responsiveness.
        Optimize by:
        - Moving heavy imports inside functions
        - Using lazy loading patterns
        - Caching expensive computations
        """
        max_parse_time = 5.0  # seconds per DAG (configurable)

        for dag_file in dag_files:
            start = time.time()
            spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                try:
                    spec.loader.exec_module(module)
                except (SyntaxError, ImportError, ModuleNotFoundError):
                    # Import errors are caught by other tests
                    continue
            elapsed = time.time() - start

            assert elapsed < max_parse_time, (
                f"DAG '{dag_file.name}' took {elapsed:.2f}s to parse "
                f"(max: {max_parse_time}s). Consider optimizing imports."
            )
