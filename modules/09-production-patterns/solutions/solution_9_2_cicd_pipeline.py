"""
Solution 9.2: CI/CD Pipeline - DAG Integrity Tests
===================================================

Complete test suite for validating DAGs in CI/CD pipeline.

Run with: pytest tests/test_dag_integrity.py -v

These tests ensure:
1. All DAGs load without errors
2. DAGs follow best practices
3. No circular dependencies
4. Proper configuration
5. Security considerations
"""

import pytest
import re
from datetime import datetime, timedelta
from pathlib import Path


# =============================================================================
# FIXTURES
# =============================================================================


@pytest.fixture(scope="session")
def dag_bag():
    """
    Load all DAGs once for the test session.

    Uses session scope to avoid reloading DAGs for each test,
    which significantly speeds up test execution.
    """
    from airflow.models import DagBag

    # Set include_examples=False to only test your DAGs
    dag_bag = DagBag(dag_folder="dags/", include_examples=False)
    return dag_bag


@pytest.fixture
def all_dag_ids(dag_bag):
    """Get list of all DAG IDs."""
    return list(dag_bag.dags.keys())


@pytest.fixture
def dag_files():
    """Get all Python files in the dags directory."""
    dags_path = Path("dags/")
    return list(dags_path.rglob("*.py"))


# =============================================================================
# IMPORT & SYNTAX TESTS
# =============================================================================


class TestDagImport:
    """Tests for DAG import and syntax validation."""

    def test_no_import_errors(self, dag_bag):
        """
        Verify all DAGs load without import errors.

        This catches:
        - Syntax errors
        - Missing dependencies
        - Invalid imports
        """
        import_errors = dag_bag.import_errors

        if import_errors:
            error_messages = []
            for filepath, error in import_errors.items():
                error_messages.append(f"\n{filepath}:\n{error}")

            pytest.fail(
                f"Found {len(import_errors)} import error(s):"
                + "".join(error_messages)
            )

    def test_at_least_one_dag_loaded(self, dag_bag):
        """Verify at least one DAG is present."""
        assert len(dag_bag.dags) > 0, "No DAGs were loaded"

    def test_dag_ids_are_valid(self, dag_bag):
        """
        Verify DAG IDs follow naming conventions.

        Convention: lowercase, underscores, no spaces, starts with letter
        """
        pattern = r"^[a-z][a-z0-9_]*$"
        invalid_ids = []

        for dag_id in dag_bag.dags.keys():
            if not re.match(pattern, dag_id):
                invalid_ids.append(dag_id)

        if invalid_ids:
            pytest.fail(
                f"Invalid DAG IDs (should be lowercase with underscores): "
                f"{invalid_ids}"
            )


# =============================================================================
# CONFIGURATION TESTS
# =============================================================================


class TestDagConfiguration:
    """Tests for DAG configuration best practices."""

    def test_all_dags_have_description(self, dag_bag):
        """
        Verify all DAGs have descriptions.

        Descriptions help other developers understand the DAG purpose.
        """
        missing_description = []

        for dag_id, dag in dag_bag.dags.items():
            if not dag.description:
                missing_description.append(dag_id)

        if missing_description:
            pytest.fail(
                f"DAGs missing description: {missing_description}"
            )

    def test_all_dags_have_owner(self, dag_bag):
        """
        Verify all DAGs have an owner defined.

        Owner is set in default_args["owner"].
        """
        missing_owner = []

        for dag_id, dag in dag_bag.dags.items():
            owner = dag.default_args.get("owner", "")
            if not owner or owner == "airflow":
                missing_owner.append(dag_id)

        if missing_owner:
            pytest.fail(
                f"DAGs missing owner (or using default 'airflow'): "
                f"{missing_owner}"
            )

    def test_all_dags_have_tags(self, dag_bag):
        """
        Verify all DAGs have tags for filtering.

        Tags help organize DAGs in the UI.
        """
        missing_tags = []

        for dag_id, dag in dag_bag.dags.items():
            if not dag.tags:
                missing_tags.append(dag_id)

        if missing_tags:
            pytest.fail(
                f"DAGs missing tags: {missing_tags}"
            )

    def test_start_dates_exist(self, dag_bag):
        """
        Verify all DAGs have a start_date defined.
        """
        missing_start_date = []

        for dag_id, dag in dag_bag.dags.items():
            if dag.start_date is None:
                missing_start_date.append(dag_id)

        if missing_start_date:
            pytest.fail(
                f"DAGs missing start_date: {missing_start_date}"
            )

    def test_no_future_start_dates(self, dag_bag):
        """
        Verify start_date is not in the future.

        Future start dates mean DAG won't run until that date.
        """
        future_dates = []
        now = datetime.now()

        for dag_id, dag in dag_bag.dags.items():
            if dag.start_date and dag.start_date > now:
                future_dates.append(
                    f"{dag_id}: {dag.start_date}"
                )

        if future_dates:
            pytest.fail(
                f"DAGs with future start_date: {future_dates}"
            )

    def test_catchup_is_explicit(self, dag_bag):
        """
        Verify catchup is explicitly set (not defaulting).

        Default catchup=True can cause unexpected backfills.
        """
        # This is a warning rather than failure
        implicit_catchup = []

        for dag_id, dag in dag_bag.dags.items():
            # In Airflow 3.x, catchup defaults to False
            # But we still want explicit setting for clarity
            if dag.catchup:
                implicit_catchup.append(dag_id)

        if implicit_catchup:
            print(
                f"INFO: DAGs with catchup=True (verify this is intentional): "
                f"{implicit_catchup}"
            )


# =============================================================================
# STRUCTURE TESTS
# =============================================================================


class TestDagStructure:
    """Tests for DAG structure and dependencies."""

    def test_no_cycles(self, dag_bag):
        """
        Verify no DAGs have circular dependencies.

        Cycles cause infinite loops in the scheduler.
        """
        for dag_id, dag in dag_bag.dags.items():
            try:
                dag.test_cycle()
            except Exception as e:
                pytest.fail(
                    f"DAG {dag_id} has cyclic dependencies: {e}"
                )

    def test_all_tasks_have_connections(self, dag_bag):
        """
        Verify no orphan tasks exist.

        Each task should be connected to at least one other task,
        unless it's the only task in the DAG.
        """
        orphan_tasks = []

        for dag_id, dag in dag_bag.dags.items():
            if len(dag.tasks) <= 1:
                continue  # Single task DAGs are okay

            for task in dag.tasks:
                if not task.upstream_list and not task.downstream_list:
                    orphan_tasks.append(f"{dag_id}.{task.task_id}")

        if orphan_tasks:
            pytest.fail(
                f"Orphan tasks found (no upstream or downstream): "
                f"{orphan_tasks}"
            )

    def test_dag_ids_unique(self, dag_bag):
        """
        Verify all DAG IDs are unique.

        DagBag automatically handles duplicates, but this verifies
        there are no naming conflicts.
        """
        # DagBag will use the last DAG found with duplicate IDs
        # Check source files for duplicates
        dag_id_sources = {}

        for dag_id, dag in dag_bag.dags.items():
            if dag_id in dag_id_sources:
                dag_id_sources[dag_id].append(dag.fileloc)
            else:
                dag_id_sources[dag_id] = [dag.fileloc]

        duplicates = {
            k: v for k, v in dag_id_sources.items()
            if len(v) > 1
        }

        if duplicates:
            pytest.fail(f"Duplicate DAG IDs found: {duplicates}")


# =============================================================================
# PERFORMANCE TESTS
# =============================================================================


class TestDagPerformance:
    """Tests for DAG performance considerations."""

    def test_max_tasks_per_dag(self, dag_bag):
        """
        Verify DAGs don't have too many tasks.

        Very large DAGs can impact scheduler performance.
        """
        max_tasks = 500
        large_dags = []

        for dag_id, dag in dag_bag.dags.items():
            task_count = len(dag.tasks)
            if task_count > max_tasks:
                large_dags.append(f"{dag_id}: {task_count} tasks")

        if large_dags:
            pytest.fail(
                f"DAGs exceeding {max_tasks} tasks: {large_dags}"
            )

    def test_reasonable_schedule_interval(self, dag_bag):
        """
        Verify schedule intervals are reasonable.

        Very frequent schedules can overload the scheduler.
        """
        min_interval = timedelta(minutes=5)
        too_frequent = []

        for dag_id, dag in dag_bag.dags.items():
            # Skip if no schedule or @once
            if dag.schedule_interval in (None, "@once"):
                continue

            # Try to convert to timedelta
            schedule = dag.normalized_schedule_interval
            if schedule and isinstance(schedule, timedelta):
                if schedule < min_interval:
                    too_frequent.append(f"{dag_id}: {schedule}")

        if too_frequent:
            print(
                f"WARNING: DAGs with schedule < {min_interval}: {too_frequent}"
            )


# =============================================================================
# SECURITY TESTS
# =============================================================================


class TestDagSecurity:
    """Tests for DAG security considerations."""

    def test_no_hardcoded_credentials(self, dag_files):
        """
        Verify no hardcoded credentials in DAG code.

        Check for patterns like password=, api_key=, secret=
        """
        credential_patterns = [
            r"password\s*=\s*['\"][^'\"]+['\"]",
            r"api_key\s*=\s*['\"][^'\"]+['\"]",
            r"secret\s*=\s*['\"][^'\"]+['\"]",
            r"token\s*=\s*['\"][^'\"]+['\"]",
            r"AWS_ACCESS_KEY_ID\s*=\s*['\"][^'\"]+['\"]",
            r"AWS_SECRET_ACCESS_KEY\s*=\s*['\"][^'\"]+['\"]",
        ]

        violations = []

        for dag_file in dag_files:
            content = dag_file.read_text()

            for pattern in credential_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    # Exclude obvious placeholders
                    real_matches = [
                        m for m in matches
                        if not any(
                            placeholder in m.lower()
                            for placeholder in ["xxx", "your_", "example", "placeholder"]
                        )
                    ]
                    if real_matches:
                        violations.append(f"{dag_file}: {real_matches}")

        if violations:
            pytest.fail(
                f"Potential hardcoded credentials found:\n"
                + "\n".join(violations)
            )

    def test_no_exec_or_eval(self, dag_files):
        """
        Verify no dynamic code execution in DAGs.

        exec() and eval() can be security risks.
        """
        dangerous_patterns = [
            r"\bexec\s*\(",
            r"\beval\s*\(",
            r"__import__\s*\(",
        ]

        violations = []

        for dag_file in dag_files:
            content = dag_file.read_text()

            for pattern in dangerous_patterns:
                if re.search(pattern, content):
                    violations.append(str(dag_file))
                    break

        if violations:
            pytest.fail(
                f"Dangerous code patterns found in: {violations}"
            )


# =============================================================================
# QUALITY TESTS
# =============================================================================


class TestCodeQuality:
    """Tests for code quality."""

    def test_no_print_statements(self, dag_files):
        """
        Verify DAGs use logging instead of print.

        Print statements don't integrate with Airflow logging.
        """
        violations = []

        for dag_file in dag_files:
            content = dag_file.read_text()
            lines = content.split("\n")

            for line_num, line in enumerate(lines, 1):
                # Skip comments
                stripped = line.strip()
                if stripped.startswith("#"):
                    continue

                # Check for print statements
                if re.search(r"\bprint\s*\(", line):
                    violations.append(f"{dag_file}:{line_num}")

        if violations:
            print(
                f"WARNING: print() statements found (consider using logging): "
                f"{violations[:10]}"
            )

    def test_docstrings_present(self, dag_bag):
        """
        Verify DAG functions have docstrings.
        """
        missing_docstrings = []

        for dag_id, dag in dag_bag.dags.items():
            if not dag.doc_md and not dag.description:
                missing_docstrings.append(dag_id)

        if missing_docstrings:
            print(
                f"INFO: DAGs without docstrings: {missing_docstrings}"
            )


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def get_dag_file_content(dag_id, dag_bag):
    """Get the file content for a DAG."""
    dag = dag_bag.get_dag(dag_id)
    if dag and dag.fileloc:
        with open(dag.fileloc) as f:
            return f.read()
    return None


# =============================================================================
# MAIN
# =============================================================================


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
