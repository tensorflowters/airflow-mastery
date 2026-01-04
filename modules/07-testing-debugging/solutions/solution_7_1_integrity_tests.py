"""
Solution 7.1: DAG Integrity Tests
=================================

Complete test suite for DAG validation.

Tests included:
1. No import errors
2. Owner configuration
3. Schedule documentation
4. Task ID naming conventions
5. Additional quality checks
"""

import os
import re
import pytest
from airflow.models import DagBag


# =========================================================================
# FIXTURES
# =========================================================================


@pytest.fixture(scope="session")
def dag_bag():
    """
    Load all DAGs for testing.

    scope="session" ensures DAGs are loaded once for all tests,
    significantly improving test performance.
    """
    dag_folder = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "..",
        "dags"
    )

    # Handle case where dags folder doesn't exist in test environment
    if not os.path.exists(dag_folder):
        pytest.skip(f"DAGs folder not found: {dag_folder}")

    return DagBag(dag_folder=dag_folder, include_examples=False)


@pytest.fixture
def dag_list(dag_bag):
    """Return list of all loaded DAGs."""
    return list(dag_bag.dags.values())


# =========================================================================
# HELPER FUNCTIONS
# =========================================================================


def is_valid_task_id(task_id: str) -> bool:
    """
    Check if task_id follows snake_case convention.

    Rules:
    - Starts with lowercase letter
    - Contains only lowercase letters, numbers, underscores
    - No consecutive underscores
    - Doesn't start or end with underscore
    """
    pattern = r'^[a-z][a-z0-9]*(_[a-z0-9]+)*$'
    return bool(re.match(pattern, task_id))


def get_dag_owner(dag) -> str:
    """Extract owner from DAG, checking multiple sources."""
    # Check default_args first
    owner = dag.default_args.get("owner")
    if owner:
        return owner

    # Check for owner in tags (e.g., "owner:data-team")
    for tag in dag.tags or []:
        if tag.startswith("owner:"):
            return tag.split(":")[1]

    return None


# =========================================================================
# TEST 1: No Import Errors
# =========================================================================


class TestImportErrors:
    """Tests for DAG import validation."""

    def test_no_import_errors(self, dag_bag):
        """
        Verify all DAGs load without import errors.

        Import errors can be caused by:
        - Syntax errors in DAG files
        - Missing Python dependencies
        - Invalid imports
        - Circular dependencies
        - Runtime errors at module level
        """
        if dag_bag.import_errors:
            error_messages = []
            for dag_file, error in dag_bag.import_errors.items():
                error_messages.append(f"\n{dag_file}:\n{error}")

            pytest.fail(
                f"Found {len(dag_bag.import_errors)} DAG import errors:"
                f"{''.join(error_messages)}"
            )

    def test_at_least_one_dag_loaded(self, dag_bag):
        """Verify at least one DAG was loaded (sanity check)."""
        assert len(dag_bag.dags) > 0, (
            "No DAGs were loaded. Check DAG folder path."
        )


# =========================================================================
# TEST 2: Owner Configuration
# =========================================================================


class TestOwnerConfiguration:
    """Tests for DAG ownership validation."""

    def test_dags_have_owner(self, dag_bag):
        """
        Verify all DAGs have an owner specified.

        Owners are important for:
        - Accountability for DAG health
        - Alerting the right team
        - Code review routing
        """
        dags_without_owner = []

        for dag_id, dag in dag_bag.dags.items():
            owner = get_dag_owner(dag)
            if not owner:
                dags_without_owner.append(dag_id)

        assert len(dags_without_owner) == 0, (
            f"DAGs missing owner: {dags_without_owner}\n"
            f"Set owner in default_args or add 'owner:<name>' tag."
        )

    def test_owner_not_default(self, dag_bag):
        """
        Verify no DAG uses the default 'airflow' owner.

        The default owner doesn't help with accountability.
        Use a specific team or person identifier.
        """
        dags_with_default_owner = []

        for dag_id, dag in dag_bag.dags.items():
            owner = get_dag_owner(dag)
            if owner and owner.lower() == "airflow":
                dags_with_default_owner.append(dag_id)

        assert len(dags_with_default_owner) == 0, (
            f"DAGs using default 'airflow' owner: {dags_with_default_owner}\n"
            f"Replace with specific owner (team or person)."
        )


# =========================================================================
# TEST 3: Schedule Documentation
# =========================================================================


class TestScheduleDocumentation:
    """Tests for schedule-related validation."""

    def test_manual_dags_have_description(self, dag_bag):
        """
        Verify DAGs with schedule=None have a description.

        Manual-trigger DAGs should document:
        - Purpose of the DAG
        - When/how it should be triggered
        - Expected inputs/outputs
        """
        undocumented_manual_dags = []

        for dag_id, dag in dag_bag.dags.items():
            if dag.schedule_interval is None:
                if not dag.description or len(dag.description.strip()) < 10:
                    undocumented_manual_dags.append(dag_id)

        assert len(undocumented_manual_dags) == 0, (
            f"Manual-trigger DAGs without description: "
            f"{undocumented_manual_dags}\n"
            f"Add 'description' parameter explaining the DAG purpose."
        )


# =========================================================================
# TEST 4: Task ID Naming Convention
# =========================================================================


class TestTaskNaming:
    """Tests for task naming conventions."""

    def test_task_ids_snake_case(self, dag_bag):
        """
        Verify all task IDs follow snake_case naming convention.

        Benefits of consistent naming:
        - Easier log searching
        - Pattern matching in monitoring
        - Follows Python conventions
        - Clear in DAG visualizations
        """
        invalid_task_ids = []

        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                if not is_valid_task_id(task.task_id):
                    invalid_task_ids.append(
                        f"{dag_id}.{task.task_id}"
                    )

        assert len(invalid_task_ids) == 0, (
            f"Tasks with invalid IDs (should be snake_case): "
            f"{invalid_task_ids}"
        )

    def test_no_duplicate_task_ids(self, dag_bag):
        """
        Verify no DAG has duplicate task IDs.

        Duplicate task IDs can cause:
        - Undefined behavior
        - Incorrect dependencies
        - Confusing logs
        """
        dags_with_duplicates = []

        for dag_id, dag in dag_bag.dags.items():
            task_ids = [task.task_id for task in dag.tasks]
            duplicates = [
                tid for tid in task_ids
                if task_ids.count(tid) > 1
            ]
            if duplicates:
                dags_with_duplicates.append(
                    f"{dag_id}: {set(duplicates)}"
                )

        assert len(dags_with_duplicates) == 0, (
            f"DAGs with duplicate task IDs: {dags_with_duplicates}"
        )


# =========================================================================
# ADDITIONAL QUALITY TESTS
# =========================================================================


class TestQualityStandards:
    """Additional quality validation tests."""

    def test_dags_have_tags(self, dag_bag):
        """
        Verify all DAGs have at least one tag.

        Tags help with:
        - Filtering in the UI
        - Categorizing DAGs
        - Access control policies
        """
        dags_without_tags = []

        for dag_id, dag in dag_bag.dags.items():
            if not dag.tags or len(dag.tags) == 0:
                dags_without_tags.append(dag_id)

        # This is a warning, not a failure
        if dags_without_tags:
            pytest.skip(
                f"DAGs without tags (recommendation): "
                f"{dags_without_tags}"
            )

    def test_no_hardcoded_dates(self, dag_bag):
        """
        Verify no DAG has a start_date in the past year.

        Very old start_dates with catchup=True can cause
        massive backfills.
        """
        from datetime import datetime, timedelta

        old_start_date_dags = []
        one_year_ago = datetime.now() - timedelta(days=365)

        for dag_id, dag in dag_bag.dags.items():
            if dag.start_date and dag.start_date < one_year_ago:
                if dag.catchup:  # Only flag if catchup is enabled
                    old_start_date_dags.append(
                        f"{dag_id} (start: {dag.start_date})"
                    )

        assert len(old_start_date_dags) == 0, (
            f"DAGs with old start_date and catchup=True: "
            f"{old_start_date_dags}\n"
            f"Consider setting catchup=False or using a recent date."
        )


# =========================================================================
# CONFTEST CONTENT (put in conftest.py for real usage)
# =========================================================================

"""
# conftest.py content for test directory

import os
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dag_bag():
    dag_folder = os.environ.get("AIRFLOW_DAG_FOLDER", "dags/")
    return DagBag(dag_folder=dag_folder, include_examples=False)
"""
