"""
Exercise 7.1: DAG Integrity Tests (Starter)
============================================

Create a test suite for DAG validation.

Tests to implement:
1. No import errors
2. Owner configuration
3. Schedule documentation
4. Task ID naming conventions
"""

import os
import re

# TODO: Import pytest and DagBag
# import pytest
# from airflow.models import DagBag


# =========================================================================
# FIXTURE: Load DAGs
# =========================================================================

# TODO: Create a fixture that loads all DAGs
# @pytest.fixture(scope="session")
# def dag_bag():
#     """
#     Load all DAGs for testing.
#
#     scope="session" means this runs once for all tests,
#     not once per test (faster).
#     """
#     # Adjust path to your dags folder
#     dag_folder = os.path.join(
#         os.path.dirname(__file__),
#         "..", "..", "..",
#         "dags"
#     )
#     return DagBag(dag_folder=dag_folder, include_examples=False)


# =========================================================================
# TEST 1: No Import Errors
# =========================================================================

# TODO: Test that all DAGs load without errors
# def test_no_import_errors(dag_bag):
#     """
#     Verify all DAGs load without import errors.
#
#     Import errors indicate:
#     - Syntax errors in DAG files
#     - Missing dependencies
#     - Invalid Python code
#     - Circular imports
#     """
#     assert len(dag_bag.import_errors) == 0, (
#         f"DAG import errors: {dag_bag.import_errors}"
#     )


# =========================================================================
# TEST 2: DAGs Have Owners
# =========================================================================

# TODO: Test that all DAGs have proper owner configuration
# def test_dags_have_owner(dag_bag):
#     """
#     Verify all DAGs have an owner specified.
#
#     Owner should:
#     - Be set in default_args or as a DAG parameter
#     - Not be the default "airflow"
#     - Be a real team or person identifier
#     """
#     for dag_id, dag in dag_bag.dags.items():
#         owner = dag.default_args.get("owner", "airflow")
#
#         assert owner, f"DAG '{dag_id}' has no owner"
#         assert owner != "airflow", (
#             f"DAG '{dag_id}' uses default owner 'airflow'. "
#             f"Set a specific owner."
#         )


# =========================================================================
# TEST 3: Manual DAGs Documented
# =========================================================================

# TODO: Test that DAGs with schedule=None have documentation
# def test_manual_dags_documented(dag_bag):
#     """
#     Verify DAGs with schedule=None have a description.
#
#     Manual-trigger DAGs should document:
#     - Why they don't have a schedule
#     - How/when they should be triggered
#     - What they do
#     """
#     for dag_id, dag in dag_bag.dags.items():
#         # Check if DAG has no schedule
#         if dag.schedule_interval is None:
#             assert dag.description, (
#                 f"DAG '{dag_id}' has schedule=None but no description. "
#                 f"Add a description explaining the manual trigger purpose."
#             )


# =========================================================================
# TEST 4: Task IDs Follow Naming Convention
# =========================================================================

# TODO: Helper function to validate snake_case
# def is_valid_task_id(task_id: str) -> bool:
#     """
#     Check if task_id follows snake_case convention.
#
#     Valid: process_data, extract_users, load_to_warehouse
#     Invalid: ProcessData, extract-users, load to warehouse
#     """
#     pattern = r'^[a-z][a-z0-9_]*$'
#     return bool(re.match(pattern, task_id))


# TODO: Test that all task IDs use snake_case
# def test_task_ids_snake_case(dag_bag):
#     """
#     Verify all task IDs follow snake_case naming convention.
#
#     Consistent naming:
#     - Makes logs easier to read
#     - Enables pattern matching in monitoring
#     - Follows Python conventions
#     """
#     for dag_id, dag in dag_bag.dags.items():
#         for task in dag.tasks:
#             assert is_valid_task_id(task.task_id), (
#                 f"Task '{task.task_id}' in DAG '{dag_id}' "
#                 f"doesn't follow snake_case convention"
#             )


# =========================================================================
# OPTIONAL: Additional Tests
# =========================================================================

# TODO: Add more tests as needed
# - Test that all DAGs have tags
# - Test that production DAGs have retries
# - Test that no DAG has catchup=True (if that's your policy)
# - Test that all DAGs in 'production/' folder have SLA
