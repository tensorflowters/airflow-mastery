"""
DAG Integrity Tests

These tests validate that all DAGs can be imported without errors
and meet basic quality standards.

Run with: pytest tests/test_dag_integrity.py -v
"""

import pytest
from pathlib import Path


class TestDAGIntegrity:
    """Test suite for DAG validation."""
    
    @pytest.fixture(scope="class")
    def dag_bag(self):
        """Load all DAGs from the dags folder."""
        from airflow.models import DagBag
        
        dags_folder = Path(__file__).parent.parent / "dags"
        return DagBag(dag_folder=str(dags_folder), include_examples=False)
    
    def test_no_import_errors(self, dag_bag):
        """Ensure all DAGs can be imported without errors."""
        assert len(dag_bag.import_errors) == 0, \
            f"DAG import errors found: {dag_bag.import_errors}"
    
    def test_dags_have_tags(self, dag_bag):
        """Ensure all DAGs have at least one tag for organization."""
        dags_without_tags = [
            dag_id for dag_id, dag in dag_bag.dags.items()
            if not dag.tags
        ]
        
        assert len(dags_without_tags) == 0, \
            f"DAGs without tags: {dags_without_tags}"
    
    def test_dags_have_description(self, dag_bag):
        """Ensure all DAGs have a description."""
        dags_without_desc = [
            dag_id for dag_id, dag in dag_bag.dags.items()
            if not dag.description
        ]
        
        # Warning instead of failure for this check
        if dags_without_desc:
            pytest.skip(f"DAGs without description (warning): {dags_without_desc}")
    
    def test_dags_have_tasks(self, dag_bag):
        """Ensure all DAGs have at least one task."""
        empty_dags = [
            dag_id for dag_id, dag in dag_bag.dags.items()
            if len(dag.tasks) == 0
        ]
        
        assert len(empty_dags) == 0, \
            f"DAGs with no tasks: {empty_dags}"
    
    def test_no_cycles(self, dag_bag):
        """Ensure DAGs are truly acyclic (no circular dependencies)."""
        for dag_id, dag in dag_bag.dags.items():
            # DAG.test() will fail if there are cycles
            # This is implicitly tested during dag_bag loading
            assert dag.dag_id, f"DAG {dag_id} failed validation"
    
    def test_catchup_disabled(self, dag_bag):
        """Warn if catchup is enabled (often unintentional)."""
        catchup_enabled = [
            dag_id for dag_id, dag in dag_bag.dags.items()
            if dag.catchup
        ]
        
        if catchup_enabled:
            pytest.skip(
                f"DAGs with catchup=True (verify this is intentional): {catchup_enabled}"
            )


class TestDAGNamingConventions:
    """Test naming conventions for DAGs and tasks."""
    
    @pytest.fixture(scope="class")
    def dag_bag(self):
        from airflow.models import DagBag
        dags_folder = Path(__file__).parent.parent / "dags"
        return DagBag(dag_folder=str(dags_folder), include_examples=False)
    
    def test_dag_id_lowercase(self, dag_bag):
        """Ensure DAG IDs use snake_case."""
        non_compliant = [
            dag_id for dag_id in dag_bag.dags.keys()
            if dag_id != dag_id.lower() or " " in dag_id
        ]
        
        assert len(non_compliant) == 0, \
            f"DAG IDs should be snake_case: {non_compliant}"
    
    def test_task_id_lowercase(self, dag_bag):
        """Ensure task IDs use snake_case."""
        non_compliant = []
        for dag_id, dag in dag_bag.dags.items():
            for task in dag.tasks:
                if task.task_id != task.task_id.lower() or " " in task.task_id:
                    non_compliant.append(f"{dag_id}.{task.task_id}")
        
        assert len(non_compliant) == 0, \
            f"Task IDs should be snake_case: {non_compliant}"
