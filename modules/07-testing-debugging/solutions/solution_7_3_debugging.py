"""
Solution 7.3: Fixed DAG
=======================

This is the corrected version of broken_dag.py.

BUGS FIXED:
1. Import Error: Fixed module name (pythons → python)
2. Syntax Error: Added missing comma after schedule
3. Syntax Error: Added missing colon after function definition
4. Runtime Error: Fixed key name (amount → value)
5. Logic Error: Fixed comparison direction (< → >)
6. Dependency Error: Fixed task order and data flow
7. Instantiation: Uncommented DAG instantiation

Each fix is marked with # FIX comment
"""

from airflow.sdk import dag, task
from datetime import datetime, timedelta
# FIX 1: Correct import path (pythons → python)
from airflow.operators.python import PythonOperator


# FIX 2: Added missing comma after schedule parameter
@dag(
    dag_id="solution_7_3_fixed_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # FIX: Added comma
    catchup=False,
    tags=["solution", "debugging"],
    description="Fixed version of the broken DAG exercise",
    default_args={
        "owner": "data-team",  # FIX: Changed from default "airflow"
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def fixed_dag():
    """
    A corrected ETL pipeline DAG.

    This DAG demonstrates proper:
    - Import statements
    - Syntax
    - Configuration
    - Error handling
    - Business logic
    - Task dependencies
    """

    # FIX 3: Added missing colon after function definition
    @task
    def extract_data():  # FIX: Added colon
        """Extract data from source."""
        print("=" * 50)
        print("EXTRACT DATA")
        print("=" * 50)

        data = [
            {"id": 1, "value": 100, "category": "A"},
            {"id": 2, "value": 200, "category": "B"},
            {"id": 3, "value": 150, "category": "A"},
        ]

        print(f"Extracted {len(data)} records")
        return data

    @task
    def transform_data(data: list) -> dict:
        """Transform the extracted data."""
        print("=" * 50)
        print("TRANSFORM DATA")
        print("=" * 50)

        print(f"Transforming {len(data)} records...")

        # FIX 4: Corrected key name (amount → value)
        total = sum(item["value"] for item in data)  # FIX: Changed "amount" to "value"

        by_category = {}
        for item in data:
            cat = item["category"]
            by_category[cat] = by_category.get(cat, 0) + item["value"]

        result = {
            "total": total,
            "by_category": by_category,
            "count": len(data),
        }

        print(f"Total value: {total}")
        print(f"By category: {by_category}")

        return result

    @task
    def validate_data(transformed: dict) -> bool:
        """Validate the transformed data."""
        print("=" * 50)
        print("VALIDATE DATA")
        print("=" * 50)

        # FIX 5: Corrected comparison (< → >)
        if transformed["count"] > 0:  # FIX: Changed < to >
            print(f"Validation passed! {transformed['count']} records found.")
            return True
        else:
            print("Validation failed: No data!")
            return False

    @task
    def load_data(transformed: dict, is_valid: bool) -> dict:
        """Load data to destination."""
        print("=" * 50)
        print("LOAD DATA")
        print("=" * 50)

        if not is_valid:
            raise ValueError("Cannot load invalid data!")

        print(f"Loading {transformed['count']} records...")
        print(f"Total value: {transformed['total']}")

        result = {
            "status": "success",
            "records_loaded": transformed["count"],
            "total_value": transformed["total"],
        }

        print("Load completed successfully!")
        return result

    # FIX 6: Corrected dependency order
    # The correct flow is: extract → transform → validate → load
    extracted = extract_data()
    transformed = transform_data(extracted)
    validated = validate_data(transformed)  # FIX: Now receives transformed, not extracted
    load_data(transformed, validated)


# FIX 7: Uncommented DAG instantiation
fixed_dag()


# =========================================================================
# REGRESSION TESTS
# =========================================================================

"""
Regression tests that would catch these bugs:

# test_broken_dag.py

import pytest
from datetime import datetime

def test_no_import_errors():
    '''Catches BUG 1: Import errors'''
    from airflow.models import DagBag
    dag_bag = DagBag(include_examples=False)
    assert len(dag_bag.import_errors) == 0

def test_dag_loads():
    '''Catches BUG 2, 3, 7: Syntax and config errors'''
    from airflow.models import DagBag
    dag_bag = DagBag(include_examples=False)
    assert "solution_7_3_fixed_dag" in dag_bag.dags

def test_extract_returns_data():
    '''Basic data extraction test'''
    # Import the task function
    result = extract_data.function()
    assert len(result) > 0
    assert all("value" in item for item in result)

def test_transform_uses_correct_key():
    '''Catches BUG 4: KeyError on wrong key'''
    data = [{"id": 1, "value": 100}]
    result = transform_data.function(data)
    assert result["total"] == 100

def test_validate_correct_logic():
    '''Catches BUG 5: Wrong comparison'''
    # Should return True when count > 0
    assert validate_data.function({"count": 1}) == True
    # Should return False when count == 0
    assert validate_data.function({"count": 0}) == False

def test_task_dependencies():
    '''Catches BUG 6: Wrong dependency order'''
    from airflow.models import DagBag
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.dags["solution_7_3_fixed_dag"]

    # validate_data should depend on transform_data
    validate_task = dag.get_task("validate_data")
    upstream_ids = [t.task_id for t in validate_task.upstream_list]
    assert "transform_data" in upstream_ids
"""
