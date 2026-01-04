"""
Broken DAG - Exercise 7.3
=========================

This DAG contains INTENTIONAL BUGS for debugging practice.

Your task:
1. Find all the bugs
2. Understand what each bug causes
3. Fix them
4. Write tests to prevent regression

BUGS INCLUDED:
- Import error
- Syntax error
- Configuration error
- Runtime error
- Logic error
- Dependency error

DO NOT look at the solution until you've tried debugging!
"""

# BUG 1: Import Error - Wrong import path
from airflow.sdk import dag, task
from datetime import datetime, timedelta
# Intentional wrong import:
from airflow.operators.pythons import PythonOperator  # BUG: Wrong module name


# BUG 2: Configuration Error - Invalid schedule format
@dag(
    dag_id="broken_dag_exercise",
    start_date=datetime(2024, 1, 1),
    schedule="@daily"  # This is actually correct, but...
    catchup=False,  # BUG: Missing comma above
    tags=["exercise", "debugging"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def broken_dag():
    """
    A deliberately broken DAG for debugging practice.

    This DAG simulates a simple ETL pipeline with multiple bugs.
    """

    # BUG 3: Syntax Error - Missing colon
    @task
    def extract_data()  # BUG: Missing colon
        """Extract data from source."""
        print("Extracting data...")

        data = [
            {"id": 1, "value": 100, "category": "A"},
            {"id": 2, "value": 200, "category": "B"},
            {"id": 3, "value": 150, "category": "A"},
        ]

        return data

    @task
    def transform_data(data: list) -> dict:
        """Transform the extracted data."""
        print(f"Transforming {len(data)} records...")

        # BUG 4: Runtime Error - KeyError
        total = sum(item["amount"] for item in data)  # BUG: Wrong key name

        by_category = {}
        for item in data:
            cat = item["category"]
            by_category[cat] = by_category.get(cat, 0) + item["value"]

        return {
            "total": total,
            "by_category": by_category,
            "count": len(data),
        }

    @task
    def validate_data(transformed: dict) -> bool:
        """Validate the transformed data."""
        print("Validating data...")

        # BUG 5: Logic Error - Wrong comparison
        if transformed["count"] < 0:  # BUG: Should check > 0
            print("Validation passed!")
            return True
        else:
            print("Validation failed: No data!")
            return False

    @task
    def load_data(transformed: dict, is_valid: bool) -> dict:
        """Load data to destination."""
        if not is_valid:
            raise ValueError("Cannot load invalid data!")

        print(f"Loading {transformed['count']} records...")

        return {
            "status": "success",
            "records_loaded": transformed["count"],
        }

    # BUG 6: Dependency Error - Wrong order
    # The logic below has incorrect dependency chain
    extracted = extract_data()
    validated = validate_data(extracted)  # BUG: Should pass transformed, not extracted
    transformed = transform_data(extracted)
    load_data(transformed, validated)


# BUG 7: Missing DAG instantiation
# broken_dag()  # BUG: This line is commented out!
