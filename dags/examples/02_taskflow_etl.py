"""
Example DAG: TaskFlow ETL Pipeline

Demonstrates the TaskFlow API with a complete ETL pattern.
Shows data passing between tasks using XCom automatically.

Module: 02-taskflow-api
"""

from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="02_taskflow_etl",
    description="ETL pipeline using TaskFlow API",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-02", "etl"],
)
def taskflow_etl():
    """ETL pipeline demonstrating TaskFlow patterns."""

    @task
    def extract() -> dict:
        """Extract data from a source system.

        In real scenarios, this might:
        - Query a database
        - Call an API
        - Read from a file system
        """
        print("Extracting data from source...")

        # Simulated raw data
        raw_data = {
            "users": [
                {"id": 1, "name": "Alice", "active": True},
                {"id": 2, "name": "Bob", "active": False},
                {"id": 3, "name": "Charlie", "active": True},
            ],
            "extracted_at": datetime.now().isoformat(),
        }

        print(f"Extracted {len(raw_data['users'])} users")
        return raw_data

    @task
    def transform(raw_data: dict) -> dict:
        """Transform the extracted data.

        Apply business logic:
        - Filter active users only
        - Normalize names to uppercase
        - Add computed fields
        """
        print("Transforming data...")

        active_users = [
            {
                "id": user["id"],
                "name": user["name"].upper(),
                "status": "ACTIVE",
            }
            for user in raw_data["users"]
            if user["active"]
        ]

        transformed_data = {
            "users": active_users,
            "count": len(active_users),
            "source_timestamp": raw_data["extracted_at"],
            "transformed_at": datetime.now().isoformat(),
        }

        print(f"Transformed to {transformed_data['count']} active users")
        return transformed_data

    @task
    def load(transformed_data: dict) -> dict:
        """Load transformed data to destination.

        In real scenarios, this might:
        - Insert into a database
        - Write to a data warehouse
        - Publish to a message queue
        """
        print("Loading data to destination...")
        print(f"Users to load: {transformed_data['users']}")
        print(f"Total count: {transformed_data['count']}")

        # Simulated load operation
        for user in transformed_data["users"]:
            print(f"  → Loaded user {user['id']}: {user['name']}")

        return {
            "loaded_count": transformed_data["count"],
            "status": "SUCCESS",
        }

    # Dependencies are automatic through data flow!
    raw = extract()
    transformed = transform(raw)
    load(transformed)


# Instantiate the DAG
taskflow_etl()
