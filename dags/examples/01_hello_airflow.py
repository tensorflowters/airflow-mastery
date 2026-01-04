"""
Example DAG: Hello Airflow 3

A minimal DAG demonstrating the new Airflow 3 import structure.
Use this as your starting template.

Module: 01-foundations
"""

from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="01_hello_airflow",
    description="Your first Airflow 3 DAG",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-01"],
)
def hello_airflow():
    """Hello Airflow 3 example DAG."""

    @task
    def say_hello() -> str:
        """Simple task that prints a greeting."""
        print("Hello, Airflow 3!")
        return "success"

    say_hello()


# Instantiate the DAG
hello_airflow()
