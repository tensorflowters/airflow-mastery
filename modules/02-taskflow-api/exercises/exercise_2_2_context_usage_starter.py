"""
Exercise 2.2: Context Usage in TaskFlow
=======================================

Learn to access Airflow runtime context within TaskFlow tasks.

You'll practice:
- Accessing logical_date and data intervals
- Using context for dynamic file naming
- Passing context-derived data between tasks
"""

from datetime import datetime

# TODO: Import dag and task from airflow.sdk
# from airflow.sdk import dag, task

# TODO: Import get_current_context if you prefer that approach
# from airflow.sdk import get_current_context


# TODO: Add @dag decorator with configuration
# @dag(
#     dag_id="exercise_2_2_context_usage",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-02", "context"],
# )
def context_usage_dag():
    """Demonstrate context access patterns in TaskFlow."""

    # =========================================================================
    # TASK 1: Get Execution Info
    # =========================================================================

    # TODO: Create task that accesses context
    # Option A: Use **context parameter
    # @task
    # def get_execution_info(**context) -> dict:
    #     logical_date = context["logical_date"]
    #     ...

    # Option B: Use get_current_context()
    # @task
    # def get_execution_info() -> dict:
    #     context = get_current_context()
    #     logical_date = context["logical_date"]
    #     ...

    # Return should include:
    # {
    #     "logical_date": "2024-01-01",
    #     "logical_date_full": "2024-01-01T00:00:00+00:00",
    #     "interval_start": "...",
    #     "interval_end": "...",
    #     "run_id": "...",
    # }

    # =========================================================================
    # TASK 2: Generate Filename
    # =========================================================================

    # TODO: Create task that generates filename from execution info
    # @task
    # def generate_filename(exec_info: dict) -> dict:
    #     # Use the logical_date to create filename
    #     # Pattern: data_YYYY-MM-DD_HHmmss.csv
    #     # Return: {"filename": "...", "path": "/data/output/..."}
    #     pass

    # =========================================================================
    # TASK 3: Simulate File Write
    # =========================================================================

    # TODO: Create task that simulates file writing
    # @task
    # def simulate_file_write(file_info: dict) -> dict:
    #     # Print simulation message
    #     # print(f"Simulating write to: {file_info['path']}")
    #     # Return: {"bytes_written": 1024, "rows": 100, "status": "simulated"}
    #     pass

    # =========================================================================
    # TASK 4: Log Summary
    # =========================================================================

    # TODO: Create task that logs the final summary
    # @task
    # def log_summary(write_result: dict) -> dict:
    #     # Print formatted summary
    #     # Return: {"overall_status": "success", "completed_at": "..."}
    #     pass

    # =========================================================================
    # WIRE UP THE TASKS
    # =========================================================================

    # TODO: Chain the tasks together
    # exec_info = get_execution_info()
    # file_info = generate_filename(exec_info)
    # write_result = simulate_file_write(file_info)
    # log_summary(write_result)

    pass  # Remove when implementing


# TODO: Instantiate the DAG
# context_usage_dag()
