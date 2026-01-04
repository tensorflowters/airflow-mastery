"""
Exercise 6.1: Basic Mapping
===========================

Learn Dynamic Task Mapping with expand().

You'll create:
- A task that generates a list of numbers
- A mapped task that processes each number in parallel
- An aggregation task that combines all results
"""

from datetime import datetime

# TODO: Import dag and task from airflow.sdk
# from airflow.sdk import dag, task


# =========================================================================
# DAG DEFINITION
# =========================================================================

# TODO: Add @dag decorator
# @dag(
#     dag_id="exercise_6_1_basic_mapping",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-06", "dynamic-tasks"],
#     description="Demonstrates basic dynamic task mapping",
# )
def basic_mapping_dag():
    """
    Basic Dynamic Task Mapping example.

    Pipeline:
    generate_numbers → square_number (×N parallel) → sum_squares

    This demonstrates:
    1. Generating data at runtime
    2. Processing items in parallel with expand()
    3. Aggregating results from mapped tasks
    """

    # =====================================================================
    # TASK 1: Generate Numbers
    # =====================================================================

    # TODO: Create task that generates a list of random numbers
    # @task
    # def generate_numbers() -> list[int]:
    #     """Generate a list of 10 random integers."""
    #     import random
    #
    #     numbers = [random.randint(1, 100) for _ in range(10)]
    #
    #     print("=" * 60)
    #     print("GENERATE NUMBERS")
    #     print("=" * 60)
    #     print(f"Generated {len(numbers)} random numbers:")
    #     print(f"  {numbers}")
    #     print("=" * 60)
    #
    #     return numbers

    # =====================================================================
    # TASK 2: Square Number (Mapped Task)
    # =====================================================================

    # TODO: Create task that squares a single number
    # @task
    # def square_number(x: int) -> int:
    #     """
    #     Square a single number.
    #
    #     This task will be executed once per item when used with expand().
    #     Each instance runs in parallel and has its own logs.
    #     """
    #     result = x ** 2
    #     print(f"Squaring {x}: {x}² = {result}")
    #     return result

    # =====================================================================
    # TASK 3: Sum Squares (Aggregation)
    # =====================================================================

    # TODO: Create task that aggregates all squared results
    # @task
    # def sum_squares(squares: list[int]) -> dict:
    #     """
    #     Sum all the squared numbers.
    #
    #     When a task receives a mapped task output, it gets a LIST
    #     containing all the individual outputs.
    #     """
    #     print("=" * 60)
    #     print("SUM SQUARES")
    #     print("=" * 60)
    #
    #     print(f"Received {len(squares)} squared values:")
    #     print(f"  {squares}")
    #     print()
    #
    #     total = sum(squares)
    #     print(f"Sum of squares: {total}")
    #     print("=" * 60)
    #
    #     return {
    #         "count": len(squares),
    #         "squares": squares,
    #         "total": total,
    #     }

    # =====================================================================
    # WIRE UP THE PIPELINE
    # =====================================================================

    # TODO: Connect the tasks using expand()
    #
    # Step 1: Generate the numbers
    # numbers = generate_numbers()
    #
    # Step 2: Square each number in parallel using expand()
    # squared = square_number.expand(x=numbers)
    #
    # Step 3: Sum all the squares
    # sum_squares(squared)

    pass  # Remove when implementing


# TODO: Instantiate the DAG
# basic_mapping_dag()
