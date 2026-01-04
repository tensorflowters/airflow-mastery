"""
Solution 6.1: Basic Mapping
===========================

Complete solution demonstrating:
- Dynamic task generation with expand()
- Parallel processing of list items
- Result aggregation from mapped tasks
"""

from datetime import datetime
import random
from airflow.sdk import dag, task


@dag(
    dag_id="solution_6_1_basic_mapping",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-06", "dynamic-tasks"],
    description="Demonstrates basic dynamic task mapping with expand()",
)
def basic_mapping_dag():
    """
    Basic Dynamic Task Mapping Pipeline.

    This DAG demonstrates the map-reduce pattern:
    1. Generate: Create a list of items to process
    2. Map: Process each item in parallel
    3. Reduce: Aggregate all results

    Pipeline flow:
    generate_numbers → square_number[0..9] → sum_squares
                            ↓
                    (10 parallel instances)
    """

    @task
    def generate_numbers() -> list[int]:
        """
        Generate a list of 10 random integers.

        This task returns a list that will be expanded by the next task.
        The size of this list determines how many mapped task instances
        will be created.
        """
        print("=" * 60)
        print("GENERATE NUMBERS")
        print("=" * 60)
        print()

        numbers = [random.randint(1, 100) for _ in range(10)]

        print(f"Generated {len(numbers)} random numbers:")
        print(f"  {numbers}")
        print()
        print("These numbers will each be processed by a separate")
        print("task instance created by expand().")
        print()
        print("=" * 60)

        return numbers

    @task
    def square_number(x: int) -> int:
        """
        Square a single number.

        This task receives ONE number at a time when used with expand().
        If the upstream task returns [4, 7, 2, 9, ...], this task will:
        - Run 10 times in parallel
        - Each instance gets one number (x=4, x=7, x=2, etc.)
        - Each instance returns one result
        """
        result = x ** 2

        print("=" * 40)
        print(f"SQUARE_NUMBER INSTANCE")
        print("=" * 40)
        print(f"  Input: {x}")
        print(f"  Calculation: {x}² = {result}")
        print(f"  Output: {result}")
        print("=" * 40)

        return result

    @task
    def sum_squares(squares: list[int]) -> dict:
        """
        Sum all the squared numbers.

        When a task receives the output of a mapped task (expand()),
        it automatically receives a LIST containing ALL the individual
        outputs, in the order of map_index.

        For example, if square_number ran 10 times with outputs
        [16, 49, 4, 81, ...], this task receives that entire list.
        """
        print("=" * 60)
        print("SUM SQUARES (Aggregation)")
        print("=" * 60)
        print()

        print(f"📥 Received {len(squares)} squared values from mapped tasks:")
        print(f"   {squares}")
        print()

        # Calculate statistics
        total = sum(squares)
        minimum = min(squares)
        maximum = max(squares)
        average = total / len(squares)

        print("📊 Statistics:")
        print(f"   Count: {len(squares)}")
        print(f"   Sum: {total}")
        print(f"   Min: {minimum}")
        print(f"   Max: {maximum}")
        print(f"   Average: {average:.2f}")
        print()

        result = {
            "count": len(squares),
            "squares": squares,
            "total": total,
            "min": minimum,
            "max": maximum,
            "average": round(average, 2),
        }

        print("=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)
        print()
        print("Summary:")
        print(f"  ✓ Generated 10 random numbers")
        print(f"  ✓ Squared each in parallel (10 task instances)")
        print(f"  ✓ Aggregated results: sum = {total}")
        print()
        print("=" * 60)

        return result

    # =====================================================================
    # WIRE UP THE PIPELINE
    # =====================================================================

    # Step 1: Generate the numbers (returns a list)
    numbers = generate_numbers()

    # Step 2: Square each number using expand()
    # This creates N task instances, one per number
    # Each instance runs in parallel
    squared = square_number.expand(x=numbers)

    # Step 3: Aggregate all results
    # sum_squares receives ALL outputs as a list
    sum_squares(squared)


# Instantiate the DAG
basic_mapping_dag()
