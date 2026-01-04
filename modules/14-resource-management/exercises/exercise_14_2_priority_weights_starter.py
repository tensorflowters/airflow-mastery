"""
Exercise 14.2: Priority Weights (Starter)
==========================================

Implement priority-based task scheduling.

TODO: Complete all the implementation sections.
"""

from typing import Optional
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# PART 1: PRIORITY CONSTANTS
# =============================================================================


class Priority:
    """
    Priority weight constants for consistent task scheduling.

    Higher weights = higher priority = runs first when resources are limited.
    """

    # Tier 1: Critical - Must run immediately
    CRITICAL = 100

    # Tier 2: High - Important business processes
    HIGH = 75

    # Tier 3: Normal - Standard operations
    NORMAL = 50

    # Tier 4: Low - Can wait
    LOW = 25

    # Tier 5: Background - Run when nothing else needs to
    BACKGROUND = 1

    @staticmethod
    def get_tier(weight: int) -> str:
        """
        Get tier name from weight.

        TODO: Implement tier lookup:
        - >= 90: "critical"
        - >= 70: "high"
        - >= 40: "normal"
        - >= 20: "low"
        - < 20: "background"

        Returns:
            Tier name string
        """
        # TODO: Implement
        pass

    @staticmethod
    def validate_weight(weight: int) -> bool:
        """
        Validate weight is within acceptable range.

        TODO: Check weight is between 0 and 100.

        Returns:
            True if valid
        """
        # TODO: Implement
        pass


# =============================================================================
# PART 2: PRIORITY-AWARE DAG (Template)
# =============================================================================


"""
TODO: Create a DAG that demonstrates priority weights.

Requirements:
1. Use TaskFlow API
2. Create tasks with different priorities:
   - check_alerts: CRITICAL priority
   - run_production_etl: HIGH priority
   - generate_reports: NORMAL priority
   - cleanup_old_data: LOW priority
   - archive_logs: BACKGROUND priority

3. Create meaningful dependencies between tasks

Example structure:
    check_alerts (critical)
         ↓
    run_production_etl (high)
         ↓
    generate_reports (normal)
       ↓        ↓
  cleanup    archive
  (low)    (background)
"""

# from airflow.sdk import dag, task
# from datetime import datetime
#
# @dag(...)
# def priority_demo():
#     pass


# =============================================================================
# PART 3: PRIORITY CALCULATOR
# =============================================================================


class PriorityCalculator:
    """
    Calculate effective priorities based on weight rules.

    Weight rules:
    - ABSOLUTE: Use the specified weight
    - DOWNSTREAM: Sum of current + all downstream weights
    - UPSTREAM: Sum of current + all upstream weights
    """

    def calculate_downstream(self, task_weights: dict, dependencies: dict, task_id: str) -> int:
        """
        Calculate weight based on downstream tasks.

        TODO: Implement recursive downstream weight calculation.

        Args:
            task_weights: Dict of {task_id: weight}
            dependencies: Dict of {task_id: [downstream_task_ids]}
            task_id: Task to calculate

        Returns:
            Sum of task weight + all downstream weights
        """
        # TODO: Implement
        pass

    def calculate_upstream(self, task_weights: dict, dependencies: dict, task_id: str) -> int:
        """
        Calculate weight based on upstream tasks.

        TODO: Implement recursive upstream weight calculation.

        Args:
            task_weights: Dict of {task_id: weight}
            dependencies: Dict of {task_id: [downstream_task_ids]}
            task_id: Task to calculate

        Returns:
            Sum of task weight + all upstream weights
        """
        # TODO: Implement
        pass

    def get_execution_order(self, task_weights: dict, dependencies: dict) -> list:
        """
        Get expected task execution order based on priority.

        TODO: Implement:
        1. Calculate effective weights (using downstream rule)
        2. Sort tasks by effective weight (descending)
        3. Return ordered list of task_ids

        Returns:
            List of task_ids in priority order
        """
        # TODO: Implement
        pass


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test priority weights."""
    print("Priority Weights - Exercise 14.2")
    print("=" * 50)

    # Test 1: Priority tiers
    print("\n1. Priority Tiers:")
    # TODO: Test Priority.get_tier()

    # Test 2: Weight validation
    print("\n2. Weight Validation:")
    # TODO: Test Priority.validate_weight()

    # Test 3: Priority calculation
    print("\n3. Priority Calculation:")
    # TODO: Test PriorityCalculator with sample DAG

    # Sample DAG structure for testing:
    # task_weights = {
    #     "A": 10,
    #     "B": 20,
    #     "C": 30,
    #     "D": 5,
    # }
    # dependencies = {
    #     "A": ["B", "C"],
    #     "B": ["D"],
    #     "C": ["D"],
    #     "D": [],
    # }

    print("\n" + "=" * 50)
    print("Test complete!")


if __name__ == "__main__":
    main()
