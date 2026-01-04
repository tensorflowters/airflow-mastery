"""
Solution 14.2: Priority Weights
================================

Complete implementation of priority-based task scheduling.
"""

import logging
from typing import Optional
from collections import defaultdict

from airflow.sdk import dag, task
from airflow.utils.weight_rule import WeightRule
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# PART 1: PRIORITY CONSTANTS
# =============================================================================


class Priority:
    """
    Priority weight constants for consistent task scheduling.

    Higher weights = higher priority = runs first when resources are limited.

    Tier System:
    - CRITICAL (90-100): Alert systems, monitoring, SLA-bound tasks
    - HIGH (70-89): Production ETL, customer-facing processes
    - NORMAL (40-69): Standard reporting, analytics
    - LOW (20-39): Data cleanup, optimization tasks
    - BACKGROUND (1-19): Archival, experimental, non-essential
    """

    # Tier 1: Critical
    CRITICAL = 100
    URGENT = 95
    SLA_BOUND = 90

    # Tier 2: High
    HIGH = 75
    PRODUCTION = 70

    # Tier 3: Normal
    NORMAL = 50
    STANDARD = 45
    REPORTING = 40

    # Tier 4: Low
    LOW = 25
    CLEANUP = 20

    # Tier 5: Background
    BACKGROUND = 10
    EXPERIMENTAL = 5
    MINIMAL = 1

    @staticmethod
    def get_tier(weight: int) -> str:
        """
        Get tier name from weight.

        Args:
            weight: Priority weight value

        Returns:
            Tier name string
        """
        if weight >= 90:
            return "critical"
        elif weight >= 70:
            return "high"
        elif weight >= 40:
            return "normal"
        elif weight >= 20:
            return "low"
        else:
            return "background"

    @staticmethod
    def validate_weight(weight: int) -> bool:
        """
        Validate weight is within acceptable range.

        Args:
            weight: Weight to validate

        Returns:
            True if valid (0-100)
        """
        return 0 <= weight <= 100

    @staticmethod
    def from_sla_hours(hours: float) -> int:
        """
        Calculate priority based on SLA deadline.

        Tighter SLAs get higher priority.

        Args:
            hours: SLA deadline in hours

        Returns:
            Appropriate priority weight
        """
        if hours <= 1:
            return Priority.CRITICAL
        elif hours <= 4:
            return Priority.HIGH
        elif hours <= 12:
            return Priority.NORMAL
        elif hours <= 24:
            return Priority.LOW
        else:
            return Priority.BACKGROUND


# =============================================================================
# PART 2: PRIORITY-AWARE DAG
# =============================================================================


@dag(
    dag_id="solution_priority_demo",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "priority", "module-14"],
)
def priority_demo():
    """
    DAG demonstrating priority weights.

    Task Structure:
        check_alerts (CRITICAL)
             ↓
        run_production_etl (HIGH)
             ↓
        generate_reports (NORMAL)
           ↓        ↓
      cleanup    archive
      (LOW)    (BACKGROUND)
    """

    @task(priority_weight=Priority.CRITICAL)
    def check_alerts():
        """
        Critical priority: Alert checking.

        Must run first to catch any system issues.
        """
        logger.info("Checking system alerts...")
        return {"alerts": 0, "status": "healthy"}

    @task(priority_weight=Priority.HIGH)
    def run_production_etl(alert_status: dict):
        """
        High priority: Production ETL.

        Only runs if system is healthy.
        """
        if alert_status["status"] != "healthy":
            raise ValueError("System unhealthy - aborting ETL")

        logger.info("Running production ETL...")
        return {"records_processed": 10000, "duration_sec": 120}

    @task(priority_weight=Priority.NORMAL)
    def generate_reports(etl_result: dict):
        """
        Normal priority: Report generation.

        Standard business reporting.
        """
        logger.info(f"Generating reports from {etl_result['records_processed']} records")
        return {
            "daily_report": "generated",
            "weekly_summary": "generated",
        }

    @task(priority_weight=Priority.LOW)
    def cleanup_old_data(reports: dict):
        """
        Low priority: Data cleanup.

        Can wait until other tasks complete.
        """
        logger.info("Cleaning up old temporary data...")
        return {"files_cleaned": 50}

    @task(priority_weight=Priority.BACKGROUND)
    def archive_logs(reports: dict):
        """
        Background priority: Log archival.

        Runs last, non-critical.
        """
        logger.info("Archiving old logs...")
        return {"logs_archived": 1000}

    # Define dependencies
    alerts = check_alerts()
    etl = run_production_etl(alerts)
    reports = generate_reports(etl)

    # Parallel low-priority tasks
    cleanup_old_data(reports)
    archive_logs(reports)


# =============================================================================
# PART 3: PRIORITY CALCULATOR
# =============================================================================


class PriorityCalculator:
    """
    Calculate effective priorities based on weight rules.

    Weight Rules:
    - ABSOLUTE: Use the specified weight exactly
    - DOWNSTREAM: Sum of current + all downstream weights
    - UPSTREAM: Sum of current + all upstream weights
    """

    def calculate_downstream(
        self,
        task_weights: dict,
        dependencies: dict,
        task_id: str,
        visited: set = None,
    ) -> int:
        """
        Calculate weight based on downstream tasks.

        Args:
            task_weights: Dict of {task_id: weight}
            dependencies: Dict of {task_id: [downstream_task_ids]}
            task_id: Task to calculate

        Returns:
            Sum of task weight + all downstream weights
        """
        if visited is None:
            visited = set()

        # Prevent infinite loops
        if task_id in visited:
            return 0
        visited.add(task_id)

        # Base weight
        weight = task_weights.get(task_id, 0)

        # Add downstream weights
        downstream_tasks = dependencies.get(task_id, [])
        for downstream_id in downstream_tasks:
            weight += self.calculate_downstream(
                task_weights, dependencies, downstream_id, visited
            )

        return weight

    def calculate_upstream(
        self,
        task_weights: dict,
        dependencies: dict,
        task_id: str,
        visited: set = None,
    ) -> int:
        """
        Calculate weight based on upstream tasks.

        Args:
            task_weights: Dict of {task_id: weight}
            dependencies: Dict of {task_id: [downstream_task_ids]}
            task_id: Task to calculate

        Returns:
            Sum of task weight + all upstream weights
        """
        if visited is None:
            visited = set()

        if task_id in visited:
            return 0
        visited.add(task_id)

        # Base weight
        weight = task_weights.get(task_id, 0)

        # Find upstream tasks (reverse dependencies)
        for upstream_id, downstream_list in dependencies.items():
            if task_id in downstream_list:
                weight += self.calculate_upstream(
                    task_weights, dependencies, upstream_id, visited
                )

        return weight

    def get_execution_order(
        self,
        task_weights: dict,
        dependencies: dict,
        weight_rule: str = "downstream",
    ) -> list:
        """
        Get expected task execution order based on priority.

        Args:
            task_weights: Dict of {task_id: weight}
            dependencies: Dict of {task_id: [downstream_task_ids]}
            weight_rule: "absolute", "downstream", or "upstream"

        Returns:
            List of task_ids in priority order (highest first)
        """
        effective_weights = {}

        for task_id in task_weights:
            if weight_rule == "absolute":
                effective_weights[task_id] = task_weights[task_id]
            elif weight_rule == "downstream":
                effective_weights[task_id] = self.calculate_downstream(
                    task_weights, dependencies, task_id
                )
            elif weight_rule == "upstream":
                effective_weights[task_id] = self.calculate_upstream(
                    task_weights, dependencies, task_id
                )

        # Sort by effective weight (descending)
        ordered = sorted(
            effective_weights.items(),
            key=lambda x: x[1],
            reverse=True,
        )

        return [task_id for task_id, weight in ordered]

    def analyze_dag_priorities(
        self,
        task_weights: dict,
        dependencies: dict,
    ) -> str:
        """
        Generate a priority analysis report for a DAG.

        Returns:
            Formatted analysis string
        """
        lines = [
            "=" * 60,
            "DAG PRIORITY ANALYSIS",
            "=" * 60,
            "",
            "Task Weights (Absolute):",
            "-" * 40,
        ]

        for task_id, weight in sorted(task_weights.items()):
            tier = Priority.get_tier(weight)
            lines.append(f"  {task_id}: {weight} ({tier})")

        # Calculate effective weights
        lines.extend([
            "",
            "Effective Weights (Downstream Rule):",
            "-" * 40,
        ])

        effective = {}
        for task_id in task_weights:
            effective[task_id] = self.calculate_downstream(
                task_weights, dependencies, task_id
            )

        for task_id, weight in sorted(effective.items(), key=lambda x: -x[1]):
            lines.append(f"  {task_id}: {weight}")

        # Execution order
        lines.extend([
            "",
            "Expected Execution Order:",
            "-" * 40,
        ])

        order = self.get_execution_order(task_weights, dependencies)
        for i, task_id in enumerate(order, 1):
            lines.append(f"  {i}. {task_id}")

        lines.append("=" * 60)
        return "\n".join(lines)


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test priority weights."""
    print("Priority Weights - Solution 14.2")
    print("=" * 60)

    # Test 1: Priority tiers
    print("\n1. PRIORITY TIERS")
    print("-" * 40)

    test_weights = [100, 85, 50, 25, 5]
    for weight in test_weights:
        tier = Priority.get_tier(weight)
        valid = Priority.validate_weight(weight)
        print(f"   Weight {weight:3d}: tier={tier:<12} valid={valid}")

    # Test 2: SLA-based priority
    print("\n2. SLA-BASED PRIORITY")
    print("-" * 40)

    sla_hours = [0.5, 2, 8, 18, 48]
    for hours in sla_hours:
        priority = Priority.from_sla_hours(hours)
        tier = Priority.get_tier(priority)
        print(f"   SLA {hours:4.1f}h: priority={priority:3d} ({tier})")

    # Test 3: Priority calculator
    print("\n3. PRIORITY CALCULATION")
    print("-" * 40)

    # Sample DAG structure:
    #     A (10)
    #    / \
    #   B   C
    #  (20) (30)
    #    \ /
    #     D (5)
    task_weights = {
        "A": 10,
        "B": 20,
        "C": 30,
        "D": 5,
    }
    dependencies = {
        "A": ["B", "C"],
        "B": ["D"],
        "C": ["D"],
        "D": [],
    }

    calc = PriorityCalculator()
    print(calc.analyze_dag_priorities(task_weights, dependencies))

    # Test execution order with different rules
    print("\n4. EXECUTION ORDER BY RULE")
    print("-" * 40)

    for rule in ["absolute", "downstream", "upstream"]:
        order = calc.get_execution_order(task_weights, dependencies, rule)
        print(f"   {rule:<12}: {' -> '.join(order)}")

    print("\n" + "=" * 60)
    print("Test complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()


# Export the DAG
priority_demo_dag = priority_demo()
