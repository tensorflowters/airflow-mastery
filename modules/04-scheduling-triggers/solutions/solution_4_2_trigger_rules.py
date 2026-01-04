"""
Solution 4.2: Trigger Rules Pipeline
====================================

Complete solution demonstrating:
- Different trigger rule behaviors
- Intentional task failures
- Pipeline resilience patterns
- Cleanup task that always executes
"""

from datetime import datetime
import random
from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException


@dag(
    dag_id="solution_4_2_trigger_rules",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-04", "trigger-rules"],
    description="Demonstrates trigger rules for conditional task execution",
)
def trigger_rules_dag():
    """
    Trigger Rules Pipeline.

    Demonstrates how different trigger rules control task execution
    based on the states of upstream dependencies.

    Pipeline structure:
                        ┌─────────────────┐
                        │  always_succeed │──┐
                        └─────────────────┘  │
                                             │
    ┌─────────┐         ┌─────────────────┐  │    ┌──────────────┐    ┌─────────┐
    │  start  │────────▶│  always_fail    │──┼───▶│  all_done    │───▶│ cleanup │
    └─────────┘         └─────────────────┘  │    └──────────────┘    └─────────┘
                                             │           │
                        ┌─────────────────┐  │    ┌──────────────┐
                        │ random_outcome  │──┘    │  one_failed  │
                        └─────────────────┘       └──────────────┘
    """

    @task
    def start():
        """Starting point for the pipeline."""
        print("=" * 60)
        print("TRIGGER RULES PIPELINE STARTED")
        print("=" * 60)
        print()
        print("This pipeline demonstrates trigger rules:")
        print("  - TriggerRule.ALL_SUCCESS (default)")
        print("  - TriggerRule.ALL_DONE")
        print("  - TriggerRule.ONE_FAILED")
        print("  - TriggerRule.ALWAYS")
        print()
        print("Three parallel tasks will run:")
        print("  1. always_succeed - Always completes successfully")
        print("  2. always_fail - Always raises an exception")
        print("  3. random_outcome - 50/50 chance of success/failure")
        print("=" * 60)
        return "Pipeline started"

    @task
    def always_succeed():
        """This task always succeeds."""
        print("=" * 60)
        print("ALWAYS_SUCCEED TASK")
        print("=" * 60)
        print()
        print("✅ This task always succeeds!")
        print("   It represents a reliable operation that works correctly.")
        print()
        print("=" * 60)
        return "success"

    @task
    def always_fail():
        """This task always fails with an exception."""
        print("=" * 60)
        print("ALWAYS_FAIL TASK")
        print("=" * 60)
        print()
        print("❌ This task is designed to always fail.")
        print("   It simulates a known failure scenario.")
        print()
        print("   About to raise AirflowException...")
        print("=" * 60)
        raise AirflowException("This task always fails! (Intentional)")

    @task
    def random_outcome():
        """This task randomly succeeds or fails (50/50)."""
        print("=" * 60)
        print("RANDOM_OUTCOME TASK")
        print("=" * 60)
        print()

        outcome = random.random()
        print(f"🎲 Random value: {outcome:.4f}")
        print(f"   Threshold: 0.5")
        print()

        if outcome < 0.5:
            print("🎲 Result: FAILURE (value < 0.5)")
            print("=" * 60)
            raise AirflowException("Random failure occurred!")
        else:
            print("🎲 Result: SUCCESS (value >= 0.5)")
            print("=" * 60)
            return "random success"

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def all_done_task(**context):
        """
        Runs when ALL upstream tasks complete, regardless of state.

        trigger_rule=TriggerRule.ALL_DONE

        Use cases:
        - Sending completion notifications
        - Aggregating results from parallel tasks
        - Logging pipeline statistics
        - Post-processing regardless of individual task outcomes
        """
        print("=" * 60)
        print("ALL_DONE TRIGGER TASK")
        print("=" * 60)
        print()
        print("📋 This task runs because ALL upstream tasks completed.")
        print("   It doesn't matter if they succeeded, failed, or were skipped.")
        print()

        dag_run = context["dag_run"]
        task_instances = dag_run.get_task_instances()

        # Categorize upstream tasks by state
        states = {"success": [], "failed": [], "upstream_failed": [], "skipped": []}

        upstream_tasks = ["always_succeed", "always_fail", "random_outcome"]

        print("Upstream task states:")
        print("-" * 40)
        for ti in task_instances:
            if ti.task_id in upstream_tasks:
                state = ti.state or "pending"
                print(f"  • {ti.task_id}: {state}")
                if state in states:
                    states[state].append(ti.task_id)

        print()
        print("Summary:")
        print(f"  ✅ Succeeded: {len(states['success'])}")
        print(f"  ❌ Failed: {len(states['failed'])}")
        print(f"  ⏭️  Skipped: {len(states['skipped'])}")
        print()
        print("=" * 60)

        return {
            "trigger_rule": "ALL_DONE",
            "upstream_states": {
                task_id: ti.state
                for ti in task_instances
                if ti.task_id in upstream_tasks
            },
        }

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def one_failed_task(**context):
        """
        Runs when AT LEAST ONE upstream task failed.

        trigger_rule=TriggerRule.ONE_FAILED

        Use cases:
        - Sending failure alerts
        - Triggering recovery procedures
        - Logging failure details for debugging
        - Notifying on-call engineers
        """
        print("=" * 60)
        print("ONE_FAILED TRIGGER TASK")
        print("=" * 60)
        print()
        print("🚨 This task runs because AT LEAST ONE upstream task failed!")
        print()

        dag_run = context["dag_run"]
        task_instances = dag_run.get_task_instances()

        upstream_tasks = ["always_succeed", "always_fail", "random_outcome"]

        failed_tasks = [
            ti.task_id
            for ti in task_instances
            if ti.task_id in upstream_tasks and ti.state == "failed"
        ]

        print(f"Failed tasks: {failed_tasks}")
        print()
        print("In a real pipeline, this task might:")
        print("  • Send a Slack/PagerDuty alert")
        print("  • Write to an error log")
        print("  • Trigger a compensation workflow")
        print("  • Notify the data team")
        print()
        print("=" * 60)

        return {
            "trigger_rule": "ONE_FAILED",
            "failed_tasks": failed_tasks,
            "failed_count": len(failed_tasks),
        }

    @task(trigger_rule=TriggerRule.ALWAYS)
    def cleanup(**context):
        """
        Runs ALWAYS, regardless of upstream task states.

        trigger_rule=TriggerRule.ALWAYS

        Use cases:
        - Releasing resources (database connections, file locks)
        - Cleaning up temporary files
        - Sending final notifications
        - Logging pipeline completion metrics
        - Closing API sessions
        """
        print("=" * 60)
        print("CLEANUP TASK (ALWAYS RUNS)")
        print("=" * 60)
        print()
        print("✨ This task runs ALWAYS, no matter what happened upstream!")
        print()

        dag_run = context["dag_run"]
        task_instances = dag_run.get_task_instances()

        # Count all states
        state_counts = {}
        for ti in task_instances:
            state = ti.state or "pending"
            state_counts[state] = state_counts.get(state, 0) + 1

        print("Final pipeline summary:")
        print("-" * 40)
        for state, count in sorted(state_counts.items()):
            emoji = {
                "success": "✅",
                "failed": "❌",
                "skipped": "⏭️",
                "upstream_failed": "⬆️❌",
                "running": "🔄",
                "pending": "⏳",
            }.get(state, "•")
            print(f"  {emoji} {state}: {count} task(s)")

        print()
        print("Cleanup actions (simulated):")
        print("  • Released database connection pool")
        print("  • Deleted temporary files")
        print("  • Closed API session")
        print("  • Logged pipeline metrics")
        print()
        print("=" * 60)
        print("PIPELINE COMPLETED")
        print("=" * 60)

        return {
            "trigger_rule": "ALWAYS",
            "final_states": state_counts,
            "cleanup_status": "completed",
        }

    # Wire up the pipeline
    start_result = start()

    # Three parallel tasks with different behaviors
    succeed = always_succeed()
    fail = always_fail()
    random_result = random_outcome()

    # Start triggers the three parallel tasks
    start_result >> [succeed, fail, random_result]

    # all_done runs when all three complete (any state)
    all_done = all_done_task()
    [succeed, fail, random_result] >> all_done

    # one_failed runs when at least one fails
    one_failed = one_failed_task()
    [succeed, fail, random_result] >> one_failed

    # cleanup always runs at the end
    cleanup_result = cleanup()
    [all_done, one_failed] >> cleanup_result


# Instantiate the DAG
trigger_rules_dag()
