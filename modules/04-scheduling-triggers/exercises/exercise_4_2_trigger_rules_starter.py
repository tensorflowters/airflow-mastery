"""
Exercise 4.2: Trigger Rules Pipeline
====================================

Learn to use trigger rules for conditional task execution.

You'll practice:
- Using different trigger rules
- Handling expected failures
- Building robust pipelines with cleanup tasks
"""

from datetime import datetime

# TODO: Import dag and task from airflow.sdk
# from airflow.sdk import dag, task

# TODO: Import TriggerRule
# from airflow.utils.trigger_rule import TriggerRule

# TODO: Import AirflowException for intentional failures
# from airflow.exceptions import AirflowException

# For random task outcome
# import random


# =========================================================================
# DAG DEFINITION
# =========================================================================

# TODO: Add @dag decorator
# @dag(
#     dag_id="exercise_4_2_trigger_rules",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-04", "trigger-rules"],
# )
def trigger_rules_dag():
    """
    Demonstrates trigger rules for conditional task execution.

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

    # =====================================================================
    # TASK 1: Start Task
    # =====================================================================

    # TODO: Create simple start task
    # @task
    # def start():
    #     """Starting point for the pipeline."""
    #     print("=" * 60)
    #     print("TRIGGER RULES PIPELINE STARTED")
    #     print("=" * 60)
    #     return "Pipeline started"

    # =====================================================================
    # TASK 2: Always Succeed
    # =====================================================================

    # TODO: Create task that always succeeds
    # @task
    # def always_succeed():
    #     """This task always succeeds."""
    #     print("✅ This task always succeeds!")
    #     return "success"

    # =====================================================================
    # TASK 3: Always Fail
    # =====================================================================

    # TODO: Create task that always fails
    # @task
    # def always_fail():
    #     """This task always fails with an exception."""
    #     print("❌ This task is about to fail...")
    #     raise AirflowException("This task always fails!")

    # =====================================================================
    # TASK 4: Random Outcome
    # =====================================================================

    # TODO: Create task with random success/failure
    # @task
    # def random_outcome():
    #     """This task randomly succeeds or fails (50/50)."""
    #     import random
    #
    #     if random.random() < 0.5:
    #         print("🎲 Random result: FAILURE")
    #         raise AirflowException("Random failure occurred!")
    #     else:
    #         print("🎲 Random result: SUCCESS")
    #         return "random success"

    # =====================================================================
    # TASK 5: All Done Task
    # =====================================================================

    # TODO: Create task that runs when all upstream complete (any state)
    # @task(trigger_rule=TriggerRule.ALL_DONE)
    # def all_done_task(**context):
    #     """
    #     Runs when ALL upstream tasks complete, regardless of state.
    #
    #     This is useful for:
    #     - Sending completion notifications
    #     - Aggregating results from parallel tasks
    #     - Logging pipeline statistics
    #     """
    #     print("=" * 60)
    #     print("ALL_DONE TRIGGER - All upstream tasks completed")
    #     print("=" * 60)
    #
    #     # Access the dag_run to check upstream states
    #     dag_run = context["dag_run"]
    #     task_instances = dag_run.get_task_instances()
    #
    #     print("\nUpstream task states:")
    #     for ti in task_instances:
    #         if ti.task_id != "all_done_task":
    #             print(f"  - {ti.task_id}: {ti.state}")
    #
    #     return "all_done executed"

    # =====================================================================
    # TASK 6: One Failed Task
    # =====================================================================

    # TODO: Create task that runs when at least one upstream failed
    # @task(trigger_rule=TriggerRule.ONE_FAILED)
    # def one_failed_task(**context):
    #     """
    #     Runs when AT LEAST ONE upstream task failed.
    #
    #     This is useful for:
    #     - Sending failure alerts
    #     - Triggering recovery procedures
    #     - Logging failure details
    #     """
    #     print("=" * 60)
    #     print("ONE_FAILED TRIGGER - At least one task failed!")
    #     print("=" * 60)
    #
    #     dag_run = context["dag_run"]
    #     task_instances = dag_run.get_task_instances()
    #
    #     failed_tasks = [
    #         ti.task_id for ti in task_instances
    #         if ti.state == "failed"
    #     ]
    #
    #     print(f"\n🚨 Failed tasks: {failed_tasks}")
    #     return {"failed_tasks": failed_tasks}

    # =====================================================================
    # TASK 7: Cleanup Task
    # =====================================================================

    # TODO: Create cleanup task that ALWAYS runs
    # @task(trigger_rule=TriggerRule.ALWAYS)
    # def cleanup(**context):
    #     """
    #     Runs ALWAYS, regardless of upstream task states.
    #
    #     This is useful for:
    #     - Releasing resources (connections, locks)
    #     - Cleaning up temporary files
    #     - Sending final notifications
    #     - Logging pipeline completion
    #     """
    #     print("=" * 60)
    #     print("CLEANUP - This task ALWAYS runs!")
    #     print("=" * 60)
    #
    #     dag_run = context["dag_run"]
    #     task_instances = dag_run.get_task_instances()
    #
    #     # Count states
    #     states = {}
    #     for ti in task_instances:
    #         state = ti.state or "pending"
    #         states[state] = states.get(state, 0) + 1
    #
    #     print("\nFinal pipeline summary:")
    #     for state, count in sorted(states.items()):
    #         print(f"  - {state}: {count} task(s)")
    #
    #     print("\n✨ Cleanup completed successfully!")
    #     return "cleanup done"

    # =====================================================================
    # WIRE UP THE TASKS
    # =====================================================================

    # TODO: Create the task flow with proper dependencies
    #
    # Pipeline structure:
    # start >> [always_succeed, always_fail, random_outcome]
    # [always_succeed, always_fail, random_outcome] >> all_done_task
    # [always_succeed, always_fail, random_outcome] >> one_failed_task
    # [all_done_task, one_failed_task] >> cleanup
    #
    # Example:
    # start_result = start()
    # succeed = always_succeed()
    # fail = always_fail()
    # random = random_outcome()
    #
    # start_result >> [succeed, fail, random]
    #
    # all_done = all_done_task()
    # one_failed = one_failed_task()
    # cleanup_task = cleanup()
    #
    # [succeed, fail, random] >> all_done
    # [succeed, fail, random] >> one_failed
    # [all_done, one_failed] >> cleanup_task

    pass  # Remove when implementing


# TODO: Instantiate the DAG
# trigger_rules_dag()
