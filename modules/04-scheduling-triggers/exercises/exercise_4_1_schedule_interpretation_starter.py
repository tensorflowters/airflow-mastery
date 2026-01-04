"""
Exercise 4.1: Schedule Interpretation
=====================================

Understand Airflow scheduling concepts including:
- Cron expressions
- Logical dates vs data intervals
- Template variables
"""

from datetime import datetime

# TODO: Import dag, task from airflow.sdk
# from airflow.sdk import dag, task

# TODO: Import BashOperator for templating demo
# from airflow.providers.standard.operators.bash import BashOperator


# TODO: Add @dag decorator with schedule
# @dag(
#     dag_id="exercise_4_1_schedule_interpretation",
#     start_date=datetime(2024, 1, 1),
#     schedule="0 */6 * * 1-5",  # Every 6 hours on weekdays
#     catchup=False,
#     tags=["exercise", "module-04", "scheduling"],
# )
def schedule_interpretation_dag():
    """
    Demonstrates scheduling concepts and data intervals.

    Schedule: 0 */6 * * 1-5
    - Runs at 00:00, 06:00, 12:00, 18:00
    - Only on Monday (1) through Friday (5)
    """

    # =====================================================================
    # TASK 1: Explain the Schedule
    # =====================================================================

    # TODO: Create task that explains what the schedule means
    # @task
    # def explain_schedule():
    #     """Explain the cron schedule in human-readable terms."""
    #     schedule = "0 */6 * * 1-5"
    #
    #     print("=" * 60)
    #     print("SCHEDULE EXPLANATION")
    #     print("=" * 60)
    #     print(f"Cron expression: {schedule}")
    #     print()
    #     print("Breakdown:")
    #     print("  0     - At minute 0 (top of the hour)")
    #     print("  */6   - Every 6 hours (0, 6, 12, 18)")
    #     print("  *     - Every day of the month")
    #     print("  *     - Every month")
    #     print("  1-5   - Monday through Friday only")
    #     print()
    #     print("This DAG runs 4 times per weekday:")
    #     print("  - 00:00 (midnight)")
    #     print("  - 06:00 (6 AM)")
    #     print("  - 12:00 (noon)")
    #     print("  - 18:00 (6 PM)")
    #     print("=" * 60)

    # =====================================================================
    # TASK 2: Show Execution Context
    # =====================================================================

    # TODO: Create task that shows context variables
    # @task
    # def show_execution_context(**context):
    #     """Display execution context and data intervals."""
    #     logical_date = context["logical_date"]
    #     interval_start = context["data_interval_start"]
    #     interval_end = context["data_interval_end"]
    #
    #     print("=" * 60)
    #     print("EXECUTION CONTEXT")
    #     print("=" * 60)
    #
    #     print(f"Logical Date: {logical_date}")
    #     print(f"  - This is WHEN the DAG run is logically scheduled")
    #     print()
    #
    #     print(f"Data Interval Start: {interval_start}")
    #     print(f"Data Interval End: {interval_end}")
    #     print()
    #
    #     # Calculate interval duration
    #     if interval_start and interval_end:
    #         duration = interval_end - interval_start
    #         print(f"Interval Duration: {duration}")
    #         print(f"  - This run processes data from this time window")
    #     print()
    #
    #     print("KEY INSIGHT:")
    #     print("  For a scheduled DAG, logical_date == data_interval_end")
    #     print("  The run processes data from interval_start to interval_end")
    #     print("=" * 60)

    # =====================================================================
    # TASK 3: Demonstrate Templating
    # =====================================================================

    # TODO: Create BashOperator that uses Jinja templates
    # demonstrate_templating = BashOperator(
    #     task_id="demonstrate_templating",
    #     bash_command="""
    #         echo "========================================"
    #         echo "JINJA TEMPLATE VARIABLES"
    #         echo "========================================"
    #         echo ""
    #         echo "Date stamp (ds): {{ ds }}"
    #         echo "Date stamp no dashes: {{ ds_nodash }}"
    #         echo ""
    #         echo "Logical date: {{ logical_date }}"
    #         echo "Data interval start: {{ data_interval_start }}"
    #         echo "Data interval end: {{ data_interval_end }}"
    #         echo ""
    #         echo "Formatted examples:"
    #         echo "  File path: /data/{{ ds }}/output.csv"
    #         echo "  Partition: year={{ logical_date.year }}/month={{ logical_date.month }}"
    #         echo "========================================"
    #     """,
    # )

    # =====================================================================
    # WIRE UP THE TASKS
    # =====================================================================

    # TODO: Chain the tasks
    # explain_schedule() >> show_execution_context() >> demonstrate_templating

    pass  # Remove when implementing


# TODO: Instantiate the DAG
# schedule_interpretation_dag()
