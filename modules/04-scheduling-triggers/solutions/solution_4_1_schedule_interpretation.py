"""
Solution 4.1: Schedule Interpretation
=====================================

Complete solution demonstrating:
- Cron expression scheduling
- Data interval concepts
- Jinja template usage
- Context variable access
"""

from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="solution_4_1_schedule_interpretation",
    start_date=datetime(2024, 1, 1),
    schedule="0 */6 * * 1-5",  # Every 6 hours on weekdays
    catchup=False,
    tags=["solution", "module-04", "scheduling"],
    description="Demonstrates scheduling concepts and data intervals",
)
def schedule_interpretation_dag():
    """
    Schedule Interpretation DAG.

    Cron: 0 */6 * * 1-5
    Meaning: Run at 00:00, 06:00, 12:00, 18:00 on Mon-Fri

    This DAG demonstrates:
    1. How to interpret cron schedules
    2. The relationship between logical_date and data_interval
    3. How to use Jinja templates with scheduling context
    """

    @task
    def explain_schedule():
        """Explain the cron schedule in human-readable terms."""
        schedule = "0 */6 * * 1-5"

        print("=" * 60)
        print("SCHEDULE EXPLANATION")
        print("=" * 60)
        print(f"Cron expression: {schedule}")
        print()
        print("Position breakdown:")
        print("  ┌───────────── minute (0)")
        print("  │ ┌───────────── hour (*/6 = every 6 hours)")
        print("  │ │ ┌───────────── day of month (*)")
        print("  │ │ │ ┌───────────── month (*)")
        print("  │ │ │ │ ┌───────────── day of week (1-5 = Mon-Fri)")
        print("  │ │ │ │ │")
        print("  0 */6 * * 1-5")
        print()
        print("This DAG runs 4 times per weekday:")
        print("  - 00:00 (midnight) - processes data from 18:00-00:00")
        print("  - 06:00 (6 AM)     - processes data from 00:00-06:00")
        print("  - 12:00 (noon)     - processes data from 06:00-12:00")
        print("  - 18:00 (6 PM)     - processes data from 12:00-18:00")
        print()
        print("Note: Does NOT run on Saturday (6) or Sunday (0)")
        print("=" * 60)

    @task
    def show_execution_context(**context):
        """Display execution context and explain data intervals."""
        logical_date = context["logical_date"]
        interval_start = context["data_interval_start"]
        interval_end = context["data_interval_end"]
        dag_run = context["dag_run"]

        print("=" * 60)
        print("EXECUTION CONTEXT")
        print("=" * 60)

        print("\n📅 LOGICAL DATE")
        print(f"   Value: {logical_date}")
        print("   Meaning: The logical timestamp for this DAG run")
        print("   Use case: Partition data, name files, filter queries")

        print("\n📊 DATA INTERVAL")
        print(f"   Start: {interval_start}")
        print(f"   End:   {interval_end}")

        if interval_start and interval_end:
            duration = interval_end - interval_start
            print(f"   Duration: {duration}")

            print("\n   Meaning: This run processes data generated between")
            print(f"            {interval_start} and {interval_end}")

        print("\n🔑 KEY RELATIONSHIPS")
        print("   1. For scheduled runs: logical_date == data_interval_end")
        print("   2. The run is triggered AFTER data_interval_end")
        print("   3. Query data WHERE timestamp >= interval_start")
        print("                   AND timestamp < interval_end")

        print("\n📌 RUN METADATA")
        print(f"   Run ID: {dag_run.run_id}")
        print(f"   Run Type: {dag_run.run_type}")
        print(f"   Queued: {dag_run.queued_at}")

        print("=" * 60)

        return {
            "logical_date": str(logical_date),
            "interval_start": str(interval_start),
            "interval_end": str(interval_end),
        }

    demonstrate_templating = BashOperator(
        task_id="demonstrate_templating",
        bash_command="""
            echo "========================================"
            echo "JINJA TEMPLATE VARIABLES"
            echo "========================================"
            echo ""
            echo "📆 DATE STAMPS"
            echo "   ds:          {{ ds }}"
            echo "   ds_nodash:   {{ ds_nodash }}"
            echo ""
            echo "⏰ TIMESTAMPS"
            echo "   logical_date:         {{ logical_date }}"
            echo "   data_interval_start:  {{ data_interval_start }}"
            echo "   data_interval_end:    {{ data_interval_end }}"
            echo ""
            echo "📁 PRACTICAL EXAMPLES"
            echo "   File path:   /data/{{ ds }}/output.csv"
            echo "   S3 key:      s3://bucket/data/{{ ds_nodash }}/file.parquet"
            echo "   Partition:   year={{ logical_date.year }}/month={{ logical_date.month }}/day={{ logical_date.day }}"
            echo ""
            echo "📊 SQL QUERY EXAMPLE"
            echo "   SELECT * FROM events"
            echo "   WHERE event_time >= '{{ data_interval_start }}'"
            echo "   AND event_time < '{{ data_interval_end }}'"
            echo "========================================"
        """,
        doc_md="""
        ### Template Variables

        This task demonstrates Jinja templating with Airflow macros.
        Templates are rendered at runtime with actual execution values.
        """,
    )

    # Wire up the tasks
    explain_schedule() >> show_execution_context() >> demonstrate_templating


# Instantiate the DAG
schedule_interpretation_dag()
