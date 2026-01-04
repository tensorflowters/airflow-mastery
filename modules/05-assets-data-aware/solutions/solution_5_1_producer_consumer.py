"""
Solution 5.1: Basic Producer/Consumer
=====================================

Complete solution demonstrating:
- Asset definition and usage
- Producer task with outlets parameter
- Consumer DAG with Asset-based schedule
- Accessing triggering event information
"""

from datetime import datetime
from airflow.sdk import dag, task, Asset


# =========================================================================
# ASSET DEFINITION
# =========================================================================

# Define the Asset that connects producer and consumer
# The URI is a logical identifier - it doesn't need to be a real location
processed_data = Asset("s3://data-lake/processed_data")


# =========================================================================
# PRODUCER DAG
# =========================================================================


@dag(
    dag_id="solution_5_1_producer",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "module-05", "assets", "producer"],
    description="Produces processed_data Asset on hourly schedule",
)
def producer_dag():
    """
    Producer DAG.

    This DAG simulates data processing and signals completion
    by updating the processed_data Asset.

    When the produce_data task completes successfully, Airflow
    automatically updates the Asset, which triggers any
    consumer DAGs watching it.
    """

    @task(outlets=[processed_data])
    def produce_data():
        """
        Process data and signal completion via Asset outlet.

        The outlets=[processed_data] parameter tells Airflow:
        1. This task produces the processed_data Asset
        2. Update the Asset when this task succeeds
        3. Trigger any DAGs scheduled on this Asset
        """
        print("=" * 60)
        print("PRODUCER DAG - Data Processing")
        print("=" * 60)
        print()

        # Simulate data processing
        print("📊 Processing data...")
        import time

        time.sleep(1)

        # Generate processing metadata
        result = {
            "processed_at": datetime.now().isoformat(),
            "records_processed": 1000,
            "data_source": "raw_events_table",
            "output_location": "s3://data-lake/processed_data",
            "status": "success",
        }

        print()
        print("Processing Summary:")
        print("-" * 40)
        for key, value in result.items():
            print(f"  {key}: {value}")
        print()

        print("📤 Asset Update:")
        print(f"  Asset URI: {processed_data.uri}")
        print("  This task has outlets=[processed_data]")
        print("  When this task completes successfully:")
        print("    → The Asset will be marked as updated")
        print("    → Consumer DAGs will be triggered")
        print()
        print("=" * 60)

        return result

    # Execute the task
    produce_data()


# =========================================================================
# CONSUMER DAG
# =========================================================================


@dag(
    dag_id="solution_5_1_consumer",
    schedule=[processed_data],  # Triggered by Asset update!
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "module-05", "assets", "consumer"],
    description="Consumes processed_data Asset when updated",
)
def consumer_dag():
    """
    Consumer DAG.

    This DAG is scheduled with `schedule=[processed_data]`, which means:
    - It does NOT run on a time-based schedule
    - It runs whenever the processed_data Asset is updated
    - Multiple consumers can watch the same Asset
    """

    @task
    def consume_data(**context):
        """
        Process the data that triggered this DAG run.

        The context contains information about what triggered this run,
        accessible via the 'triggering_asset_events' key.
        """
        print("=" * 60)
        print("CONSUMER DAG - Data is Available!")
        print("=" * 60)
        print()

        print("🎉 This DAG was triggered because an Asset was updated!")
        print()

        # Access triggering Asset events
        triggering_events = context.get("triggering_asset_events", {})

        if triggering_events:
            print("📥 Triggering Events:")
            print("-" * 40)
            for asset, events in triggering_events.items():
                print(f"  Asset URI: {asset.uri}")
                for event in events:
                    print(f"    • Timestamp: {event.timestamp}")
                    print(f"    • Source DAG: {event.source_dag_id}")
                    print(f"    • Source Task: {event.source_task_id}")
                    if event.extra:
                        print(f"    • Extra: {event.extra}")
        else:
            print("📝 No triggering events found")
            print("   This can happen with manual triggers or during testing")

        print()
        print("📊 Consuming the data...")
        print("   Reading from: s3://data-lake/processed_data")
        print("   Performing downstream processing...")
        print()

        # Simulate data consumption
        result = {
            "consumed_at": datetime.now().isoformat(),
            "source_asset": "s3://data-lake/processed_data",
            "records_consumed": 1000,
            "status": "success",
        }

        print("Consumption Summary:")
        print("-" * 40)
        for key, value in result.items():
            print(f"  {key}: {value}")
        print()

        print("✅ Consumer task completed successfully!")
        print("=" * 60)

        return result

    @task
    def log_completion(consume_result: dict):
        """Log the completion of the data consumption."""
        print("=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)
        print()
        print(f"Data consumed at: {consume_result['consumed_at']}")
        print(f"Records processed: {consume_result['records_consumed']}")
        print()
        print("The Asset-based pipeline completed successfully:")
        print("  1. Producer DAG processed data")
        print("  2. Producer task updated the Asset (via outlets)")
        print("  3. Consumer DAG was automatically triggered")
        print("  4. Consumer processed the available data")
        print()
        print("=" * 60)

    # Chain the tasks
    result = consume_data()
    log_completion(result)


# Instantiate both DAGs
producer_dag()
consumer_dag()
