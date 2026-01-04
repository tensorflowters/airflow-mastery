"""
Exercise 5.1: Basic Producer/Consumer
=====================================

Learn the fundamental Asset producer/consumer pattern.

You'll create:
- A producer DAG that signals data availability
- A consumer DAG that triggers on data updates
"""

from datetime import datetime

# TODO: Import dag, task, and Asset from airflow.sdk
# from airflow.sdk import dag, task, Asset


# =========================================================================
# ASSET DEFINITION
# =========================================================================

# TODO: Define the Asset that connects producer and consumer
# The Asset URI should be: "s3://data-lake/processed_data"
#
# processed_data = Asset("s3://data-lake/processed_data")


# =========================================================================
# PRODUCER DAG
# =========================================================================

# TODO: Create the producer DAG
# @dag(
#     dag_id="exercise_5_1_producer",
#     schedule="@hourly",
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["exercise", "module-05", "assets", "producer"],
#     description="Produces processed_data Asset",
# )
def producer_dag():
    """
    Producer DAG that processes data and signals availability.

    This DAG:
    1. Simulates data processing
    2. Updates the processed_data Asset via outlets
    3. Triggers any downstream consumer DAGs
    """

    # TODO: Create produce_data task with outlets
    # @task(outlets=[processed_data])
    # def produce_data():
    #     """
    #     Process data and signal completion via Asset.
    #
    #     The outlets=[processed_data] parameter tells Airflow
    #     to update the Asset when this task succeeds.
    #     """
    #     print("=" * 60)
    #     print("PRODUCER: Processing data...")
    #     print("=" * 60)
    #
    #     # Simulate data processing
    #     import time
    #     time.sleep(1)
    #
    #     result = {
    #         "processed_at": datetime.now().isoformat(),
    #         "records_processed": 1000,
    #         "status": "success",
    #     }
    #
    #     print(f"Processed {result['records_processed']} records")
    #     print(f"Timestamp: {result['processed_at']}")
    #     print()
    #     print("Asset 'processed_data' will be updated when this task completes.")
    #     print("Any consumer DAGs watching this Asset will be triggered.")
    #     print("=" * 60)
    #
    #     return result

    # TODO: Call the task
    # produce_data()

    pass  # Remove when implementing


# =========================================================================
# CONSUMER DAG
# =========================================================================

# TODO: Create the consumer DAG
# @dag(
#     dag_id="exercise_5_1_consumer",
#     schedule=[processed_data],  # Triggered by Asset!
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["exercise", "module-05", "assets", "consumer"],
#     description="Consumes processed_data Asset",
# )
def consumer_dag():
    """
    Consumer DAG that triggers when processed_data is updated.

    This DAG:
    1. Automatically runs when the producer updates the Asset
    2. Accesses triggering event information
    3. Processes the available data
    """

    # TODO: Create consume_data task
    # @task
    # def consume_data(**context):
    #     """
    #     Process the data that triggered this DAG.
    #
    #     Access triggering information via context.
    #     """
    #     print("=" * 60)
    #     print("CONSUMER: Data is available for processing!")
    #     print("=" * 60)
    #
    #     # Access triggering Asset events
    #     triggering_events = context.get("triggering_asset_events", {})
    #
    #     if triggering_events:
    #         print("\nTriggering events:")
    #         for asset, events in triggering_events.items():
    #             print(f"  Asset: {asset.uri}")
    #             for event in events:
    #                 print(f"    - Timestamp: {event.timestamp}")
    #                 print(f"    - Source DAG: {event.source_dag_id}")
    #                 print(f"    - Source Task: {event.source_task_id}")
    #     else:
    #         print("  No triggering events found (manual trigger?)")
    #
    #     # Simulate consuming the data
    #     print("\nProcessing the available data...")
    #     print("Consumer task completed successfully!")
    #     print("=" * 60)
    #
    #     return {"status": "consumed"}

    # TODO: Call the task
    # consume_data()

    pass  # Remove when implementing


# TODO: Instantiate both DAGs
# producer_dag()
# consumer_dag()
