"""
Example DAG: Asset Pipeline (Data-Aware Scheduling)

Demonstrates Airflow's Asset (formerly Dataset) feature for
data-aware scheduling. Producer DAGs update assets,
consumer DAGs trigger when assets are updated.

Module: 05-assets
"""

from datetime import datetime

from airflow.sdk import dag, task, Asset


# =============================================================================
# Define Assets
# =============================================================================

# Assets represent logical data entities that DAGs produce/consume
raw_sales_data = Asset("s3://data-lake/raw/sales/daily")
processed_sales = Asset("s3://data-lake/processed/sales/daily")
sales_report = Asset("s3://reports/sales/daily")

# Assets can have any URI scheme - they're logical identifiers
user_events = Asset("postgres://warehouse/user_events")
aggregated_metrics = Asset("postgres://warehouse/aggregated_metrics")


# =============================================================================
# Producer DAG: Ingests Raw Data
# =============================================================================


@dag(
    dag_id="05_asset_producer_raw",
    description="Produces raw sales data asset",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["example", "module-05", "assets", "producer"],
)
def asset_producer_raw():
    """Producer DAG that ingests raw sales data."""

    @task(outlets=[raw_sales_data])
    def ingest_sales_data() -> dict:
        """
        Ingest raw sales data from source systems.

        The `outlets` parameter declares that this task
        produces the raw_sales_data asset.
        """
        print("Ingesting raw sales data...")

        # Simulated ingestion
        records = [
            {"id": 1, "product": "Widget A", "amount": 99.99},
            {"id": 2, "product": "Widget B", "amount": 149.99},
            {"id": 3, "product": "Widget C", "amount": 199.99},
        ]

        print(f"Ingested {len(records)} sales records")
        return {"record_count": len(records), "status": "success"}

    ingest_sales_data()


# =============================================================================
# Consumer DAG: Processes Raw Data (Triggered by Asset)
# =============================================================================


@dag(
    dag_id="05_asset_consumer_processor",
    description="Consumes raw sales and produces processed sales",
    start_date=datetime(2024, 1, 1),
    # Schedule on asset updates - triggers when raw_sales_data is updated
    schedule=[raw_sales_data],
    catchup=False,
    tags=["example", "module-05", "assets", "consumer", "producer"],
)
def asset_consumer_processor():
    """Consumer DAG that processes raw sales data."""

    @task
    def validate_raw_data() -> dict:
        """Validate the raw data before processing."""
        print("Validating raw sales data...")
        return {"validation": "passed", "errors": 0}

    @task(outlets=[processed_sales])
    def transform_sales_data(validation_result: dict) -> dict:
        """
        Transform raw data into processed format.

        This task:
        - Consumes: raw_sales_data (via DAG schedule)
        - Produces: processed_sales (via outlets)
        """
        print("Transforming sales data...")

        if validation_result["validation"] != "passed":
            raise ValueError("Validation failed")

        # Simulated transformation
        transformed = {
            "aggregated_sales": 449.97,
            "record_count": 3,
            "processed_at": datetime.now().isoformat(),
        }

        print(f"Processed sales: ${transformed['aggregated_sales']}")
        return transformed

    validation = validate_raw_data()
    transform_sales_data(validation)


# =============================================================================
# Consumer DAG: Generates Reports (Multiple Asset Dependencies)
# =============================================================================


@dag(
    dag_id="05_asset_consumer_reporter",
    description="Generates reports from processed sales",
    start_date=datetime(2024, 1, 1),
    # Trigger when processed_sales is updated
    schedule=[processed_sales],
    catchup=False,
    tags=["example", "module-05", "assets", "consumer"],
)
def asset_consumer_reporter():
    """Consumer DAG that generates sales reports."""

    @task(outlets=[sales_report])
    def generate_sales_report() -> dict:
        """
        Generate sales report from processed data.

        Triggers when processed_sales asset is updated.
        Produces sales_report asset when complete.
        """
        print("Generating sales report...")

        report = {
            "title": "Daily Sales Report",
            "generated_at": datetime.now().isoformat(),
            "summary": "All sales metrics within normal ranges",
        }

        print(f"Report generated: {report['title']}")
        return report

    generate_sales_report()


# =============================================================================
# Multi-Asset Consumer (AND Logic)
# =============================================================================


@dag(
    dag_id="05_asset_multi_consumer",
    description="Triggers when multiple assets are updated",
    start_date=datetime(2024, 1, 1),
    # AND logic: triggers when BOTH assets have been updated
    schedule=[raw_sales_data, user_events],
    catchup=False,
    tags=["example", "module-05", "assets", "consumer"],
)
def asset_multi_consumer():
    """Consumer DAG that triggers on multiple asset updates."""

    @task
    def correlate_data() -> dict:
        """
        Correlate sales with user events.

        Only runs when BOTH raw_sales_data AND user_events
        have been updated since the last run.
        """
        print("Correlating sales with user events...")

        correlation = {
            "sales_events_matched": 150,
            "correlation_score": 0.87,
            "insights": ["Peak sales during evening hours"],
        }

        print(f"Correlation complete: {correlation['sales_events_matched']} matches")
        return correlation

    correlate_data()


# =============================================================================
# Manual Producer (Updates Asset Programmatically)
# =============================================================================


@dag(
    dag_id="05_asset_manual_trigger",
    description="Manually updates an asset",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["example", "module-05", "assets", "producer"],
)
def asset_manual_trigger():
    """Manual producer DAG for backfilling user events."""

    @task(outlets=[user_events])
    def backfill_user_events() -> dict:
        """
        Manually backfill user events.

        This can be triggered manually to update the
        user_events asset, which will in turn trigger
        any DAGs scheduled on that asset.
        """
        print("Backfilling user events...")

        events = {
            "backfill_start": "2024-01-01",
            "backfill_end": "2024-01-31",
            "events_loaded": 10000,
        }

        print(f"Backfilled {events['events_loaded']} events")
        return events

    backfill_user_events()


# Instantiate all DAGs
asset_producer_raw()
asset_consumer_processor()
asset_consumer_reporter()
asset_multi_consumer()
asset_manual_trigger()
