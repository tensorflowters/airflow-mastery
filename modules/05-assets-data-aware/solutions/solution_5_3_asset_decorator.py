"""
Solution 5.3: @asset Decorator Pattern
======================================

Complete solution demonstrating:
- The @asset decorator for asset-centric pipelines
- Chained Asset dependencies
- Data flow through function parameters
- Clean, self-documenting pipeline code
"""

from datetime import datetime
import random
from airflow.sdk import asset


# =========================================================================
# ASSET A: Raw Data (Bronze Layer)
# =========================================================================


@asset(
    uri="s3://data-lake/bronze/raw_data",
    schedule="@daily",
)
def raw_data():
    """
    Extract raw data from source API.

    The @asset decorator combines:
    1. Asset definition (uri)
    2. Scheduling (schedule)
    3. Producer logic (function body)

    This is the root of our data pipeline:
    - Runs daily on schedule
    - No upstream dependencies
    - Returns data that downstream Assets can use
    """
    print("=" * 60)
    print("RAW_DATA ASSET - Data Extraction")
    print("=" * 60)
    print()

    print("📥 Extracting data from source API...")
    print("   Source: https://api.example.com/events")
    print("   Method: GET")
    print()

    # Simulate API response
    records = []
    for i in range(100):
        records.append(
            {
                "id": i + 1,
                "event_type": random.choice(["click", "purchase", "view", "signup"]),
                "user_id": random.randint(1000, 9999),
                "value": round(random.uniform(10, 500), 2),
                "timestamp": datetime.now().isoformat(),
                "raw_metadata": {"source": "web", "version": "1.0"},
            }
        )

    result = {
        "records": records,
        "record_count": len(records),
        "extracted_at": datetime.now().isoformat(),
        "source": "external_api",
        "schema_version": "1.0",
    }

    print("Extraction Summary:")
    print("-" * 40)
    print(f"   Records extracted: {result['record_count']}")
    print(f"   Timestamp: {result['extracted_at']}")
    print(f"   Schema version: {result['schema_version']}")
    print()

    # Event type distribution
    event_types = {}
    for r in records:
        et = r["event_type"]
        event_types[et] = event_types.get(et, 0) + 1
    print("   Event distribution:")
    for et, count in sorted(event_types.items()):
        print(f"     - {et}: {count}")
    print()

    print("📤 Asset URI: s3://data-lake/bronze/raw_data")
    print("   Status: Available for downstream consumers")
    print("=" * 60)

    return result


# =========================================================================
# ASSET B: Transformed Data (Silver Layer)
# =========================================================================


@asset(
    uri="s3://data-lake/silver/transformed_data",
    schedule=[raw_data],  # Triggered when raw_data updates
)
def transformed_data(raw_data):
    """
    Transform and clean the raw data.

    The schedule=[raw_data] means:
    - This Asset triggers when raw_data is updated
    - No time-based schedule

    The parameter raw_data receives:
    - The return value of the raw_data Asset function
    - Automatic data passing through Airflow's infrastructure
    """
    print("=" * 60)
    print("TRANSFORMED_DATA ASSET - Data Transformation")
    print("=" * 60)
    print()

    print(f"📥 Received {raw_data['record_count']} raw records")
    print(f"   From: {raw_data['source']}")
    print(f"   Extracted at: {raw_data['extracted_at']}")
    print()

    print("🔄 Applying transformations...")
    transformations = [
        ("clean_nulls", "Replacing NULL values with defaults"),
        ("normalize_timestamps", "Converting to UTC"),
        ("categorize_values", "Bucketing values into tiers"),
        ("add_derived_fields", "Computing aggregates"),
        ("validate_schema", "Ensuring data quality"),
    ]

    for transform_id, description in transformations:
        print(f"   ✓ {transform_id}: {description}")
    print()

    # Apply transformations
    transformed_records = []
    for record in raw_data["records"]:
        value = record["value"]
        transformed_records.append(
            {
                "id": record["id"],
                "event_type": record["event_type"],
                "user_id": record["user_id"],
                "value": value,
                "value_tier": "high" if value > 300 else "medium" if value > 100 else "low",
                "value_normalized": round(value / 500, 4),
                "timestamp_utc": record["timestamp"],
                "processing_date": datetime.now().strftime("%Y-%m-%d"),
            }
        )

    # Calculate summary stats
    values = [r["value"] for r in transformed_records]
    tiers = {}
    for r in transformed_records:
        t = r["value_tier"]
        tiers[t] = tiers.get(t, 0) + 1

    result = {
        "records": transformed_records,
        "record_count": len(transformed_records),
        "transformed_at": datetime.now().isoformat(),
        "transformations_applied": [t[0] for t in transformations],
        "stats": {
            "min_value": min(values),
            "max_value": max(values),
            "avg_value": round(sum(values) / len(values), 2),
        },
        "tier_distribution": tiers,
    }

    print("Transformation Summary:")
    print("-" * 40)
    print(f"   Records: {result['record_count']}")
    print(f"   Transformations: {len(result['transformations_applied'])}")
    print(f"   Value stats: min={result['stats']['min_value']:.2f}, "
          f"max={result['stats']['max_value']:.2f}, "
          f"avg={result['stats']['avg_value']:.2f}")
    print(f"   Tier distribution: {tiers}")
    print()

    print("📤 Asset URI: s3://data-lake/silver/transformed_data")
    print("=" * 60)

    return result


# =========================================================================
# ASSET C: Warehouse Data (Gold Layer)
# =========================================================================


@asset(
    uri="postgres://warehouse/analytics_table",
    schedule=[transformed_data],  # Triggered when transformed_data updates
)
def warehouse_data(transformed_data):
    """
    Load transformed data to the analytics warehouse.

    This is the final Asset in the pipeline chain:
    raw_data → transformed_data → warehouse_data

    The data flows automatically through the pipeline:
    1. raw_data runs daily
    2. transformed_data triggers when raw_data completes
    3. warehouse_data triggers when transformed_data completes
    """
    print("=" * 60)
    print("WAREHOUSE_DATA ASSET - Data Loading")
    print("=" * 60)
    print()

    print(f"📥 Received {transformed_data['record_count']} transformed records")
    print(f"   Transformations: {transformed_data['transformations_applied']}")
    print(f"   Value stats: {transformed_data['stats']}")
    print()

    print("💾 Loading to warehouse...")
    print("   Target: postgres://warehouse/analytics_table")
    print("   Mode: APPEND")
    print()

    # Simulate warehouse operations
    print("   Operations:")
    print("     1. ✓ Validating schema compatibility")
    print("     2. ✓ Creating staging table")
    print("     3. ✓ Bulk inserting records")
    print("     4. ✓ Updating table statistics")
    print("     5. ✓ Refreshing materialized views")
    print()

    result = {
        "records_loaded": transformed_data["record_count"],
        "loaded_at": datetime.now().isoformat(),
        "target_table": "analytics_table",
        "target_schema": "warehouse",
        "load_mode": "append",
        "status": "success",
        "pipeline_stats": {
            "source_records": transformed_data["record_count"],
            "transformations": len(transformed_data["transformations_applied"]),
            "final_records": transformed_data["record_count"],
        },
    }

    print("Load Summary:")
    print("-" * 40)
    print(f"   Records loaded: {result['records_loaded']}")
    print(f"   Target: {result['target_schema']}.{result['target_table']}")
    print(f"   Mode: {result['load_mode']}")
    print(f"   Timestamp: {result['loaded_at']}")
    print()

    print("=" * 60)
    print("✅ ETL PIPELINE COMPLETE")
    print("=" * 60)
    print()
    print("Data lineage:")
    print("   ┌─────────────────┐")
    print("   │    raw_data     │  ← Daily schedule")
    print("   │    (Bronze)     │")
    print("   └────────┬────────┘")
    print("            │")
    print("            ▼")
    print("   ┌─────────────────┐")
    print("   │ transformed_data│  ← Triggered by raw_data")
    print("   │    (Silver)     │")
    print("   └────────┬────────┘")
    print("            │")
    print("            ▼")
    print("   ┌─────────────────┐")
    print("   │ warehouse_data  │  ← Triggered by transformed_data")
    print("   │    (Gold)       │")
    print("   └─────────────────┘")
    print()
    print("Benefits of @asset pattern:")
    print("   • Asset and producer colocated")
    print("   • Dependencies visible in function signature")
    print("   • Data flow is self-documenting")
    print("   • Less boilerplate than traditional DAGs")
    print("=" * 60)

    return result
