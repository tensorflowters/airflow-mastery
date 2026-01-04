"""
Exercise 5.3: @asset Decorator Pattern
======================================

Learn the Airflow 3.x @asset decorator for asset-centric pipelines.

You'll create:
- raw_data Asset (daily schedule)
- transformed_data Asset (depends on raw_data)
- warehouse_data Asset (depends on transformed_data)
"""

from datetime import datetime

# TODO: Import asset from airflow.sdk
# from airflow.sdk import asset


# =========================================================================
# ASSET A: Raw Data (Source)
# =========================================================================

# TODO: Create raw_data Asset with @asset decorator
# @asset(
#     uri="s3://data-lake/bronze/raw_data",
#     schedule="@daily",
# )
# def raw_data():
#     """
#     Extract raw data from source API.
#
#     This is the first Asset in the pipeline:
#     - Runs on a daily schedule
#     - Extracts data from an external source
#     - Makes data available for downstream Assets
#     """
#     print("=" * 60)
#     print("RAW_DATA ASSET - Extraction")
#     print("=" * 60)
#     print()
#
#     print("📥 Extracting data from source API...")
#
#     # Simulate API data extraction
#     import random
#     records = [
#         {
#             "id": i,
#             "name": f"Record {i}",
#             "value": random.randint(100, 1000),
#             "timestamp": datetime.now().isoformat(),
#         }
#         for i in range(100)
#     ]
#
#     result = {
#         "records": records,
#         "record_count": len(records),
#         "extracted_at": datetime.now().isoformat(),
#         "source": "external_api",
#     }
#
#     print(f"   Extracted {result['record_count']} records")
#     print(f"   Timestamp: {result['extracted_at']}")
#     print()
#     print("📤 Data available at: s3://data-lake/bronze/raw_data")
#     print("=" * 60)
#
#     return result


# =========================================================================
# ASSET B: Transformed Data (Silver Layer)
# =========================================================================

# TODO: Create transformed_data Asset that depends on raw_data
# @asset(
#     uri="s3://data-lake/silver/transformed_data",
#     schedule=[raw_data],  # Triggered by raw_data
# )
# def transformed_data(raw_data):  # Receives raw_data output
#     """
#     Transform the raw data.
#
#     This Asset:
#     - Triggers when raw_data is updated
#     - Receives raw_data output as input parameter
#     - Applies transformations
#     - Makes clean data available for downstream
#     """
#     print("=" * 60)
#     print("TRANSFORMED_DATA ASSET - Transformation")
#     print("=" * 60)
#     print()
#
#     print(f"📥 Received {raw_data['record_count']} raw records")
#     print()
#
#     print("🔄 Applying transformations...")
#     print("   - Cleaning null values")
#     print("   - Normalizing formats")
#     print("   - Calculating derived fields")
#     print()
#
#     # Simulate transformation
#     transformed_records = []
#     for record in raw_data["records"]:
#         transformed_records.append({
#             **record,
#             "value_normalized": record["value"] / 1000,
#             "category": "A" if record["value"] > 500 else "B",
#         })
#
#     result = {
#         "records": transformed_records,
#         "record_count": len(transformed_records),
#         "transformed_at": datetime.now().isoformat(),
#         "transformations": ["clean_nulls", "normalize", "categorize"],
#     }
#
#     print(f"   Transformed {result['record_count']} records")
#     print(f"   Applied {len(result['transformations'])} transformations")
#     print()
#     print("📤 Data available at: s3://data-lake/silver/transformed_data")
#     print("=" * 60)
#
#     return result


# =========================================================================
# ASSET C: Warehouse Data (Gold Layer)
# =========================================================================

# TODO: Create warehouse_data Asset that depends on transformed_data
# @asset(
#     uri="postgres://warehouse/analytics_table",
#     schedule=[transformed_data],  # Triggered by transformed_data
# )
# def warehouse_data(transformed_data):  # Receives transformed_data output
#     """
#     Load transformed data to the warehouse.
#
#     This is the final Asset in the pipeline:
#     - Triggers when transformed_data is updated
#     - Loads data to the analytics warehouse
#     - Makes data ready for consumption
#     """
#     print("=" * 60)
#     print("WAREHOUSE_DATA ASSET - Loading")
#     print("=" * 60)
#     print()
#
#     print(f"📥 Received {transformed_data['record_count']} transformed records")
#     print()
#
#     print("💾 Loading to warehouse...")
#     print("   Target: postgres://warehouse/analytics_table")
#     print("   Mode: append")
#     print()
#
#     result = {
#         "records_loaded": transformed_data["record_count"],
#         "loaded_at": datetime.now().isoformat(),
#         "target_table": "analytics_table",
#         "status": "success",
#     }
#
#     print("Load Summary:")
#     print(f"   Records loaded: {result['records_loaded']}")
#     print(f"   Target: {result['target_table']}")
#     print(f"   Timestamp: {result['loaded_at']}")
#     print()
#     print("✅ ETL Pipeline Complete!")
#     print()
#     print("Data Flow:")
#     print("   raw_data → transformed_data → warehouse_data")
#     print("=" * 60)
#
#     return result
