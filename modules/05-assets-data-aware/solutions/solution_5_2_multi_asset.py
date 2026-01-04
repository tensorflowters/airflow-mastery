"""
Solution 5.2: Multi-Asset Dependencies
======================================

Complete solution demonstrating:
- Multiple Asset definitions
- Three producer DAGs
- Consumer with AND logic (wait for all)
- Logging triggering events from multiple sources
"""

from datetime import datetime
from airflow.sdk import dag, task, Asset


# =========================================================================
# ASSET DEFINITIONS
# =========================================================================

# Define three Assets representing different data sources
orders_asset = Asset("postgres://warehouse/orders")
products_asset = Asset("postgres://warehouse/products")
customers_asset = Asset("postgres://warehouse/customers")


# =========================================================================
# PRODUCER DAG 1: Orders
# =========================================================================


@dag(
    dag_id="solution_5_2_orders_producer",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "module-05", "assets", "producer", "orders"],
    description="Loads orders data and updates orders Asset",
)
def orders_producer():
    """
    Orders Producer DAG.

    Simulates loading order data to the warehouse and
    signals availability via the orders Asset.
    """

    @task(outlets=[orders_asset])
    def load_orders():
        """Load orders data and update the orders Asset."""
        print("=" * 60)
        print("ORDERS PRODUCER - Data Loading")
        print("=" * 60)
        print()

        print("📦 Loading orders data to warehouse...")
        print("   Source: orders_api")
        print("   Target: postgres://warehouse/orders")
        print()

        # Simulate order data
        import random

        record_count = random.randint(4000, 6000)

        result = {
            "asset": "orders",
            "records_loaded": record_count,
            "timestamp": datetime.now().isoformat(),
            "source": "orders_api",
            "status": "success",
        }

        print("Load Summary:")
        print("-" * 40)
        print(f"   Records loaded: {record_count:,}")
        print(f"   Timestamp: {result['timestamp']}")
        print()

        print("📤 Asset Update:")
        print(f"   Asset: {orders_asset.uri}")
        print("   Status: Will be updated on task success")
        print()
        print("=" * 60)

        return result

    load_orders()


# =========================================================================
# PRODUCER DAG 2: Products
# =========================================================================


@dag(
    dag_id="solution_5_2_products_producer",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "module-05", "assets", "producer", "products"],
    description="Loads products data and updates products Asset",
)
def products_producer():
    """
    Products Producer DAG.

    Simulates loading product catalog data to the warehouse.
    """

    @task(outlets=[products_asset])
    def load_products():
        """Load products data and update the products Asset."""
        print("=" * 60)
        print("PRODUCTS PRODUCER - Data Loading")
        print("=" * 60)
        print()

        print("🏷️ Loading products data to warehouse...")
        print("   Source: product_catalog_db")
        print("   Target: postgres://warehouse/products")
        print()

        import random

        record_count = random.randint(800, 1200)

        result = {
            "asset": "products",
            "records_loaded": record_count,
            "timestamp": datetime.now().isoformat(),
            "source": "product_catalog_db",
            "categories": ["electronics", "clothing", "home", "sports"],
            "status": "success",
        }

        print("Load Summary:")
        print("-" * 40)
        print(f"   Records loaded: {record_count:,}")
        print(f"   Categories: {len(result['categories'])}")
        print(f"   Timestamp: {result['timestamp']}")
        print()

        print("📤 Asset Update:")
        print(f"   Asset: {products_asset.uri}")
        print("=" * 60)

        return result

    load_products()


# =========================================================================
# PRODUCER DAG 3: Customers
# =========================================================================


@dag(
    dag_id="solution_5_2_customers_producer",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "module-05", "assets", "producer", "customers"],
    description="Loads customers data and updates customers Asset",
)
def customers_producer():
    """
    Customers Producer DAG.

    Simulates loading customer data to the warehouse.
    """

    @task(outlets=[customers_asset])
    def load_customers():
        """Load customers data and update the customers Asset."""
        print("=" * 60)
        print("CUSTOMERS PRODUCER - Data Loading")
        print("=" * 60)
        print()

        print("👥 Loading customers data to warehouse...")
        print("   Source: crm_system")
        print("   Target: postgres://warehouse/customers")
        print()

        import random

        record_count = random.randint(2000, 3000)

        result = {
            "asset": "customers",
            "records_loaded": record_count,
            "timestamp": datetime.now().isoformat(),
            "source": "crm_system",
            "segments": ["enterprise", "small_business", "consumer"],
            "status": "success",
        }

        print("Load Summary:")
        print("-" * 40)
        print(f"   Records loaded: {record_count:,}")
        print(f"   Segments: {len(result['segments'])}")
        print(f"   Timestamp: {result['timestamp']}")
        print()

        print("📤 Asset Update:")
        print(f"   Asset: {customers_asset.uri}")
        print("=" * 60)

        return result

    load_customers()


# =========================================================================
# CONSUMER DAG: Analytics
# =========================================================================


@dag(
    dag_id="solution_5_2_analytics_consumer",
    schedule=[orders_asset, products_asset, customers_asset],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "module-05", "assets", "consumer", "analytics"],
    description="Runs analytics when all data sources are ready",
)
def analytics_consumer():
    """
    Analytics Consumer DAG.

    This DAG demonstrates multi-Asset dependencies using AND logic.

    schedule=[orders_asset, products_asset, customers_asset]
    means this DAG will ONLY trigger when ALL THREE Assets
    have been updated. It waits for complete data availability.
    """

    @task
    def log_triggering_events(**context):
        """Log which Assets triggered this run."""
        print("=" * 60)
        print("ANALYTICS CONSUMER - Trigger Analysis")
        print("=" * 60)
        print()

        print("🎉 All required data sources are now available!")
        print()

        events = context.get("triggering_asset_events", {})

        print("📥 Triggering Assets:")
        print("-" * 40)

        asset_info = {}
        for asset, event_list in events.items():
            asset_name = asset.uri.split("/")[-1]
            print(f"\n  📊 {asset.uri}")
            for event in event_list:
                print(f"      Timestamp: {event.timestamp}")
                print(f"      Source DAG: {event.source_dag_id}")
                print(f"      Source Task: {event.source_task_id}")
                asset_info[asset_name] = {
                    "uri": asset.uri,
                    "timestamp": str(event.timestamp),
                    "source_dag": event.source_dag_id,
                }

        print()
        print("=" * 60)
        return asset_info

    @task
    def run_analytics(trigger_info: dict):
        """Run analytics on the combined data sources."""
        print("=" * 60)
        print("ANALYTICS CONSUMER - Running Analytics")
        print("=" * 60)
        print()

        print("📊 Analytics Pipeline:")
        print()

        # Step 1: Join data
        print("Step 1: Joining data sources...")
        print("   - Joining orders ← customers (on customer_id)")
        print("   - Joining orders ← products (on product_id)")
        print()

        # Step 2: Calculate metrics
        print("Step 2: Calculating metrics...")
        import random

        metrics = {
            "total_revenue": random.randint(1000000, 2000000),
            "total_orders": random.randint(4000, 6000),
            "unique_customers": random.randint(2000, 3000),
            "products_sold": random.randint(700, 1000),
            "avg_order_value": random.randint(250, 350),
            "repeat_customer_rate": round(random.uniform(0.3, 0.5), 2),
        }

        print("   Metrics calculated:")
        for metric, value in metrics.items():
            if isinstance(value, float):
                print(f"     - {metric}: {value:.1%}")
            elif value > 1000:
                print(f"     - {metric}: ${value:,}" if "revenue" in metric else f"     - {metric}: {value:,}")
            else:
                print(f"     - {metric}: {value}")
        print()

        # Step 3: Generate insights
        print("Step 3: Generating insights...")
        insights = [
            "Top product category: Electronics (35% of revenue)",
            "Customer segment with highest LTV: Enterprise",
            "Peak order day: Friday",
        ]
        for insight in insights:
            print(f"   💡 {insight}")
        print()

        print("=" * 60)

        return {
            "metrics": metrics,
            "insights": insights,
            "data_sources": list(trigger_info.keys()),
        }

    @task
    def save_results(analytics_result: dict):
        """Save analytics results."""
        print("=" * 60)
        print("ANALYTICS CONSUMER - Saving Results")
        print("=" * 60)
        print()

        print("💾 Saving analytics results...")
        print(f"   Data sources used: {analytics_result['data_sources']}")
        print(f"   Metrics calculated: {len(analytics_result['metrics'])}")
        print(f"   Insights generated: {len(analytics_result['insights'])}")
        print()

        print("📍 Output locations:")
        print("   - s3://analytics/daily_metrics.parquet")
        print("   - postgres://warehouse/analytics_summary")
        print()

        print("✅ Analytics pipeline completed successfully!")
        print("=" * 60)

        return {"status": "success", "timestamp": datetime.now().isoformat()}

    # Wire up the pipeline
    trigger_info = log_triggering_events()
    analytics = run_analytics(trigger_info)
    save_results(analytics)


# Instantiate all DAGs
orders_producer()
products_producer()
customers_producer()
analytics_consumer()
