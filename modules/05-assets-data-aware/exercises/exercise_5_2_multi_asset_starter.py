"""
Exercise 5.2: Multi-Asset Dependencies
======================================

Learn to create pipelines with multiple Asset dependencies.

You'll create:
- Three producer DAGs (orders, products, customers)
- One consumer DAG that waits for ALL three
"""

from datetime import datetime

# TODO: Import dag, task, and Asset from airflow.sdk
# from airflow.sdk import dag, task, Asset


# =========================================================================
# ASSET DEFINITIONS
# =========================================================================

# TODO: Define three Assets for the data sources
# orders_asset = Asset("postgres://warehouse/orders")
# products_asset = Asset("postgres://warehouse/products")
# customers_asset = Asset("postgres://warehouse/customers")


# =========================================================================
# PRODUCER DAG 1: Orders
# =========================================================================

# TODO: Create orders producer DAG
# @dag(
#     dag_id="exercise_5_2_orders_producer",
#     schedule=None,
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["exercise", "module-05", "assets", "producer", "orders"],
# )
def orders_producer():
    """Produces the orders Asset."""

    # TODO: Create task with outlet
    # @task(outlets=[orders_asset])
    # def load_orders():
    #     """Load orders data to warehouse."""
    #     print("=" * 60)
    #     print("ORDERS PRODUCER")
    #     print("=" * 60)
    #     print("Loading orders data...")
    #
    #     result = {
    #         "asset": "orders",
    #         "records": 5000,
    #         "timestamp": datetime.now().isoformat(),
    #     }
    #
    #     print(f"Loaded {result['records']} orders")
    #     print(f"Asset 'orders' will be updated")
    #     print("=" * 60)
    #     return result
    #
    # load_orders()

    pass  # Remove when implementing


# =========================================================================
# PRODUCER DAG 2: Products
# =========================================================================

# TODO: Create products producer DAG
# @dag(
#     dag_id="exercise_5_2_products_producer",
#     schedule=None,
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["exercise", "module-05", "assets", "producer", "products"],
# )
def products_producer():
    """Produces the products Asset."""

    # TODO: Create task with outlet
    # @task(outlets=[products_asset])
    # def load_products():
    #     """Load products data to warehouse."""
    #     print("=" * 60)
    #     print("PRODUCTS PRODUCER")
    #     print("=" * 60)
    #     print("Loading products data...")
    #
    #     result = {
    #         "asset": "products",
    #         "records": 1000,
    #         "timestamp": datetime.now().isoformat(),
    #     }
    #
    #     print(f"Loaded {result['records']} products")
    #     print(f"Asset 'products' will be updated")
    #     print("=" * 60)
    #     return result
    #
    # load_products()

    pass  # Remove when implementing


# =========================================================================
# PRODUCER DAG 3: Customers
# =========================================================================

# TODO: Create customers producer DAG
# @dag(
#     dag_id="exercise_5_2_customers_producer",
#     schedule=None,
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["exercise", "module-05", "assets", "producer", "customers"],
# )
def customers_producer():
    """Produces the customers Asset."""

    # TODO: Create task with outlet
    # @task(outlets=[customers_asset])
    # def load_customers():
    #     """Load customers data to warehouse."""
    #     print("=" * 60)
    #     print("CUSTOMERS PRODUCER")
    #     print("=" * 60)
    #     print("Loading customers data...")
    #
    #     result = {
    #         "asset": "customers",
    #         "records": 2500,
    #         "timestamp": datetime.now().isoformat(),
    #     }
    #
    #     print(f"Loaded {result['records']} customers")
    #     print(f"Asset 'customers' will be updated")
    #     print("=" * 60)
    #     return result
    #
    # load_customers()

    pass  # Remove when implementing


# =========================================================================
# CONSUMER DAG: Analytics
# =========================================================================

# TODO: Create consumer DAG with multi-Asset dependencies
# @dag(
#     dag_id="exercise_5_2_analytics_consumer",
#     schedule=[orders_asset, products_asset, customers_asset],
#     start_date=datetime(2024, 1, 1),
#     catchup=False,
#     tags=["exercise", "module-05", "assets", "consumer", "analytics"],
# )
def analytics_consumer():
    """
    Consumer DAG that runs analytics when ALL data sources are ready.

    Uses AND logic by default: [asset1, asset2, asset3]
    Only triggers when ALL Assets have been updated.
    """

    # TODO: Create task that logs triggering events
    # @task
    # def run_analytics(**context):
    #     """Run analytics on the combined data."""
    #     print("=" * 60)
    #     print("ANALYTICS CONSUMER")
    #     print("=" * 60)
    #     print()
    #     print("All data sources are ready!")
    #     print()
    #
    #     # Access triggering events
    #     events = context.get("triggering_asset_events", {})
    #
    #     print("Triggering Assets:")
    #     print("-" * 40)
    #     for asset, event_list in events.items():
    #         print(f"  Asset: {asset.uri}")
    #         for event in event_list:
    #             print(f"    - Timestamp: {event.timestamp}")
    #             print(f"    - Source: {event.source_dag_id}/{event.source_task_id}")
    #
    #     print()
    #     print("Running analytics...")
    #     print("  - Joining orders with customers...")
    #     print("  - Enriching with product information...")
    #     print("  - Calculating metrics...")
    #     print()
    #
    #     result = {
    #         "total_revenue": 1500000,
    #         "unique_customers": 2500,
    #         "products_sold": 800,
    #         "avg_order_value": 300,
    #     }
    #
    #     print("Analytics Results:")
    #     print("-" * 40)
    #     for metric, value in result.items():
    #         print(f"  {metric}: {value}")
    #
    #     print()
    #     print("Analytics completed successfully!")
    #     print("=" * 60)
    #
    #     return result
    #
    # run_analytics()

    pass  # Remove when implementing


# TODO: Instantiate all DAGs
# orders_producer()
# products_producer()
# customers_producer()
# analytics_consumer()
