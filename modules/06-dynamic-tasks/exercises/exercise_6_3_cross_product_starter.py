"""
Exercise 6.3: Cross-Product Processing
======================================

Learn partial() + expand() for Cartesian product patterns.

You'll create:
- A config task that returns regions and products
- A mapped task that processes each combination
- An aggregation task that summarizes by dimension
"""

from datetime import datetime

# TODO: Import dag and task from airflow.sdk
# from airflow.sdk import dag, task


# =========================================================================
# DAG DEFINITION
# =========================================================================

# TODO: Add @dag decorator
# @dag(
#     dag_id="exercise_6_3_cross_product",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-06", "dynamic-tasks", "cross-product"],
#     description="Cross-product processing with partial() and expand()",
# )
def cross_product_dag():
    """
    Cross-Product Processing Pipeline.

    Processes all combinations of:
    - 3 regions: us, eu, apac
    - 4 products: widget, gadget, doohickey, thingamajig

    Total: 3 × 4 = 12 task instances

    This pattern is useful for:
    - Regional reporting (region × metric)
    - A/B testing (variant × segment)
    - Data validation (table × rule)
    - Multi-dimensional aggregation
    """

    # =====================================================================
    # TASK 1: Get Configuration
    # =====================================================================

    # TODO: Create task that returns regions and products
    # @task
    # def get_regions() -> list[str]:
    #     """Return list of regions to process."""
    #     regions = ["us", "eu", "apac"]
    #     print(f"Regions: {regions}")
    #     return regions
    #
    # @task
    # def get_products() -> list[str]:
    #     """Return list of products to process."""
    #     products = ["widget", "gadget", "doohickey", "thingamajig"]
    #     print(f"Products: {products}")
    #     return products

    # =====================================================================
    # TASK 2: Process Combination (Mapped)
    # =====================================================================

    # TODO: Create task that processes a single region + product
    # @task
    # def process_combination(region: str, product: str) -> dict:
    #     """
    #     Process a single region + product combination.
    #
    #     When used with partial().expand() on multiple arguments,
    #     this task runs once for EACH combination (Cartesian product).
    #     """
    #     import random
    #
    #     print("=" * 50)
    #     print(f"PROCESSING: {region.upper()} - {product}")
    #     print("=" * 50)
    #
    #     # Simulate regional product data
    #     base_sales = {
    #         "us": 10000,
    #         "eu": 7500,
    #         "apac": 12000,
    #     }.get(region, 5000)
    #
    #     product_multiplier = {
    #         "widget": 1.0,
    #         "gadget": 0.8,
    #         "doohickey": 0.6,
    #         "thingamajig": 0.4,
    #     }.get(product, 0.5)
    #
    #     sales = int(base_sales * product_multiplier * random.uniform(0.8, 1.2))
    #     units = int(sales / random.randint(10, 50))
    #
    #     result = {
    #         "region": region,
    #         "product": product,
    #         "sales": sales,
    #         "units": units,
    #         "avg_price": round(sales / units, 2) if units > 0 else 0,
    #         "status": "processed",
    #     }
    #
    #     print(f"  Sales: ${result['sales']:,}")
    #     print(f"  Units: {result['units']:,}")
    #     print(f"  Avg Price: ${result['avg_price']}")
    #     print("=" * 50)
    #
    #     return result

    # =====================================================================
    # TASK 3: Aggregate Results
    # =====================================================================

    # TODO: Create task that aggregates all results
    # @task
    # def aggregate_results(results: list[dict]) -> dict:
    #     """
    #     Aggregate results from all combinations.
    #
    #     Provides summaries by:
    #     - Region (total across all products)
    #     - Product (total across all regions)
    #     - Grand total
    #     """
    #     print("=" * 60)
    #     print("AGGREGATE RESULTS")
    #     print("=" * 60)
    #     print()
    #
    #     # Group by region
    #     by_region = {}
    #     for r in results:
    #         region = r["region"]
    #         if region not in by_region:
    #             by_region[region] = {"sales": 0, "units": 0, "count": 0}
    #         by_region[region]["sales"] += r["sales"]
    #         by_region[region]["units"] += r["units"]
    #         by_region[region]["count"] += 1
    #
    #     # Group by product
    #     by_product = {}
    #     for r in results:
    #         product = r["product"]
    #         if product not in by_product:
    #             by_product[product] = {"sales": 0, "units": 0, "count": 0}
    #         by_product[product]["sales"] += r["sales"]
    #         by_product[product]["units"] += r["units"]
    #         by_product[product]["count"] += 1
    #
    #     # Calculate totals
    #     total_sales = sum(r["sales"] for r in results)
    #     total_units = sum(r["units"] for r in results)
    #
    #     report = {
    #         "combinations_processed": len(results),
    #         "by_region": by_region,
    #         "by_product": by_product,
    #         "totals": {
    #             "sales": total_sales,
    #             "units": total_units,
    #         },
    #     }
    #
    #     print(f"📊 Processed {len(results)} combinations")
    #     print()
    #
    #     print("By Region:")
    #     for region, stats in by_region.items():
    #         print(f"  {region.upper()}: ${stats['sales']:,} ({stats['count']} products)")
    #     print()
    #
    #     print("By Product:")
    #     for product, stats in by_product.items():
    #         print(f"  {product}: ${stats['sales']:,} ({stats['count']} regions)")
    #     print()
    #
    #     print(f"Grand Total: ${total_sales:,} ({total_units:,} units)")
    #     print("=" * 60)
    #
    #     return report

    # =====================================================================
    # WIRE UP THE PIPELINE
    # =====================================================================

    # TODO: Connect with cross-product expansion
    #
    # Option 1: Using separate tasks for each dimension
    # regions = get_regions()
    # products = get_products()
    # processed = process_combination.expand(region=regions, product=products)
    #
    # Option 2: Using expand() with literal lists
    # processed = process_combination.expand(
    #     region=["us", "eu", "apac"],
    #     product=["widget", "gadget", "doohickey", "thingamajig"]
    # )
    #
    # Then aggregate:
    # aggregate_results(processed)

    pass  # Remove when implementing


# TODO: Instantiate the DAG
# cross_product_dag()
