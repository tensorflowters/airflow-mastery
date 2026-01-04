"""
Solution 6.3: Cross-Product Processing
======================================

Complete solution demonstrating:
- Cartesian product with expand() on multiple arguments
- partial() for fixed parameters
- expand_kwargs() for explicit combinations
- Multi-dimensional result aggregation
"""

from datetime import datetime
import random
from airflow.sdk import dag, task


@dag(
    dag_id="solution_6_3_cross_product",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-06", "dynamic-tasks", "cross-product"],
    description="Cross-product processing with partial() and expand()",
)
def cross_product_dag():
    """
    Cross-Product Processing Pipeline.

    Demonstrates the Cartesian product pattern where tasks run
    for every combination of multiple dimensions:

    Regions (3): us, eu, apac
    Products (4): widget, gadget, doohickey, thingamajig

    Total combinations: 3 × 4 = 12 task instances

    This pattern is commonly used for:
    - Multi-region data processing
    - A/B testing across segments
    - Data validation matrices
    - Reporting by multiple dimensions
    """

    @task
    def get_regions() -> list[str]:
        """
        Return the list of regions to process.

        This task's output will be expanded along with products
        to create all combinations.
        """
        regions = ["us", "eu", "apac"]

        print("=" * 60)
        print("GET REGIONS")
        print("=" * 60)
        print(f"Regions to process: {regions}")
        print(f"Count: {len(regions)}")
        print("=" * 60)

        return regions

    @task
    def get_products() -> list[str]:
        """
        Return the list of products to process.

        Combined with regions via expand(), this creates
        the Cartesian product of all combinations.
        """
        products = ["widget", "gadget", "doohickey", "thingamajig"]

        print("=" * 60)
        print("GET PRODUCTS")
        print("=" * 60)
        print(f"Products to process: {products}")
        print(f"Count: {len(products)}")
        print("=" * 60)

        return products

    @task
    def process_combination(region: str, product: str, **context) -> dict:
        """
        Process a single region + product combination.

        When expand() is used with multiple arguments, Airflow creates
        the Cartesian product (all combinations). The order is:
        - First argument varies slowest
        - Last argument varies fastest

        For regions=["us", "eu", "apac"] and products=["a", "b", "c", "d"]:
        [0] us+a, [1] us+b, [2] us+c, [3] us+d,
        [4] eu+a, [5] eu+b, [6] eu+c, [7] eu+d,
        [8] apac+a, [9] apac+b, [10] apac+c, [11] apac+d
        """
        map_index = context.get("map_index", 0)

        print("=" * 50)
        print(f"PROCESS COMBINATION [{map_index}]")
        print(f"Region: {region.upper()} | Product: {product}")
        print("=" * 50)
        print()

        # Simulate regional base data
        regional_factors = {
            "us": {"base_sales": 10000, "currency": "USD", "timezone": "America/New_York"},
            "eu": {"base_sales": 7500, "currency": "EUR", "timezone": "Europe/London"},
            "apac": {"base_sales": 12000, "currency": "USD", "timezone": "Asia/Tokyo"},
        }

        # Simulate product popularity
        product_popularity = {
            "widget": 1.0,
            "gadget": 0.85,
            "doohickey": 0.65,
            "thingamajig": 0.45,
        }

        region_data = regional_factors.get(region, {"base_sales": 5000})
        product_mult = product_popularity.get(product, 0.5)

        # Calculate simulated metrics
        base = region_data["base_sales"]
        variance = random.uniform(0.8, 1.2)
        sales = int(base * product_mult * variance)
        units = int(sales / random.randint(15, 45))
        returns = int(units * random.uniform(0.02, 0.08))
        margin = round(random.uniform(0.15, 0.35), 2)

        result = {
            "region": region,
            "product": product,
            "map_index": map_index,
            "metrics": {
                "sales": sales,
                "units": units,
                "returns": returns,
                "net_units": units - returns,
                "avg_price": round(sales / units, 2) if units > 0 else 0,
                "margin": margin,
                "profit": int(sales * margin),
            },
            "status": "processed",
        }

        print(f"📊 Metrics for {region.upper()} - {product}:")
        print(f"   Sales: ${result['metrics']['sales']:,}")
        print(f"   Units: {result['metrics']['units']:,}")
        print(f"   Returns: {result['metrics']['returns']:,}")
        print(f"   Avg Price: ${result['metrics']['avg_price']}")
        print(f"   Margin: {result['metrics']['margin']:.1%}")
        print(f"   Profit: ${result['metrics']['profit']:,}")
        print()
        print("=" * 50)

        return result

    @task
    def aggregate_results(results: list[dict]) -> dict:
        """
        Aggregate results from all region × product combinations.

        Provides multi-dimensional summaries for analysis and reporting.
        """
        print("=" * 60)
        print("AGGREGATE RESULTS")
        print("=" * 60)
        print()

        print(f"📊 Processing {len(results)} combinations...")
        print()

        # Initialize aggregation structures
        by_region = {}
        by_product = {}

        for r in results:
            region = r["region"]
            product = r["product"]
            metrics = r["metrics"]

            # Aggregate by region
            if region not in by_region:
                by_region[region] = {
                    "sales": 0, "units": 0, "profit": 0,
                    "products": 0
                }
            by_region[region]["sales"] += metrics["sales"]
            by_region[region]["units"] += metrics["units"]
            by_region[region]["profit"] += metrics["profit"]
            by_region[region]["products"] += 1

            # Aggregate by product
            if product not in by_product:
                by_product[product] = {
                    "sales": 0, "units": 0, "profit": 0,
                    "regions": 0
                }
            by_product[product]["sales"] += metrics["sales"]
            by_product[product]["units"] += metrics["units"]
            by_product[product]["profit"] += metrics["profit"]
            by_product[product]["regions"] += 1

        # Calculate grand totals
        total_sales = sum(r["metrics"]["sales"] for r in results)
        total_units = sum(r["metrics"]["units"] for r in results)
        total_profit = sum(r["metrics"]["profit"] for r in results)

        # Build report
        report = {
            "summary": {
                "combinations": len(results),
                "regions": len(by_region),
                "products": len(by_product),
                "total_sales": total_sales,
                "total_units": total_units,
                "total_profit": total_profit,
                "overall_margin": round(total_profit / total_sales, 2) if total_sales > 0 else 0,
            },
            "by_region": by_region,
            "by_product": by_product,
            "top_performers": {
                "best_region": max(by_region.items(), key=lambda x: x[1]["sales"])[0],
                "best_product": max(by_product.items(), key=lambda x: x[1]["sales"])[0],
            },
        }

        # Print report
        print("=" * 60)
        print("📈 SALES BY REGION")
        print("-" * 40)
        for region, stats in sorted(by_region.items(), key=lambda x: x[1]["sales"], reverse=True):
            pct = stats["sales"] / total_sales * 100
            print(f"   {region.upper():6}: ${stats['sales']:>10,} ({pct:5.1f}%)")
        print()

        print("📦 SALES BY PRODUCT")
        print("-" * 40)
        for product, stats in sorted(by_product.items(), key=lambda x: x[1]["sales"], reverse=True):
            pct = stats["sales"] / total_sales * 100
            print(f"   {product:12}: ${stats['sales']:>10,} ({pct:5.1f}%)")
        print()

        print("💰 TOTALS")
        print("-" * 40)
        print(f"   Total Sales: ${total_sales:,}")
        print(f"   Total Units: {total_units:,}")
        print(f"   Total Profit: ${total_profit:,}")
        print(f"   Overall Margin: {report['summary']['overall_margin']:.1%}")
        print()

        print("🏆 TOP PERFORMERS")
        print("-" * 40)
        print(f"   Best Region: {report['top_performers']['best_region'].upper()}")
        print(f"   Best Product: {report['top_performers']['best_product']}")
        print()

        print("=" * 60)
        print("✅ CROSS-PRODUCT PROCESSING COMPLETE")
        print("=" * 60)

        return report

    # =====================================================================
    # WIRE UP THE PIPELINE
    # =====================================================================

    # Get the dimension lists
    regions = get_regions()
    products = get_products()

    # Use expand() with multiple arguments for Cartesian product
    # This creates 3 × 4 = 12 task instances
    processed = process_combination.expand(
        region=regions,
        product=products
    )

    # Aggregate all results
    aggregate_results(processed)


# Instantiate the DAG
cross_product_dag()
