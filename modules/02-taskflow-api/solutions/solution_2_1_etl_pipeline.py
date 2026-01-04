"""
Solution 2.1: ETL Pipeline with TaskFlow API
=============================================

Complete solution demonstrating:
- Multiple extract sources running in parallel
- Transform layer with multiple_outputs
- Combine and load pattern
- Retry configuration
- Type hints throughout
"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task


@dag(
    dag_id="solution_2_1_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-02", "etl"],
    description="ETL Pipeline demonstrating TaskFlow patterns with multiple sources",
)
def etl_pipeline():
    """
    ETL Pipeline demonstrating TaskFlow patterns.

    Architecture:
    extract_users  ──┬──> transform_users  ──┬──> combine_data ──> load_to_destination
    extract_orders ──┴──> transform_orders ──┘
    """

    # =========================================================================
    # EXTRACT LAYER - Parallel extraction from multiple sources
    # =========================================================================

    @task(retries=2, retry_delay=timedelta(seconds=30))
    def extract_users() -> dict:
        """
        Extract user data from mock source.

        In production, this would connect to a database, API, or file system.
        Retries are configured to handle transient failures.
        """
        print("Extracting users from source...")

        # Simulate API call or database query
        users = [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
        ]

        print(f"Extracted {len(users)} users")
        return {"users": users}

    @task(retries=2, retry_delay=timedelta(seconds=30))
    def extract_orders() -> dict:
        """
        Extract order data from mock source.

        In production, this might come from an order management system.
        """
        print("Extracting orders from source...")

        # Simulate order data extraction
        orders = [
            {"id": 101, "user_id": 1, "amount": 99.99, "status": "completed"},
            {"id": 102, "user_id": 2, "amount": 149.99, "status": "completed"},
            {"id": 103, "user_id": 1, "amount": 49.99, "status": "completed"},
        ]

        print(f"Extracted {len(orders)} orders")
        return {"orders": orders}

    # =========================================================================
    # TRANSFORM LAYER - Process each source independently
    # =========================================================================

    @task
    def transform_users(data: dict) -> dict:
        """
        Transform user data by enriching with processing metadata.

        Args:
            data: Dictionary containing 'users' key with list of user dicts

        Returns:
            Dictionary with transformed users and count
        """
        print("Transforming user data...")

        users = data["users"]
        transformed_users = []

        for user in users:
            # Add processing metadata
            transformed_user = {
                **user,
                "processed": True,
                "username": user["name"].lower().replace(" ", "_"),
            }
            transformed_users.append(transformed_user)

        result = {
            "users": transformed_users,
            "user_count": len(transformed_users),
        }

        print(f"Transformed {result['user_count']} users")
        return result

    @task(multiple_outputs=True)
    def transform_orders(data: dict) -> dict:
        """
        Transform order data and calculate aggregates.

        Using multiple_outputs=True allows downstream tasks to access
        individual keys ('orders', 'total', 'order_count') separately.

        Args:
            data: Dictionary containing 'orders' key with list of order dicts

        Returns:
            Dictionary with orders, total, and count as separate XCom entries
        """
        print("Transforming order data...")

        orders = data["orders"]

        # Calculate aggregates
        total_amount = sum(order["amount"] for order in orders)
        order_count = len(orders)

        # Enrich each order
        transformed_orders = []
        for order in orders:
            transformed_order = {
                **order,
                "processed": True,
                "amount_formatted": f"${order['amount']:.2f}",
            }
            transformed_orders.append(transformed_order)

        result = {
            "orders": transformed_orders,
            "total": round(total_amount, 2),
            "order_count": order_count,
        }

        print(f"Transformed {order_count} orders, total: ${total_amount:.2f}")
        return result

    # =========================================================================
    # COMBINE LAYER - Merge transformed data
    # =========================================================================

    @task
    def combine_data(users_data: dict, orders_total: float, order_count: int) -> dict:
        """
        Combine transformed user and order data into a summary.

        Note: When using multiple_outputs, we can receive individual values
        rather than the full dictionary.

        Args:
            users_data: Transformed users dictionary
            orders_total: Total order amount (from multiple_outputs)
            order_count: Number of orders (from multiple_outputs)

        Returns:
            Combined summary dictionary
        """
        print("Combining user and order data...")

        summary = {
            "user_count": users_data["user_count"],
            "order_count": order_count,
            "total_revenue": orders_total,
            "avg_order_value": round(orders_total / order_count, 2) if order_count > 0 else 0,
            "processed_at": datetime.now().isoformat(),
        }

        print(f"Combined summary: {summary}")
        return summary

    # =========================================================================
    # LOAD LAYER - Output to destination
    # =========================================================================

    @task
    def load_to_destination(summary: dict) -> dict:
        """
        Load the combined data to destination.

        In production, this might write to a database, data warehouse,
        or send to an API endpoint.

        Args:
            summary: Combined data summary

        Returns:
            Load operation metadata
        """
        print("=" * 60)
        print("LOADING DATA TO DESTINATION")
        print("=" * 60)
        print(f"User Count: {summary['user_count']}")
        print(f"Order Count: {summary['order_count']}")
        print(f"Total Revenue: ${summary['total_revenue']:.2f}")
        print(f"Avg Order Value: ${summary['avg_order_value']:.2f}")
        print(f"Processed At: {summary['processed_at']}")
        print("=" * 60)

        # In production: db.insert(summary) or api.post(summary)

        return {
            "status": "success",
            "records_loaded": summary["user_count"] + summary["order_count"],
            "loaded_at": datetime.now().isoformat(),
        }

    # =========================================================================
    # WIRE UP THE PIPELINE
    # =========================================================================

    # Step 1: Extract (these run in parallel - no dependency between them)
    users_raw = extract_users()
    orders_raw = extract_orders()

    # Step 2: Transform (each waits only for its upstream extract)
    users_transformed = transform_users(users_raw)
    orders_transformed = transform_orders(orders_raw)

    # Step 3: Combine (waits for both transforms)
    # Note: With multiple_outputs=True, we can access individual keys
    combined = combine_data(
        users_data=users_transformed,
        orders_total=orders_transformed["total"],
        order_count=orders_transformed["order_count"],
    )

    # Step 4: Load
    load_to_destination(combined)


# Instantiate the DAG
etl_pipeline()
