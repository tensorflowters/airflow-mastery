"""
Solution 14.1: Pool Configuration
==================================

Complete implementation of Airflow pool management.
"""

import logging
from typing import Optional

from airflow.models import Pool
from airflow.utils.session import create_session
from airflow.sdk import dag, task
from datetime import datetime

logger = logging.getLogger(__name__)


# =============================================================================
# PART 1: POOL MANAGER
# =============================================================================


class PoolManager:
    """
    Utility class for managing Airflow pools.

    Pools limit concurrent task execution for shared resources
    like database connections, API rate limits, etc.
    """

    DEFAULT_POOLS = {
        "default_pool": {"slots": 128, "description": "Default pool"},
        "database_connections": {"slots": 10, "description": "Database connection pool"},
        "api_rate_limit": {"slots": 5, "description": "External API rate limiting"},
        "high_memory": {"slots": 3, "description": "Memory-intensive tasks"},
        "gpu_compute": {"slots": 2, "description": "GPU computation tasks"},
    }

    def create_pool(
        self,
        name: str,
        slots: int,
        description: str = None,
    ) -> bool:
        """
        Create or update a pool.

        Args:
            name: Pool name
            slots: Number of slots
            description: Pool description

        Returns:
            True if successful
        """
        try:
            with create_session() as session:
                pool = Pool(
                    pool=name,
                    slots=slots,
                    description=description or f"Pool: {name}",
                )
                session.merge(pool)  # Upsert behavior
                session.commit()
                logger.info(f"Created/updated pool: {name} with {slots} slots")
                return True
        except Exception as e:
            logger.error(f"Failed to create pool {name}: {e}")
            return False

    def delete_pool(self, name: str) -> bool:
        """
        Delete a pool.

        Args:
            name: Pool name

        Returns:
            True if deleted, False if not found or protected
        """
        if name == "default_pool":
            logger.warning("Cannot delete default_pool")
            return False

        try:
            with create_session() as session:
                result = session.query(Pool).filter(Pool.pool == name).delete()
                session.commit()

                if result > 0:
                    logger.info(f"Deleted pool: {name}")
                    return True
                else:
                    logger.warning(f"Pool not found: {name}")
                    return False
        except Exception as e:
            logger.error(f"Failed to delete pool {name}: {e}")
            return False

    def get_pool_status(self, name: str) -> Optional[dict]:
        """
        Get pool utilization status.

        Args:
            name: Pool name

        Returns:
            Pool status dict or None if not found
        """
        try:
            pool = Pool.get_pool(name)
            if not pool:
                return None

            running = pool.running_slots()
            queued = pool.queued_slots()
            total = pool.slots

            return {
                "name": pool.pool,
                "description": pool.description,
                "slots": total,
                "running": running,
                "queued": queued,
                "open": pool.open_slots(),
                "utilization": (running / total * 100) if total > 0 else 0,
            }
        except Exception as e:
            logger.error(f"Failed to get pool status for {name}: {e}")
            return None

    def list_pools(self) -> list:
        """
        List all pools with status.

        Returns:
            List of pool status dicts
        """
        try:
            pools = Pool.get_pools()
            return [
                self.get_pool_status(pool.pool)
                for pool in pools
                if pool.pool != "default_pool" or True  # Include all
            ]
        except Exception as e:
            logger.error(f"Failed to list pools: {e}")
            return []

    def initialize_default_pools(self) -> int:
        """
        Create default pools if they don't exist.

        Returns:
            Number of pools created
        """
        created = 0
        for name, config in self.DEFAULT_POOLS.items():
            if self.create_pool(name, config["slots"], config["description"]):
                created += 1
        return created

    def get_pool_utilization_report(self) -> str:
        """Generate a formatted pool utilization report."""
        lines = [
            "=" * 60,
            "POOL UTILIZATION REPORT",
            "=" * 60,
            "",
            f"{'Pool':<25} {'Slots':>6} {'Running':>8} {'Queued':>7} {'Util %':>7}",
            "-" * 60,
        ]

        for status in self.list_pools():
            if status:
                lines.append(
                    f"{status['name']:<25} "
                    f"{status['slots']:>6} "
                    f"{status['running']:>8} "
                    f"{status['queued']:>7} "
                    f"{status['utilization']:>6.1f}%"
                )

        lines.append("=" * 60)
        return "\n".join(lines)


# =============================================================================
# PART 2: POOL-AWARE DAG
# =============================================================================


@dag(
    dag_id="solution_pool_aware_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "pools", "module-14"],
)
def pool_aware_etl():
    """
    DAG demonstrating pool usage for resource management.

    Uses different pools for different resource types.
    """

    @task(pool="database_connections", pool_slots=1)
    def extract_users():
        """Extract from users table - uses 1 DB connection slot."""
        logger.info("Extracting users data")
        return [{"id": i, "name": f"User {i}"} for i in range(10)]

    @task(pool="database_connections", pool_slots=1)
    def extract_orders():
        """Extract from orders table - uses 1 DB connection slot."""
        logger.info("Extracting orders data")
        return [{"id": i, "user_id": i % 10, "amount": i * 100} for i in range(100)]

    @task(pool="database_connections", pool_slots=2)
    def extract_heavy_report():
        """
        Heavy report extraction - uses 2 DB connection slots.

        This task is more resource-intensive, so it reserves
        more slots to limit concurrent executions.
        """
        logger.info("Extracting heavy report")
        return {"report": "Complex aggregation results"}

    @task(pool="api_rate_limit", pool_slots=1)
    def enrich_with_external_api(users: list):
        """
        Enrich data with external API - respects rate limits.

        The api_rate_limit pool ensures we don't exceed
        the external API's rate limits.
        """
        logger.info(f"Enriching {len(users)} users with external data")
        for user in users:
            user["enriched"] = True
        return users

    @task(pool="high_memory", pool_slots=1)
    def compute_analytics(users: list, orders: list):
        """
        Memory-intensive analytics computation.

        The high_memory pool limits concurrent heavy computations
        to prevent memory exhaustion.
        """
        logger.info("Computing analytics")
        return {
            "total_users": len(users),
            "total_orders": len(orders),
            "avg_order_value": sum(o["amount"] for o in orders) / len(orders),
        }

    @task(pool="database_connections", pool_slots=1)
    def load_results(analytics: dict, enriched_users: list):
        """Load results back to database."""
        logger.info(f"Loading results: {analytics}")
        return {"status": "success", "analytics": analytics}

    # Define dependencies
    users = extract_users()
    orders = extract_orders()
    heavy = extract_heavy_report()

    enriched = enrich_with_external_api(users)
    analytics = compute_analytics(users, orders)

    load_results(analytics, enriched)


# =============================================================================
# PART 3: DYNAMIC POOL SIZING
# =============================================================================


class DynamicPoolManager(PoolManager):
    """
    Dynamic pool sizing based on system load.

    Adjusts pool sizes automatically to optimize resource usage.
    """

    BASE_SIZES = {
        "database_connections": 10,
        "api_rate_limit": 5,
        "high_memory": 3,
        "gpu_compute": 2,
    }

    HIGH_LOAD_THRESHOLD = 0.8
    LOW_LOAD_THRESHOLD = 0.3
    MIN_SLOTS = 1
    MAX_MULTIPLIER = 3.0

    def get_current_load(self) -> float:
        """
        Get current system load based on pool utilization.

        Returns:
            Load factor between 0.0 and 1.0
        """
        pools = self.list_pools()
        if not pools:
            return 0.5

        # Calculate average utilization across all pools
        total_utilization = sum(
            p.get("utilization", 0) for p in pools if p
        )
        return min(1.0, total_utilization / (len(pools) * 100))

    def adjust_pool_size(self, name: str, current_load: float = None) -> int:
        """
        Adjust pool size based on system load.

        Args:
            name: Pool name
            current_load: Load factor (auto-detect if None)

        Returns:
            New pool size
        """
        if current_load is None:
            current_load = self.get_current_load()

        base_size = self.BASE_SIZES.get(name, 5)

        # Determine multiplier based on load
        if current_load > self.HIGH_LOAD_THRESHOLD:
            # High load: reduce capacity
            multiplier = 0.5
            logger.info(f"High load ({current_load:.1%}): reducing {name}")
        elif current_load < self.LOW_LOAD_THRESHOLD:
            # Low load: increase capacity
            multiplier = 1.5
            logger.info(f"Low load ({current_load:.1%}): increasing {name}")
        else:
            # Normal load: use base size
            multiplier = 1.0

        new_size = max(self.MIN_SLOTS, int(base_size * multiplier))
        self.create_pool(name, new_size, f"Auto-sized pool: {name}")

        return new_size

    def scale_pool(self, name: str, factor: float) -> int:
        """
        Scale pool by a factor.

        Args:
            name: Pool name
            factor: Scale factor (e.g., 0.5 for half, 2.0 for double)

        Returns:
            New pool size
        """
        status = self.get_pool_status(name)
        if not status:
            logger.warning(f"Pool not found for scaling: {name}")
            return 0

        current_size = status["slots"]
        new_size = max(self.MIN_SLOTS, int(current_size * factor))

        # Cap at maximum multiplier of base size
        base_size = self.BASE_SIZES.get(name, current_size)
        max_size = int(base_size * self.MAX_MULTIPLIER)
        new_size = min(new_size, max_size)

        self.create_pool(name, new_size)
        logger.info(f"Scaled {name}: {current_size} -> {new_size} (factor: {factor})")

        return new_size

    def auto_scale_all(self) -> dict:
        """
        Auto-scale all managed pools based on current load.

        Returns:
            Dict of {pool_name: new_size}
        """
        current_load = self.get_current_load()
        results = {}

        for pool_name in self.BASE_SIZES:
            new_size = self.adjust_pool_size(pool_name, current_load)
            results[pool_name] = new_size

        return results

    def emergency_scale_down(self, factor: float = 0.5) -> dict:
        """
        Emergency scale down all pools.

        Used when system is under severe load.

        Args:
            factor: Scale down factor

        Returns:
            Dict of {pool_name: new_size}
        """
        logger.warning(f"Emergency scale down by factor {factor}")
        results = {}

        for pool_name in self.BASE_SIZES:
            new_size = self.scale_pool(pool_name, factor)
            results[pool_name] = new_size

        return results


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test pool configuration."""
    print("Pool Configuration - Solution 14.1")
    print("=" * 60)

    # Note: Requires Airflow database connection
    print("\nThis solution requires an Airflow environment.")
    print("Run within Airflow context to test:")
    print()
    print("  from solution_14_1_pool_configuration import PoolManager")
    print("  manager = PoolManager()")
    print("  manager.initialize_default_pools()")
    print("  print(manager.get_pool_utilization_report())")
    print()

    # Demonstrate concepts without database
    print("\nPool Concepts Demonstration:")
    print("-" * 40)

    print("\n1. Default Pools:")
    for name, config in PoolManager.DEFAULT_POOLS.items():
        print(f"   {name}: {config['slots']} slots - {config['description']}")

    print("\n2. Dynamic Pool Sizing:")
    print("   Load > 80%: Reduce pool by 50%")
    print("   Load < 30%: Increase pool by 50%")
    print("   Otherwise: Use base size")

    print("\n3. Pool-Aware DAG Tasks:")
    print("   extract_users: pool=database_connections, slots=1")
    print("   extract_orders: pool=database_connections, slots=1")
    print("   extract_heavy_report: pool=database_connections, slots=2")
    print("   enrich_with_external_api: pool=api_rate_limit, slots=1")
    print("   compute_analytics: pool=high_memory, slots=1")

    print("\n" + "=" * 60)
    print("Demonstration complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()


# Export the DAG
pool_aware_etl_dag = pool_aware_etl()
