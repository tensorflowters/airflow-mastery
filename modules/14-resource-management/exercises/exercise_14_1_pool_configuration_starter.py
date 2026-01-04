"""
Exercise 14.1: Pool Configuration (Starter)
============================================

Learn to configure and manage Airflow pools.

TODO: Complete all the implementation sections.
"""

from typing import Optional
import logging

logger = logging.getLogger(__name__)


# =============================================================================
# PART 1: POOL MANAGER
# =============================================================================


class PoolManager:
    """
    Utility class for managing Airflow pools.

    Pools limit concurrent task execution for shared resources.
    """

    # Default pools to create
    DEFAULT_POOLS = {
        "default_pool": {"slots": 128, "description": "Default pool"},
        "database_connections": {"slots": 10, "description": "Database connection pool"},
        "api_rate_limit": {"slots": 5, "description": "External API rate limiting"},
        "high_memory": {"slots": 3, "description": "Memory-intensive tasks"},
    }

    def create_pool(
        self,
        name: str,
        slots: int,
        description: str = None,
    ) -> bool:
        """
        Create or update a pool.

        TODO: Implement:
        1. Create Pool object
        2. Use session.merge() for upsert
        3. Commit and return True

        Args:
            name: Pool name
            slots: Number of slots
            description: Pool description

        Returns:
            True if successful
        """
        # TODO: Implement
        pass

    def delete_pool(self, name: str) -> bool:
        """
        Delete a pool.

        TODO: Implement pool deletion.
        Note: Cannot delete 'default_pool'

        Returns:
            True if deleted, False if not found
        """
        # TODO: Implement
        pass

    def get_pool_status(self, name: str) -> Optional[dict]:
        """
        Get pool utilization status.

        TODO: Return dict with:
        - name: Pool name
        - slots: Total slots
        - running: Running slots
        - queued: Queued slots
        - open: Available slots
        - utilization: Percentage used

        Returns:
            Pool status dict or None
        """
        # TODO: Implement
        pass

    def list_pools(self) -> list:
        """
        List all pools with status.

        TODO: Implement pool listing.

        Returns:
            List of pool status dicts
        """
        # TODO: Implement
        pass

    def initialize_default_pools(self) -> int:
        """
        Create default pools if they don't exist.

        TODO: Create all DEFAULT_POOLS.

        Returns:
            Number of pools created
        """
        # TODO: Implement
        pass


# =============================================================================
# PART 2: POOL-AWARE DAG (Template)
# =============================================================================


"""
TODO: Create a DAG that demonstrates pool usage.

Requirements:
1. Use TaskFlow API
2. Create tasks using different pools:
   - extract_from_db: uses database_connections pool
   - call_external_api: uses api_rate_limit pool
   - heavy_computation: uses high_memory pool
3. Each task should use appropriate pool_slots
"""

# from airflow.sdk import dag, task
# from datetime import datetime
#
# @dag(
#     dag_id="pool_aware_etl",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
# )
# def pool_aware_etl():
#     # TODO: Implement tasks with pools
#     pass


# =============================================================================
# PART 3: DYNAMIC POOL SIZING
# =============================================================================


class DynamicPoolManager(PoolManager):
    """
    Dynamic pool sizing based on system load.

    Extends PoolManager with dynamic scaling capabilities.
    """

    # Base sizes for pools
    BASE_SIZES = {
        "database_connections": 10,
        "api_rate_limit": 5,
        "high_memory": 3,
    }

    # Load thresholds
    HIGH_LOAD_THRESHOLD = 0.8
    LOW_LOAD_THRESHOLD = 0.3

    def get_current_load(self) -> float:
        """
        Get current system load (0.0 to 1.0).

        TODO: Implement load calculation:
        - Could use pool utilization
        - Could use system metrics
        - For now, return mock value

        Returns:
            Load factor between 0.0 and 1.0
        """
        # TODO: Implement
        pass

    def adjust_pool_size(self, name: str, current_load: float = None) -> int:
        """
        Adjust pool size based on system load.

        TODO: Implement:
        - If load > HIGH_LOAD_THRESHOLD: reduce by 50%
        - If load < LOW_LOAD_THRESHOLD: increase by 50%
        - Otherwise: use base size

        Args:
            name: Pool name
            current_load: Load factor (auto-detect if None)

        Returns:
            New pool size
        """
        # TODO: Implement
        pass

    def scale_pool(self, name: str, factor: float) -> int:
        """
        Scale pool by a factor.

        TODO: Implement:
        1. Get current pool size
        2. Multiply by factor
        3. Ensure minimum of 1
        4. Update pool

        Args:
            name: Pool name
            factor: Scale factor (e.g., 0.5 for half, 2.0 for double)

        Returns:
            New pool size
        """
        # TODO: Implement
        pass

    def auto_scale_all(self) -> dict:
        """
        Auto-scale all managed pools based on load.

        TODO: Implement auto-scaling for all BASE_SIZES pools.

        Returns:
            Dict of {pool_name: new_size}
        """
        # TODO: Implement
        pass


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test pool configuration."""
    print("Pool Configuration - Exercise 14.1")
    print("=" * 50)

    manager = PoolManager()
    dynamic_manager = DynamicPoolManager()

    # Test 1: Pool creation
    print("\n1. Pool Creation:")
    # TODO: Test create_pool

    # Test 2: Pool status
    print("\n2. Pool Status:")
    # TODO: Test get_pool_status

    # Test 3: List pools
    print("\n3. List Pools:")
    # TODO: Test list_pools

    # Test 4: Dynamic sizing
    print("\n4. Dynamic Pool Sizing:")
    # TODO: Test adjust_pool_size

    print("\n" + "=" * 50)
    print("Test complete!")


if __name__ == "__main__":
    main()
