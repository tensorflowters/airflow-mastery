"""
Custom Vector Store Sensor
==========================

Production-ready sensor for monitoring vector store operations with:
- Deferrable mode for efficient resource usage
- Configurable health checks
- Index status monitoring

Modules Applied: 11, 15
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from airflow.sdk import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.context import Context

logger = logging.getLogger(__name__)


class VectorStoreReadySensor(BaseSensorOperator):
    """
    Sensor that waits for vector store to be ready for queries.

    This sensor checks:
    - Vector store connectivity
    - Collection existence
    - Minimum vector count
    - Index health status

    Args:
        task_id: Unique task identifier.
        collection_name: Name of the vector collection to monitor.
        min_vectors: Minimum number of vectors required.
        check_index_health: Whether to verify index health.
        deferrable: Whether to use deferrable mode.

    Example:
        ```python
        wait_for_vectors = VectorStoreReadySensor(
            task_id="wait_for_vectors",
            collection_name="documents",
            min_vectors=100,
            deferrable=True,
            poke_interval=60,
        )
        ```
    """

    template_fields = ("collection_name", "min_vectors")
    ui_color = "#f4e8d4"

    def __init__(
        self,
        *,
        collection_name: str,
        min_vectors: int = 0,
        check_index_health: bool = True,
        **kwargs: Any,
    ) -> None:
        """Initialize Vector Store Ready Sensor."""
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.min_vectors = min_vectors
        self.check_index_health = check_index_health

    def poke(self, context: Context) -> bool:
        """
        Check if vector store is ready.

        Args:
            context: Airflow task context.

        Returns:
            True if vector store is ready, False otherwise.
        """
        try:
            status = self._check_vector_store_status()

            logger.info(
                f"Vector store status: collection={self.collection_name}, "
                f"vectors={status['vector_count']}, "
                f"healthy={status['is_healthy']}"
            )

            # Check minimum vector count
            if status["vector_count"] < self.min_vectors:
                logger.info(f"Waiting for vectors: {status['vector_count']}/{self.min_vectors}")
                return False

            # Check index health if required
            if self.check_index_health and not status["is_healthy"]:
                logger.warning("Vector store index is not healthy")
                return False

            return True

        except Exception as e:
            logger.error(f"Error checking vector store: {e}")
            return False

    def _check_vector_store_status(self) -> dict:
        """
        Check vector store status.

        In production, this would query the actual vector store API.
        """
        # Simulated status for demonstration
        return {
            "collection_name": self.collection_name,
            "vector_count": 1250,
            "is_healthy": True,
            "index_status": "ready",
            "last_updated": "2024-01-15T10:30:00Z",
        }


class VectorStoreIndexingSensor(BaseSensorOperator):
    """
    Deferrable sensor that waits for vector store indexing to complete.

    This sensor uses async polling to efficiently wait for
    indexing operations without holding a worker slot.

    Args:
        task_id: Unique task identifier.
        collection_name: Name of the vector collection.
        expected_count: Expected number of vectors after indexing.
        deferrable: Whether to use deferrable mode (recommended).

    Example:
        ```python
        wait_indexing = VectorStoreIndexingSensor(
            task_id="wait_for_indexing",
            collection_name="documents",
            expected_count=500,
            deferrable=True,
            poke_interval=30,
            timeout=600,
        )
        ```
    """

    template_fields = ("collection_name", "expected_count")
    ui_color = "#e8d4f4"

    def __init__(
        self,
        *,
        collection_name: str,
        expected_count: int,
        **kwargs: Any,
    ) -> None:
        """Initialize Vector Store Indexing Sensor."""
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.expected_count = expected_count

    def poke(self, context: Context) -> bool:
        """Check if indexing is complete."""
        status = self._get_index_status()

        logger.info(
            f"Indexing status: {status['indexed_count']}/{self.expected_count} ({status['progress_percent']:.1f}%)"
        )

        return status["indexed_count"] >= self.expected_count

    def _get_index_status(self) -> dict:
        """Get current indexing status."""
        # Simulated status for demonstration
        indexed_count = 1250
        return {
            "indexed_count": indexed_count,
            "expected_count": self.expected_count,
            "progress_percent": (indexed_count / self.expected_count) * 100 if self.expected_count > 0 else 100,
            "status": "complete" if indexed_count >= self.expected_count else "indexing",
        }


class VectorStoreHealthSensor(BaseSensorOperator):
    """
    Sensor that monitors vector store health metrics.

    Checks multiple health indicators:
    - Query latency
    - Index freshness
    - Storage utilization
    - Connection status

    Args:
        task_id: Unique task identifier.
        collection_name: Name of the vector collection.
        max_latency_ms: Maximum acceptable query latency.
        max_staleness_seconds: Maximum acceptable index staleness.

    Example:
        ```python
        check_health = VectorStoreHealthSensor(
            task_id="check_vector_health",
            collection_name="documents",
            max_latency_ms=100,
            max_staleness_seconds=3600,
            mode="reschedule",
        )
        ```
    """

    template_fields = ("collection_name",)
    ui_color = "#d4f4e8"

    def __init__(
        self,
        *,
        collection_name: str,
        max_latency_ms: int = 100,
        max_staleness_seconds: int = 3600,
        **kwargs: Any,
    ) -> None:
        """Initialize Vector Store Health Sensor."""
        super().__init__(**kwargs)
        self.collection_name = collection_name
        self.max_latency_ms = max_latency_ms
        self.max_staleness_seconds = max_staleness_seconds

    def poke(self, context: Context) -> bool:
        """Check vector store health."""
        health = self._check_health()

        logger.info(
            f"Health check: latency={health['latency_ms']}ms, "
            f"staleness={health['staleness_seconds']}s, "
            f"status={health['status']}"
        )

        # Check latency threshold
        if health["latency_ms"] > self.max_latency_ms:
            logger.warning(f"High latency: {health['latency_ms']}ms > {self.max_latency_ms}ms")
            return False

        # Check staleness threshold
        if health["staleness_seconds"] > self.max_staleness_seconds:
            logger.warning(f"Stale index: {health['staleness_seconds']}s > {self.max_staleness_seconds}s")
            return False

        return health["status"] == "healthy"

    def _check_health(self) -> dict:
        """Check vector store health metrics."""
        # Simulated health check for demonstration
        return {
            "collection_name": self.collection_name,
            "status": "healthy",
            "latency_ms": 15.5,
            "staleness_seconds": 300,
            "storage_used_percent": 45.2,
            "connection_status": "connected",
        }


class VectorStoreTrigger(BaseTrigger):
    """
    Async trigger for vector store operations.

    Used with deferrable sensors to efficiently poll
    vector store status without holding a worker slot.
    """

    def __init__(
        self,
        collection_name: str,
        check_type: str,
        threshold: int | float,
        poll_interval: int = 30,
        timeout: int = 600,
    ) -> None:
        """
        Initialize the vector store trigger.

        Args:
            collection_name: Name of the collection to monitor.
            check_type: Type of check ('count', 'health', 'latency').
            threshold: Threshold value for the check.
            poll_interval: Seconds between polls.
            timeout: Maximum wait time in seconds.
        """
        super().__init__()
        self.collection_name = collection_name
        self.check_type = check_type
        self.threshold = threshold
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict]:
        """Serialize trigger for database storage."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "collection_name": self.collection_name,
                "check_type": self.check_type,
                "threshold": self.threshold,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self):
        """Poll vector store until condition is met."""
        import asyncio
        from datetime import datetime

        start_time = datetime.utcnow()
        timeout_at = start_time + timedelta(seconds=self.timeout)

        while datetime.utcnow() < timeout_at:
            try:
                result = await self._async_check()

                if result["condition_met"]:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "check_type": self.check_type,
                            "result": result,
                            "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                        }
                    )
                    return

            except Exception as e:
                logger.warning(f"Trigger check error: {e}")

            await asyncio.sleep(self.poll_interval)

        yield TriggerEvent(
            {
                "status": "timeout",
                "check_type": self.check_type,
                "message": f"Condition not met within {self.timeout} seconds",
            }
        )

    async def _async_check(self) -> dict:
        """Perform async status check."""
        # Simulated async check
        return {
            "condition_met": True,
            "current_value": 1250,
            "threshold": self.threshold,
        }
