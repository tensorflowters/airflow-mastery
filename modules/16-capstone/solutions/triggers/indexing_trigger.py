"""
Custom Indexing Trigger
=======================

Production-ready trigger for async vector store indexing operations with:
- Non-blocking index status polling
- Progress tracking and reporting
- Timeout handling with graceful degradation
- Event-based completion notification

Modules Applied: 11, 15
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import Any

from airflow.triggers.base import BaseTrigger, TriggerEvent

logger = logging.getLogger(__name__)


class IndexingCompletionTrigger(BaseTrigger):
    """
    Async trigger that waits for vector indexing to complete.

    This trigger efficiently polls the vector store to check indexing
    progress, releasing the Airflow worker slot during the wait period.
    Ideal for long-running indexing operations.

    Args:
        collection_name: Name of the vector collection being indexed.
        expected_count: Number of vectors expected after indexing.
        poll_interval: Seconds between status checks.
        timeout: Maximum wait time in seconds.
        progress_callback_interval: Seconds between progress log messages.

    Example:
        ```python
        # In a deferrable operator:
        def execute(self, context):
            self.defer(
                trigger=IndexingCompletionTrigger(
                    collection_name="documents",
                    expected_count=500,
                    poll_interval=30,
                    timeout=1800,
                ),
                method_name="execute_complete",
            )
        ```
    """

    def __init__(
        self,
        collection_name: str,
        expected_count: int,
        poll_interval: int = 30,
        timeout: int = 1800,
        progress_callback_interval: int = 300,
    ) -> None:
        """Initialize the indexing completion trigger.

        Args:
            collection_name: Name of the vector collection to monitor.
            expected_count: Number of vectors expected after indexing completes.
            poll_interval: Seconds between status checks (default: 30).
            timeout: Maximum wait time in seconds (default: 1800).
            progress_callback_interval: Seconds between progress logs (default: 300).
        """
        super().__init__()
        self.collection_name = collection_name
        self.expected_count = expected_count
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.progress_callback_interval = progress_callback_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serialize trigger for database storage.

        Returns:
            Tuple of (module.class path, parameter dict).
        """
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "collection_name": self.collection_name,
                "expected_count": self.expected_count,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
                "progress_callback_interval": self.progress_callback_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Poll vector store until indexing completes or timeout.

        Yields:
            TriggerEvent with indexing completion status.
        """
        start_time = datetime.utcnow()
        timeout_at = start_time + timedelta(seconds=self.timeout)
        last_progress_log = start_time

        logger.info(
            f"Starting indexing monitor for '{self.collection_name}': "
            f"expecting {self.expected_count} vectors, "
            f"timeout={self.timeout}s"
        )

        while datetime.utcnow() < timeout_at:
            try:
                status = await self._check_index_status()
                current_count = status.get("indexed_count", 0)
                progress = (current_count / self.expected_count * 100) if self.expected_count > 0 else 100

                # Log progress periodically
                if (datetime.utcnow() - last_progress_log).total_seconds() >= self.progress_callback_interval:
                    logger.info(
                        f"Indexing progress for '{self.collection_name}': "
                        f"{current_count}/{self.expected_count} ({progress:.1f}%)"
                    )
                    last_progress_log = datetime.utcnow()

                # Check completion
                if current_count >= self.expected_count:
                    duration = (datetime.utcnow() - start_time).total_seconds()
                    logger.info(
                        f"Indexing complete for '{self.collection_name}': "
                        f"{current_count} vectors indexed in {duration:.1f}s"
                    )

                    yield TriggerEvent(
                        {
                            "status": "success",
                            "collection_name": self.collection_name,
                            "indexed_count": current_count,
                            "expected_count": self.expected_count,
                            "duration_seconds": duration,
                            "index_status": status.get("index_status", "ready"),
                        }
                    )
                    return

            except Exception as e:
                logger.warning(f"Error checking index status: {e}")

            await asyncio.sleep(self.poll_interval)

        # Timeout reached
        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.error(f"Indexing timeout for '{self.collection_name}' after {duration:.1f}s")

        yield TriggerEvent(
            {
                "status": "timeout",
                "collection_name": self.collection_name,
                "message": f"Indexing did not complete within {self.timeout} seconds",
                "duration_seconds": duration,
            }
        )

    async def _check_index_status(self) -> dict[str, Any]:
        """
        Check vector store index status.

        In production, this would make an async HTTP call to the vector store API.

        Returns:
            Dictionary with index status information.
        """
        # Simulated async check for demonstration
        # In production: use aiohttp or httpx to query vector store
        await asyncio.sleep(0.1)  # Simulate network latency

        return {
            "indexed_count": 1250,
            "index_status": "ready",
            "last_updated": datetime.utcnow().isoformat(),
            "storage_bytes": 48_500_000,
        }


class BatchIndexingTrigger(BaseTrigger):
    """
    Async trigger for monitoring batch indexing operations.

    Tracks multiple batches being indexed and reports overall progress.
    Useful when indexing large document sets in parallel batches.

    Args:
        collection_name: Name of the vector collection.
        batch_ids: List of batch identifiers to monitor.
        poll_interval: Seconds between status checks.
        timeout: Maximum wait time in seconds.
    """

    def __init__(
        self,
        collection_name: str,
        batch_ids: list[str],
        poll_interval: int = 30,
        timeout: int = 3600,
    ) -> None:
        """Initialize the batch indexing trigger.

        Args:
            collection_name: Name of the vector collection to monitor.
            batch_ids: List of batch identifiers to track.
            poll_interval: Seconds between status checks (default: 30).
            timeout: Maximum wait time in seconds (default: 3600).
        """
        super().__init__()
        self.collection_name = collection_name
        self.batch_ids = batch_ids
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger for database storage."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "collection_name": self.collection_name,
                "batch_ids": self.batch_ids,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Monitor all batches until completion or timeout.

        Yields:
            TriggerEvent with batch indexing status.
        """
        start_time = datetime.utcnow()
        timeout_at = start_time + timedelta(seconds=self.timeout)
        completed_batches: set[str] = set()

        logger.info(f"Monitoring {len(self.batch_ids)} batches for '{self.collection_name}'")

        while datetime.utcnow() < timeout_at:
            try:
                batch_statuses = await self._check_batch_statuses()

                for batch_id, status in batch_statuses.items():
                    if status["complete"] and batch_id not in completed_batches:
                        completed_batches.add(batch_id)
                        logger.info(f"Batch '{batch_id}' indexing complete")

                # Check if all batches are done
                if len(completed_batches) >= len(self.batch_ids):
                    duration = (datetime.utcnow() - start_time).total_seconds()
                    logger.info(f"All {len(self.batch_ids)} batches indexed in {duration:.1f}s")

                    yield TriggerEvent(
                        {
                            "status": "success",
                            "collection_name": self.collection_name,
                            "completed_batches": list(completed_batches),
                            "total_batches": len(self.batch_ids),
                            "duration_seconds": duration,
                        }
                    )
                    return

                # Log progress
                logger.info(f"Batch progress: {len(completed_batches)}/{len(self.batch_ids)} complete")

            except Exception as e:
                logger.warning(f"Error checking batch status: {e}")

            await asyncio.sleep(self.poll_interval)

        # Timeout
        duration = (datetime.utcnow() - start_time).total_seconds()
        failed_batches = set(self.batch_ids) - completed_batches

        yield TriggerEvent(
            {
                "status": "timeout",
                "collection_name": self.collection_name,
                "completed_batches": list(completed_batches),
                "failed_batches": list(failed_batches),
                "duration_seconds": duration,
            }
        )

    async def _check_batch_statuses(self) -> dict[str, dict]:
        """Check status of all batches."""
        # Simulated batch status check
        await asyncio.sleep(0.1)

        return {batch_id: {"complete": True, "vector_count": 100} for batch_id in self.batch_ids}


class IndexHealthTrigger(BaseTrigger):
    """
    Async trigger for monitoring vector store index health.

    Waits until index health metrics meet specified thresholds.
    Useful for ensuring index is ready before query operations.

    Args:
        collection_name: Name of the vector collection.
        max_latency_ms: Maximum acceptable query latency.
        min_availability: Minimum availability percentage (0-1).
        poll_interval: Seconds between health checks.
        timeout: Maximum wait time in seconds.
    """

    def __init__(
        self,
        collection_name: str,
        max_latency_ms: float = 50.0,
        min_availability: float = 0.99,
        poll_interval: int = 15,
        timeout: int = 300,
    ) -> None:
        """Initialize the index health trigger.

        Args:
            collection_name: Name of the vector collection to monitor.
            max_latency_ms: Maximum acceptable query latency in ms (default: 50).
            min_availability: Minimum availability percentage 0-1 (default: 0.99).
            poll_interval: Seconds between health checks (default: 15).
            timeout: Maximum wait time in seconds (default: 300).
        """
        super().__init__()
        self.collection_name = collection_name
        self.max_latency_ms = max_latency_ms
        self.min_availability = min_availability
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize trigger for database storage."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "collection_name": self.collection_name,
                "max_latency_ms": self.max_latency_ms,
                "min_availability": self.min_availability,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Monitor index health until metrics are acceptable.

        Yields:
            TriggerEvent with health status.
        """
        start_time = datetime.utcnow()
        timeout_at = start_time + timedelta(seconds=self.timeout)

        while datetime.utcnow() < timeout_at:
            try:
                health = await self._check_health()

                latency_ok = health["latency_ms"] <= self.max_latency_ms
                availability_ok = health["availability"] >= self.min_availability

                if latency_ok and availability_ok:
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "collection_name": self.collection_name,
                            "health_metrics": health,
                            "duration_seconds": (datetime.utcnow() - start_time).total_seconds(),
                        }
                    )
                    return

                logger.info(
                    f"Health check: latency={health['latency_ms']:.1f}ms "
                    f"(max={self.max_latency_ms}), "
                    f"availability={health['availability']:.2%} "
                    f"(min={self.min_availability:.2%})"
                )

            except Exception as e:
                logger.warning(f"Health check error: {e}")

            await asyncio.sleep(self.poll_interval)

        yield TriggerEvent(
            {
                "status": "timeout",
                "collection_name": self.collection_name,
                "message": "Health thresholds not met within timeout",
            }
        )

    async def _check_health(self) -> dict[str, Any]:
        """Check index health metrics."""
        await asyncio.sleep(0.05)

        return {
            "latency_ms": 12.5,
            "availability": 0.999,
            "storage_used_percent": 45.0,
            "query_success_rate": 0.998,
        }
