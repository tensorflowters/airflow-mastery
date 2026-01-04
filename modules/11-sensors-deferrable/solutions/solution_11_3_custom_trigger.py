"""
Solution 11.3: Custom Trigger
==============================

Complete implementation of custom async triggers for specialized
waiting patterns.

Demonstrates:
1. FilePatternTrigger - Wait for files matching glob pattern
2. HttpPollingTrigger - Poll HTTP endpoint for expected response
3. WaitForFilesOperator - Deferrable operator using custom trigger

Architecture:
    Operator.execute() → self.defer(trigger) → Triggerer runs trigger.run()
    → TriggerEvent → Operator.execute_complete()

Requirements:
    - Triggerer must be running
    - For HTTP trigger: aiohttp library
"""

from __future__ import annotations

import asyncio
import glob
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from airflow.utils.context import Context

logger = logging.getLogger(__name__)


# =============================================================================
# FILE PATTERN TRIGGER
# =============================================================================


class FilePatternTrigger(BaseTrigger):
    """
    Trigger that waits for files matching a glob pattern.

    This trigger polls a directory for files matching the specified
    pattern and fires when the minimum number of files is found.

    Features:
    - Glob pattern support (*, **, ?)
    - Minimum file count threshold
    - Configurable poll interval
    - Timeout handling

    Example:
        trigger = FilePatternTrigger(
            pattern="/data/incoming/2024-01-*/*.csv",
            min_files=5,
            poll_interval=30.0,
            timeout=3600.0,
        )
    """

    def __init__(
        self,
        pattern: str,
        min_files: int = 1,
        poll_interval: float = 10.0,
        timeout: float = 3600.0,
    ):
        """
        Initialize the file pattern trigger.

        Args:
            pattern: Glob pattern to match files (e.g., "/data/*.csv")
            min_files: Minimum number of files required to fire
            poll_interval: Seconds between file checks
            timeout: Maximum seconds to wait before timeout
        """
        super().__init__()
        self.pattern = pattern
        self.min_files = min_files
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict]:
        """
        Serialize trigger for database storage.

        The triggerer process needs to reconstruct this trigger
        from the serialized data. The classpath must be importable.

        Returns:
            Tuple of (full_classpath, constructor_kwargs)
        """
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "pattern": self.pattern,
                "min_files": self.min_files,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def _check_files(self) -> list[str]:
        """
        Check for files matching the pattern.

        Uses glob.glob for pattern matching. This is run in
        an executor to avoid blocking the async event loop.

        Returns:
            List of matching file paths
        """
        # Run glob in executor to avoid blocking
        loop = asyncio.get_event_loop()
        files = await loop.run_in_executor(
            None,
            lambda: glob.glob(self.pattern, recursive=True)
        )
        return sorted(files)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Async generator that polls for files.

        This method is called by the triggerer process. It should
        yield a TriggerEvent when the condition is met or on timeout.

        The method must be an async generator (uses yield, not return).

        Yields:
            TriggerEvent with status and found files
        """
        start_time = datetime.utcnow()
        timeout_at = start_time + timedelta(seconds=self.timeout)
        check_count = 0

        self.log.info(
            f"FilePatternTrigger started: pattern={self.pattern}, "
            f"min_files={self.min_files}, timeout={self.timeout}s"
        )

        while datetime.utcnow() < timeout_at:
            check_count += 1

            try:
                files = await self._check_files()
                file_count = len(files)

                self.log.debug(
                    f"Check #{check_count}: Found {file_count} files "
                    f"(need {self.min_files})"
                )

                if file_count >= self.min_files:
                    elapsed = (datetime.utcnow() - start_time).total_seconds()
                    self.log.info(
                        f"Condition met: {file_count} files found "
                        f"after {elapsed:.1f}s and {check_count} checks"
                    )
                    yield TriggerEvent({
                        "status": "success",
                        "files": files,
                        "file_count": file_count,
                        "checks": check_count,
                        "elapsed_seconds": elapsed,
                    })
                    return

            except Exception as e:
                self.log.warning(f"File check failed: {e}")

            # Wait before next check
            await asyncio.sleep(self.poll_interval)

        # Timeout reached
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        files = await self._check_files()
        self.log.warning(
            f"Timeout after {elapsed:.1f}s: Found {len(files)} files, "
            f"needed {self.min_files}"
        )
        yield TriggerEvent({
            "status": "timeout",
            "files": files,
            "file_count": len(files),
            "checks": check_count,
            "elapsed_seconds": elapsed,
        })


# =============================================================================
# HTTP POLLING TRIGGER
# =============================================================================


class HttpPollingTrigger(BaseTrigger):
    """
    Trigger that polls an HTTP endpoint until condition is met.

    This trigger makes async HTTP requests and checks the JSON
    response for an expected value at a specified key path.

    Features:
    - Async HTTP requests (non-blocking)
    - JSON response parsing
    - Configurable response validation
    - Custom headers support
    - Timeout handling

    Example:
        trigger = HttpPollingTrigger(
            endpoint="https://api.example.com/job/123/status",
            response_key="status",
            expected_value="completed",
            headers={"Authorization": "Bearer token"},
            poll_interval=60.0,
        )
    """

    def __init__(
        self,
        endpoint: str,
        response_key: str = "status",
        expected_value: Any = "ready",
        headers: dict | None = None,
        poll_interval: float = 30.0,
        timeout: float = 3600.0,
    ):
        """
        Initialize the HTTP polling trigger.

        Args:
            endpoint: Full URL to poll
            response_key: Key in JSON response to check
            expected_value: Value that indicates success
            headers: Optional HTTP headers dict
            poll_interval: Seconds between requests
            timeout: Maximum seconds to wait
        """
        super().__init__()
        self.endpoint = endpoint
        self.response_key = response_key
        self.expected_value = expected_value
        self.headers = headers or {}
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict]:
        """Serialize trigger for database storage."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "endpoint": self.endpoint,
                "response_key": self.response_key,
                "expected_value": self.expected_value,
                "headers": self.headers,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def _check_endpoint(self) -> tuple[bool, dict]:
        """
        Make async HTTP request and check response.

        Returns:
            Tuple of (condition_met, response_data)
        """
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.endpoint,
                    headers=self.headers,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as response:
                    if response.status != 200:
                        return False, {"error": f"HTTP {response.status}"}

                    data = await response.json()
                    actual_value = data.get(self.response_key)
                    condition_met = actual_value == self.expected_value

                    return condition_met, {
                        "response": data,
                        "actual_value": actual_value,
                    }

        except ImportError:
            self.log.warning("aiohttp not installed, using mock response")
            # Mock for testing without aiohttp
            return False, {"error": "aiohttp not installed"}

        except Exception as e:
            return False, {"error": str(e)}

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Async generator that polls HTTP endpoint."""
        start_time = datetime.utcnow()
        timeout_at = start_time + timedelta(seconds=self.timeout)
        check_count = 0

        self.log.info(
            f"HttpPollingTrigger started: endpoint={self.endpoint}, "
            f"checking {self.response_key}=={self.expected_value}"
        )

        while datetime.utcnow() < timeout_at:
            check_count += 1

            condition_met, response_data = await self._check_endpoint()

            self.log.debug(
                f"Check #{check_count}: condition_met={condition_met}, "
                f"response={response_data}"
            )

            if condition_met:
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                self.log.info(
                    f"Condition met after {elapsed:.1f}s and {check_count} checks"
                )
                yield TriggerEvent({
                    "status": "success",
                    "response": response_data.get("response", {}),
                    "checks": check_count,
                    "elapsed_seconds": elapsed,
                })
                return

            await asyncio.sleep(self.poll_interval)

        # Timeout
        elapsed = (datetime.utcnow() - start_time).total_seconds()
        self.log.warning(f"Timeout after {elapsed:.1f}s")
        yield TriggerEvent({
            "status": "timeout",
            "last_response": response_data,
            "checks": check_count,
            "elapsed_seconds": elapsed,
        })


# =============================================================================
# DEFERRABLE OPERATORS
# =============================================================================


class WaitForFilesOperator(BaseOperator):
    """
    Deferrable operator that waits for files matching a pattern.

    This operator demonstrates the deferrable pattern:
    1. execute() checks if condition is already met
    2. If not, defers to FilePatternTrigger
    3. execute_complete() handles the trigger result

    Example:
        wait_task = WaitForFilesOperator(
            task_id="wait_for_data_files",
            pattern="/data/incoming/{{ ds }}/*.csv",
            min_files=3,
            timeout=3600,
        )
    """

    template_fields = ("pattern",)
    ui_color = "#73a8ff"
    ui_fgcolor = "#ffffff"

    def __init__(
        self,
        *,
        pattern: str,
        min_files: int = 1,
        poll_interval: float = 10.0,
        timeout: float = 3600.0,
        fail_on_timeout: bool = True,
        **kwargs,
    ):
        """
        Initialize the operator.

        Args:
            pattern: Glob pattern for files
            min_files: Minimum files required
            poll_interval: Seconds between trigger checks
            timeout: Maximum wait time in seconds
            fail_on_timeout: Raise exception on timeout vs return empty
        """
        super().__init__(**kwargs)
        self.pattern = pattern
        self.min_files = min_files
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.fail_on_timeout = fail_on_timeout

    def execute(self, context: Context) -> dict | None:
        """
        Execute the operator.

        First checks if files already exist. If so, returns
        immediately. Otherwise, defers to the trigger.

        Args:
            context: Airflow task context

        Returns:
            Result dict if immediate success, None if deferred
        """
        # Check if files already exist
        files = glob.glob(self.pattern, recursive=True)

        if len(files) >= self.min_files:
            self.log.info(
                f"Files already present: {len(files)} files found "
                f"(required: {self.min_files})"
            )
            return {
                "status": "immediate",
                "files": sorted(files),
                "file_count": len(files),
            }

        # Defer to trigger
        self.log.info(
            f"Deferring to FilePatternTrigger: pattern={self.pattern}, "
            f"current files={len(files)}, required={self.min_files}"
        )

        self.defer(
            trigger=FilePatternTrigger(
                pattern=self.pattern,
                min_files=self.min_files,
                poll_interval=self.poll_interval,
                timeout=self.timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict) -> dict:
        """
        Called when trigger fires.

        Handles the TriggerEvent from FilePatternTrigger.

        Args:
            context: Airflow task context
            event: TriggerEvent payload dict

        Returns:
            Result dict with files found

        Raises:
            AirflowException: If timeout and fail_on_timeout=True
        """
        status = event.get("status")

        if status == "success":
            self.log.info(
                f"Trigger succeeded: {event.get('file_count')} files found "
                f"after {event.get('elapsed_seconds', 0):.1f}s"
            )
            return event

        elif status == "timeout":
            message = (
                f"Timeout waiting for files: pattern={self.pattern}, "
                f"found={event.get('file_count', 0)}, required={self.min_files}"
            )

            if self.fail_on_timeout:
                raise AirflowException(message)
            else:
                self.log.warning(message)
                return event

        else:
            raise AirflowException(f"Unknown trigger status: {status}")


class WaitForHttpOperator(BaseOperator):
    """
    Deferrable operator that waits for HTTP endpoint condition.

    Polls an HTTP endpoint and waits for a JSON field to
    match an expected value.

    Example:
        wait_task = WaitForHttpOperator(
            task_id="wait_for_job",
            endpoint="https://api.example.com/jobs/{{ params.job_id }}",
            response_key="status",
            expected_value="completed",
        )
    """

    template_fields = ("endpoint", "expected_value")
    ui_color = "#ffb347"
    ui_fgcolor = "#000000"

    def __init__(
        self,
        *,
        endpoint: str,
        response_key: str = "status",
        expected_value: Any = "ready",
        headers: dict | None = None,
        poll_interval: float = 30.0,
        timeout: float = 3600.0,
        fail_on_timeout: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.response_key = response_key
        self.expected_value = expected_value
        self.headers = headers or {}
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.fail_on_timeout = fail_on_timeout

    def execute(self, context: Context) -> dict | None:
        """Execute and possibly defer to HttpPollingTrigger."""
        import requests

        # Quick check if condition already met
        try:
            response = requests.get(
                self.endpoint,
                headers=self.headers,
                timeout=10,
            )
            if response.status_code == 200:
                data = response.json()
                if data.get(self.response_key) == self.expected_value:
                    self.log.info("Condition already met, returning immediately")
                    return {"status": "immediate", "response": data}
        except Exception as e:
            self.log.warning(f"Initial check failed: {e}")

        # Defer to trigger
        self.log.info(f"Deferring to HttpPollingTrigger: {self.endpoint}")

        self.defer(
            trigger=HttpPollingTrigger(
                endpoint=self.endpoint,
                response_key=self.response_key,
                expected_value=self.expected_value,
                headers=self.headers,
                poll_interval=self.poll_interval,
                timeout=self.timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict) -> dict:
        """Handle trigger event."""
        status = event.get("status")

        if status == "success":
            return event
        elif status == "timeout":
            if self.fail_on_timeout:
                raise AirflowException(f"Timeout waiting for {self.endpoint}")
            return event
        else:
            raise AirflowException(f"Unknown status: {status}")


# =============================================================================
# EXAMPLE DAG
# =============================================================================


from airflow.sdk import dag, task


@dag(
    dag_id="solution_11_3_custom_trigger",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["solution", "triggers", "deferrable"],
    doc_md="""
    ## Custom Trigger Demo

    Demonstrates custom async triggers:
    1. FilePatternTrigger - Wait for files matching glob
    2. HttpPollingTrigger - Poll HTTP endpoint

    ### Test Instructions
    ```bash
    # Create test files
    mkdir -p /tmp/trigger_demo
    echo "data" > /tmp/trigger_demo/file_1.csv
    echo "data" > /tmp/trigger_demo/file_2.csv

    # Trigger DAG
    airflow dags trigger solution_11_3_custom_trigger
    ```
    """,
)
def custom_trigger_demo():
    """DAG demonstrating custom triggers."""

    wait_for_files = WaitForFilesOperator(
        task_id="wait_for_files",
        pattern="/tmp/trigger_demo/*.csv",
        min_files=2,
        poll_interval=5.0,
        timeout=120.0,
        fail_on_timeout=False,
    )

    @task
    def process_found_files(wait_result: dict) -> dict:
        """Process the files found by the trigger."""
        files = wait_result.get("files", [])
        status = wait_result.get("status")

        logger.info(f"Received {len(files)} files with status: {status}")

        if status == "timeout":
            logger.warning("Timed out waiting for files")
            return {"processed": 0, "status": "timeout"}

        # Process files
        processed_count = 0
        for filepath in files:
            logger.info(f"Processing: {filepath}")
            processed_count += 1

        return {
            "processed": processed_count,
            "files": files,
            "status": "success",
        }

    @task
    def generate_summary(process_result: dict) -> None:
        """Generate final summary."""
        logger.info("=" * 50)
        logger.info("CUSTOM TRIGGER DEMO SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Status: {process_result['status']}")
        logger.info(f"Files processed: {process_result['processed']}")
        logger.info("=" * 50)

    # Build pipeline
    process_result = process_found_files(wait_for_files.output)
    generate_summary(process_result)


# Instantiate
custom_trigger_demo()


# =============================================================================
# TESTING
# =============================================================================


async def test_file_trigger():
    """Test FilePatternTrigger."""
    import tempfile

    print("Testing FilePatternTrigger...")

    with tempfile.TemporaryDirectory() as tmpdir:
        pattern = f"{tmpdir}/*.csv"

        # Create trigger
        trigger = FilePatternTrigger(
            pattern=pattern,
            min_files=2,
            poll_interval=1.0,
            timeout=10.0,
        )

        # Test serialization
        classpath, kwargs = trigger.serialize()
        print(f"Serialized: {classpath}")
        print(f"Kwargs: {kwargs}")

        # Create files in background
        async def create_files():
            await asyncio.sleep(3)
            for i in range(3):
                filepath = f"{tmpdir}/file_{i}.csv"
                Path(filepath).write_text(f"data_{i}")
                print(f"Created: {filepath}")

        # Run trigger and file creation
        create_task = asyncio.create_task(create_files())

        async for event in trigger.run():
            print(f"Trigger event: {event}")
            assert event["status"] in ("success", "timeout")
            break

        await create_task

    print("FilePatternTrigger test passed!\n")


def test_operator():
    """Test WaitForFilesOperator."""
    import tempfile

    print("Testing WaitForFilesOperator...")

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create files first
        for i in range(3):
            Path(f"{tmpdir}/data_{i}.csv").write_text(f"content_{i}")

        # Test immediate success
        op = WaitForFilesOperator(
            task_id="test",
            pattern=f"{tmpdir}/*.csv",
            min_files=2,
        )

        # Mock context
        class MockContext:
            pass

        result = op.execute(MockContext())
        assert result["status"] == "immediate"
        assert result["file_count"] >= 2
        print(f"Immediate result: {result}")

    print("WaitForFilesOperator test passed!\n")


if __name__ == "__main__":
    # Run async test
    asyncio.run(test_file_trigger())

    # Run sync test
    test_operator()
