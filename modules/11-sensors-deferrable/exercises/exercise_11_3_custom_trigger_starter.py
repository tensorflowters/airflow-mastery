"""
Exercise 11.3: Custom Trigger (Starter)
========================================

Build a custom async trigger for specialized waiting patterns.

Learning Goals:
1. Understand trigger architecture
2. Implement serialize() for database storage
3. Create async run() generator
4. Build deferrable operator using trigger

TODO: Complete the TODOs to implement the custom trigger.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, AsyncIterator

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from airflow.utils.context import Context


# =============================================================================
# PART 1: FILE PATTERN TRIGGER
# =============================================================================


class FilePatternTrigger(BaseTrigger):
    """
    Trigger that waits for files matching a glob pattern.

    This trigger polls a directory for files matching a pattern
    and fires when the minimum number of files is found.

    Example:
        trigger = FilePatternTrigger(
            pattern="/data/incoming/*.csv",
            min_files=3,
            poll_interval=10.0,
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
        Initialize the trigger.

        Args:
            pattern: Glob pattern to match files
            min_files: Minimum number of files to find
            poll_interval: Seconds between checks
            timeout: Maximum seconds to wait
        """
        super().__init__()
        self.pattern = pattern
        self.min_files = min_files
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self) -> tuple[str, dict]:
        """
        Serialize trigger for database storage.

        TODO: Return a tuple of (classpath, kwargs)

        The classpath must be the full import path to this class.
        The kwargs must contain all parameters needed to reconstruct.

        Example:
            return (
                "mymodule.triggers.FilePatternTrigger",
                {"pattern": self.pattern, ...},
            )
        """
        # TODO: Implement serialization
        pass

    async def _check_files(self) -> list[str]:
        """
        Check for files matching the pattern.

        TODO: Implement async file checking using glob.

        Returns:
            List of matching file paths
        """
        # TODO: Implement file checking
        # Hint: Use glob.glob() or pathlib.Path.glob()
        pass

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Async generator that yields when condition is met.

        TODO: Implement the polling loop that:
        1. Checks for matching files
        2. Yields TriggerEvent if min_files found
        3. Sleeps for poll_interval
        4. Handles timeout

        Yields:
            TriggerEvent with status and found files
        """
        # TODO: Implement the async polling loop
        # start_time = datetime.utcnow()
        # timeout_at = start_time + timedelta(seconds=self.timeout)
        #
        # while datetime.utcnow() < timeout_at:
        #     files = await self._check_files()
        #     if len(files) >= self.min_files:
        #         yield TriggerEvent({"status": "success", "files": files})
        #         return
        #     await asyncio.sleep(self.poll_interval)
        #
        # yield TriggerEvent({"status": "timeout", "files": []})
        pass


# =============================================================================
# PART 2: HTTP POLLING TRIGGER (ADVANCED)
# =============================================================================


class HttpPollingTrigger(BaseTrigger):
    """
    Trigger that polls an HTTP endpoint until condition is met.

    This trigger makes async HTTP requests and checks the response
    for an expected value.

    Example:
        trigger = HttpPollingTrigger(
            endpoint="https://api.example.com/status",
            response_key="status",
            expected_value="ready",
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
        Initialize the trigger.

        Args:
            endpoint: HTTP endpoint URL
            response_key: JSON key to check in response
            expected_value: Value that indicates success
            headers: Optional HTTP headers
            poll_interval: Seconds between checks
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
        # TODO: Implement serialization
        pass

    async def _check_endpoint(self) -> tuple[bool, Any]:
        """
        Check the HTTP endpoint.

        TODO: Implement async HTTP request using aiohttp.

        Returns:
            Tuple of (condition_met, response_data)
        """
        # TODO: Implement HTTP check
        # Hint: Use aiohttp.ClientSession
        pass

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Async generator that polls endpoint."""
        # TODO: Implement polling loop
        pass


# =============================================================================
# PART 3: DEFERRABLE OPERATOR
# =============================================================================


class WaitForFilesOperator(BaseOperator):
    """
    Deferrable operator that waits for files matching a pattern.

    This operator uses FilePatternTrigger to defer waiting
    to the triggerer process.

    Example:
        wait_task = WaitForFilesOperator(
            task_id="wait_for_data",
            pattern="/data/incoming/*.csv",
            min_files=5,
        )
    """

    # Fields that support Jinja templating
    template_fields = ("pattern",)

    def __init__(
        self,
        *,
        pattern: str,
        min_files: int = 1,
        poll_interval: float = 10.0,
        timeout: float = 3600.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pattern = pattern
        self.min_files = min_files
        self.poll_interval = poll_interval
        self.timeout = timeout

    def execute(self, context: Context) -> dict | None:
        """
        Execute the operator.

        TODO: Implement execute to:
        1. Check if files already exist (immediate success)
        2. If not, defer to FilePatternTrigger

        Args:
            context: Airflow task context

        Returns:
            Result dict if immediate success, None if deferred
        """
        # TODO: Implement execute
        # import glob
        # files = glob.glob(self.pattern)
        # if len(files) >= self.min_files:
        #     return {"status": "immediate", "files": files}
        #
        # self.defer(
        #     trigger=FilePatternTrigger(
        #         pattern=self.pattern,
        #         min_files=self.min_files,
        #         poll_interval=self.poll_interval,
        #         timeout=self.timeout,
        #     ),
        #     method_name="execute_complete",
        # )
        pass

    def execute_complete(self, context: Context, event: dict) -> dict:
        """
        Called when trigger fires.

        TODO: Implement to handle trigger event:
        - If status is "success", return the result
        - If status is "timeout", raise exception

        Args:
            context: Airflow task context
            event: TriggerEvent payload

        Returns:
            Result dict with found files
        """
        # TODO: Implement execute_complete
        # if event["status"] == "timeout":
        #     raise AirflowException("Timeout waiting for files")
        # return event
        pass


# =============================================================================
# TEST DAG
# =============================================================================


def create_test_dag():
    """Create a test DAG using the custom trigger."""
    from airflow.sdk import dag, task

    @dag(
        dag_id="exercise_11_3_custom_trigger",
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["exercise", "triggers"],
    )
    def custom_trigger_test():
        wait_for_files = WaitForFilesOperator(
            task_id="wait_for_files",
            pattern="/tmp/trigger_test/*.csv",
            min_files=2,
            poll_interval=5.0,
            timeout=300.0,
        )

        @task
        def process_files(result: dict) -> None:
            print(f"Found files: {result.get('files', [])}")

        # process_files(wait_for_files)

    return custom_trigger_test()


# =============================================================================
# TESTING
# =============================================================================


def test_file_trigger():
    """Test the file pattern trigger."""
    import asyncio
    import os
    import tempfile

    async def run_test():
        # Create test directory and files
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create trigger
            trigger = FilePatternTrigger(
                pattern=f"{tmpdir}/*.csv",
                min_files=2,
                poll_interval=1.0,
                timeout=10.0,
            )

            # Test serialization
            classpath, kwargs = trigger.serialize()
            print(f"Serialized: {classpath}, {kwargs}")

            # Create files in background
            async def create_files():
                await asyncio.sleep(2)
                for i in range(3):
                    with open(f"{tmpdir}/file_{i}.csv", "w") as f:
                        f.write(f"data_{i}")
                    print(f"Created file_{i}.csv")

            # Run trigger and file creation concurrently
            create_task = asyncio.create_task(create_files())

            async for event in trigger.run():
                print(f"Trigger event: {event}")
                break

            await create_task

    asyncio.run(run_test())


if __name__ == "__main__":
    test_file_trigger()
