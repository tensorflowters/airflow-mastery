# Exercise 11.3: Custom Trigger

## Objective

Build a custom async trigger for specialized waiting patterns that aren't covered by built-in sensors.

## Background

Custom triggers enable deferrable operators for any waiting scenario:

```python
from airflow.triggers.base import BaseTrigger, TriggerEvent
from typing import AsyncIterator

class MyTrigger(BaseTrigger):
    """Custom trigger for specialized waiting."""

    def serialize(self) -> tuple[str, dict]:
        """Required: Serialize for database storage."""
        pass

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Required: Async generator that yields when done."""
        pass
```

### Trigger Lifecycle

```
1. Operator calls self.defer(trigger=MyTrigger(...))
2. Trigger serialized and stored in database
3. Triggerer process picks up trigger
4. Triggerer runs trigger.run() async
5. Trigger yields TriggerEvent when condition met
6. Operator.execute_complete() receives event
```

## Requirements

### Part 1: API Polling Trigger

Create a trigger that polls an HTTP endpoint:

```python
class HttpPollingTrigger(BaseTrigger):
    """Poll HTTP endpoint until condition is met."""

    def __init__(
        self,
        endpoint: str,
        headers: dict = None,
        response_check: str = "status",  # JSON path to check
        expected_value: str = "ready",
        poll_interval: float = 30.0,
        timeout: float = 3600.0,
    ):
        ...
```

### Part 2: Deferrable Operator

Create an operator that uses your trigger:

```python
class HttpPollingOperator(BaseDeferrableOperator):
    """Wait for HTTP endpoint to return expected value."""

    def execute(self, context):
        # Check immediately, defer if not ready
        ...

    def execute_complete(self, context, event):
        # Called when trigger fires
        ...
```

### Part 3: File Pattern Trigger (Alternative)

If HTTP is complex, create a simpler trigger:

```python
class FilePatternTrigger(BaseTrigger):
    """Wait for files matching a glob pattern."""

    def __init__(
        self,
        pattern: str,          # e.g., "/data/*.csv"
        min_files: int = 1,    # Minimum files to find
        poll_interval: float = 10.0,
    ):
        ...
```

## Starter Code

See `exercise_11_3_custom_trigger_starter.py`

## Hints

<details>
<summary>Hint 1: Trigger serialization</summary>

```python
def serialize(self) -> tuple[str, dict]:
    """
    Return (classpath, kwargs) for reconstruction.

    The classpath must be importable by the triggerer.
    """
    return (
        "mymodule.triggers.HttpPollingTrigger",
        {
            "endpoint": self.endpoint,
            "headers": self.headers,
            "poll_interval": self.poll_interval,
        },
    )
```

</details>

<details>
<summary>Hint 2: Async HTTP requests</summary>

```python
import aiohttp

async def _check_endpoint(self) -> bool:
    """Make async HTTP request."""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            self.endpoint,
            headers=self.headers,
        ) as response:
            if response.status == 200:
                data = await response.json()
                return data.get(self.response_check) == self.expected_value
    return False
```

</details>

<details>
<summary>Hint 3: Trigger run method</summary>

```python
async def run(self) -> AsyncIterator[TriggerEvent]:
    """Async generator that polls until condition met."""
    import asyncio
    from datetime import datetime, timedelta

    start_time = datetime.utcnow()
    timeout_at = start_time + timedelta(seconds=self.timeout)

    while datetime.utcnow() < timeout_at:
        try:
            if await self._check_condition():
                yield TriggerEvent({"status": "success"})
                return
        except Exception as e:
            self.log.warning(f"Check failed: {e}")

        await asyncio.sleep(self.poll_interval)

    # Timeout
    yield TriggerEvent({"status": "timeout"})
```

</details>

<details>
<summary>Hint 4: Deferrable operator pattern</summary>

```python
from airflow.models import BaseDeferrableOperator

class MyDeferrableOperator(BaseDeferrableOperator):
    def execute(self, context):
        # Check condition immediately
        if self._condition_met():
            return {"status": "immediate"}

        # Defer to trigger
        self.defer(
            trigger=MyTrigger(
                param1=self.param1,
                poll_interval=self.poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event):
        """Called when trigger fires."""
        if event["status"] == "timeout":
            raise AirflowException("Trigger timed out")
        return event
```

</details>

## Success Criteria

- [ ] Custom trigger implements serialize() correctly
- [ ] Trigger run() is an async generator
- [ ] Trigger handles timeout gracefully
- [ ] Deferrable operator uses the trigger
- [ ] execute_complete handles trigger events
- [ ] Trigger works with triggerer process
- [ ] Error scenarios handled properly

---

Congratulations! You've completed the Sensors & Deferrable module!
