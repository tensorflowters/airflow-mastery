# Module 11: Sensors & Deferrable Operators

## Overview

This module covers Airflow's sensor patterns and deferrable operators - essential tools for building efficient workflows that wait for external conditions without consuming worker resources.

**Learning Time**: 4-5 hours

## Learning Objectives

By the end of this module, you will be able to:

1. Understand sensor modes (poke vs reschedule) and when to use each
2. Configure built-in sensors for files, HTTP, SQL, and external systems
3. Convert traditional sensors to deferrable operators
4. Create custom triggers for specialized waiting patterns
5. Optimize resource usage with async patterns

## Prerequisites

- Completed Modules 01-06
- Understanding of async/await in Python
- Familiarity with Airflow task states

## Key Concepts

### 1. Sensor Fundamentals

Sensors are special operators that wait for a condition to be met before continuing:

```python
from airflow.sensors.filesystem import FileSensor

# Wait for a file to appear
wait_for_file = FileSensor(
    task_id="wait_for_data",
    filepath="/data/incoming/daily_export.csv",
    poke_interval=60,  # Check every 60 seconds
    timeout=3600,      # Fail after 1 hour
)
```

### 2. Sensor Modes

| Mode | Worker Usage | Best For |
|------|--------------|----------|
| **poke** | Holds worker slot | Short waits (<5 min) |
| **reschedule** | Releases worker | Long waits (>5 min) |

```python
# Poke mode (default) - keeps worker occupied
sensor_poke = FileSensor(
    task_id="quick_check",
    mode="poke",
    poke_interval=30,
    timeout=300,
)

# Reschedule mode - releases worker between checks
sensor_reschedule = FileSensor(
    task_id="long_wait",
    mode="reschedule",
    poke_interval=300,  # 5 minutes
    timeout=86400,      # 24 hours
)
```

### 3. Built-in Sensors

Airflow provides sensors for common patterns:

| Sensor | Purpose |
|--------|---------|
| `FileSensor` | Wait for file existence |
| `ExternalTaskSensor` | Wait for another DAG/task |
| `HttpSensor` | Wait for HTTP endpoint condition |
| `SqlSensor` | Wait for SQL query result |
| `DateTimeSensor` | Wait until specific time |
| `TimeDeltaSensor` | Wait for duration after start |
| `S3KeySensor` | Wait for S3 object |
| `GCSObjectExistenceSensor` | Wait for GCS object |

### 4. Deferrable Operators

Deferrable operators use async triggers to release worker slots entirely:

```python
from airflow.sensors.filesystem import FileSensor

# Traditional sensor (uses worker during poke/reschedule)
traditional = FileSensor(
    task_id="traditional_wait",
    filepath="/data/file.csv",
    mode="reschedule",
)

# Deferrable sensor (releases worker completely)
deferrable = FileSensor(
    task_id="async_wait",
    filepath="/data/file.csv",
    deferrable=True,  # Enable async mode
)
```

### 5. How Deferrable Works

```
┌─────────────────────────────────────────────────────────────┐
│                    Deferrable Flow                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. Task starts on Worker                                   │
│     ↓                                                       │
│  2. Task defers itself, creates Trigger                     │
│     ↓                                                       │
│  3. Worker slot released (available for other tasks)        │
│     ↓                                                       │
│  4. Triggerer process monitors condition (lightweight)      │
│     ↓                                                       │
│  5. Condition met → Trigger fires event                     │
│     ↓                                                       │
│  6. Task resumes on Worker with result                      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 6. Custom Triggers

Create custom triggers for specialized waiting patterns:

```python
from airflow.triggers.base import BaseTrigger, TriggerEvent
from typing import AsyncIterator
import asyncio

class CustomFileTrigger(BaseTrigger):
    """Trigger that fires when file appears."""

    def __init__(self, filepath: str, poll_interval: float = 5.0):
        super().__init__()
        self.filepath = filepath
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict]:
        """Serialize trigger for storage."""
        return (
            "path.to.CustomFileTrigger",
            {"filepath": self.filepath, "poll_interval": self.poll_interval},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Async generator that yields when condition is met."""
        import aiofiles.os

        while True:
            if await aiofiles.os.path.exists(self.filepath):
                yield TriggerEvent({"filepath": self.filepath, "found": True})
                return
            await asyncio.sleep(self.poll_interval)
```

### 7. Triggerer Component

The triggerer is a separate Airflow component that runs triggers:

```bash
# Start the triggerer (required for deferrable operators)
airflow triggerer

# In docker-compose
triggerer:
  command: triggerer
  depends_on:
    - scheduler
```

## Resource Comparison

| Approach | Worker Usage | Scheduler Load | Best For |
|----------|--------------|----------------|----------|
| Poke mode | 100% during wait | Low | Short waits |
| Reschedule mode | ~5% (periodic) | Medium | Medium waits |
| Deferrable | 0% during wait | Very Low | Long waits, scale |

## Exercises

### Exercise 11.1: Sensor Modes
Compare poke vs reschedule modes:
- Create sensors with both modes
- Measure worker slot usage
- Understand timeout behavior

### Exercise 11.2: Deferrable Conversion
Convert traditional sensors to deferrable:
- Migrate FileSensor to deferrable mode
- Configure triggerer
- Verify resource savings

### Exercise 11.3: Custom Trigger
Build a custom trigger:
- Create async trigger for API polling
- Implement serialization
- Test with deferrable operator

## Solutions

Complete solutions are in the `solutions/` directory.

## Common Patterns

### Pattern 1: File-Based Data Pipeline

```python
from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime

@dag(start_date=datetime(2024, 1, 1))
def file_pipeline():
    wait_for_input = FileSensor(
        task_id="wait_for_input",
        filepath="/data/input/{{ ds }}/data.csv",
        deferrable=True,
        poke_interval=60,
        timeout=3600,
    )

    @task
    def process_file():
        # Process the file
        pass

    wait_for_input >> process_file()
```

### Pattern 2: Cross-DAG Dependencies

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id="wait_for_upstream",
    external_dag_id="upstream_dag",
    external_task_id="final_task",
    deferrable=True,
    allowed_states=["success"],
    failed_states=["failed", "skipped"],
)
```

### Pattern 3: HTTP API Readiness

```python
from airflow.sensors.http import HttpSensor

wait_for_api = HttpSensor(
    task_id="wait_for_api",
    http_conn_id="api_connection",
    endpoint="/health",
    response_check=lambda response: response.json()["status"] == "ready",
    deferrable=True,
    poke_interval=30,
)
```

## Best Practices

1. **Choose the right mode**:
   - Poke: waits < 5 minutes
   - Reschedule: waits 5-60 minutes
   - Deferrable: waits > 60 minutes or at scale

2. **Set appropriate timeouts**:
   ```python
   sensor = FileSensor(
       timeout=3600,           # Max wait time
       soft_fail=True,         # Mark as skipped instead of failed
       poke_interval=60,       # Balance between responsiveness and load
   )
   ```

3. **Use exponential backoff for flaky conditions**:
   ```python
   sensor = HttpSensor(
       exponential_backoff=True,
       poke_interval=10,       # Starts at 10s
       # Increases: 10, 20, 40, 80, ... up to max
   )
   ```

4. **Enable deferrable when available**:
   ```python
   # Check if sensor supports deferrable
   sensor = FileSensor(
       deferrable=True,  # Airflow 2.6+ / 3.x
   )
   ```

## Troubleshooting

### Sensor Stuck in Running State

```python
# Check sensor logs
airflow tasks logs <dag_id> <task_id> <execution_date>

# Common causes:
# 1. Condition never met → Check file/endpoint existence
# 2. Timeout too long → Reduce timeout value
# 3. poke_interval too short → Increase to reduce DB load
```

### Deferrable Not Working

```bash
# 1. Verify triggerer is running
airflow triggerer

# 2. Check trigger registration
airflow triggers list

# 3. Verify async dependencies
pip install aiohttp aiofiles
```

### High Database Load

```python
# Reduce sensor frequency
sensor = FileSensor(
    poke_interval=300,  # 5 minutes instead of default 60s
    mode="reschedule",   # Release worker between checks
)
```

## Next Steps

After completing this module:
1. Review Module 12: REST API for external integrations
2. Explore Module 13: Connections & Secrets for secure configuration
3. Study Module 14: Resource Management for scaling

## References

- [Airflow Sensors Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/sensors.html)
- [Deferrable Operators Guide](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html)
- [Writing Custom Triggers](https://airflow.apache.org/docs/apache-airflow/stable/howto/trigger.html)
