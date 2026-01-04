# Exercise 11.1: Sensor Modes

## Objective

Understand and compare Airflow sensor modes (poke vs reschedule) by building a file processing DAG that demonstrates worker resource usage patterns.

## Background

Sensors can operate in two modes:

| Mode | Behavior | Worker Usage |
|------|----------|--------------|
| **poke** | Holds worker slot, sleeps between checks | 100% during entire wait |
| **reschedule** | Releases worker, reschedules task | ~0% between checks |

## Requirements

### Part 1: Basic Sensor Setup

Create two FileSensors with different modes:

1. **Poke mode sensor**:
   - Check for file every 10 seconds
   - Timeout after 5 minutes
   - Log when checking

2. **Reschedule mode sensor**:
   - Check for file every 30 seconds
   - Timeout after 1 hour
   - Log when checking

### Part 2: Sensor Configuration

Configure sensors with:
- `soft_fail=True` - Skip instead of fail on timeout
- `exponential_backoff=True` - Increase interval on consecutive failures
- Custom `poke_interval` values

### Part 3: Multiple Sensor Types

Add additional sensors:
- `DateTimeSensor` - Wait until specific time
- `TimeDeltaSensor` - Wait for duration after start
- `HttpSensor` - Wait for API endpoint (mock)

### Part 4: Monitoring

Implement logging to observe:
- When each sensor starts
- Each poke attempt
- Worker slot status
- Final outcome

## Expected File Structure

```
/tmp/airflow_sensor_exercise/
├── input/
│   └── data_{{ ds }}.csv     # File to wait for
└── processed/
    └── result_{{ ds }}.json  # Processed output
```

## Starter Code

See `exercise_11_1_sensor_modes_starter.py`

## Verification

Test your implementation:

```bash
# Create test file to trigger sensors
mkdir -p /tmp/airflow_sensor_exercise/input
echo "data" > /tmp/airflow_sensor_exercise/input/data_2024-01-15.csv

# Trigger DAG and observe behavior
airflow dags trigger exercise_11_1_sensor_modes

# Monitor task states
airflow tasks states-for-dag-run exercise_11_1_sensor_modes <run_id>
```

## Hints

<details>
<summary>Hint 1: FileSensor configuration</summary>

```python
from airflow.sensors.filesystem import FileSensor

wait_poke = FileSensor(
    task_id="wait_poke_mode",
    filepath="/tmp/data/{{ ds }}/file.csv",
    mode="poke",
    poke_interval=10,
    timeout=300,
)
```

</details>

<details>
<summary>Hint 2: Reschedule mode setup</summary>

```python
wait_reschedule = FileSensor(
    task_id="wait_reschedule_mode",
    filepath="/tmp/data/{{ ds }}/file.csv",
    mode="reschedule",
    poke_interval=30,
    timeout=3600,
    soft_fail=True,
)
```

</details>

<details>
<summary>Hint 3: DateTimeSensor usage</summary>

```python
from airflow.sensors.date_time import DateTimeSensor
from pendulum import datetime

wait_until = DateTimeSensor(
    task_id="wait_until_time",
    target_time="{{ execution_date.add(minutes=5) }}",
)
```

</details>

## Success Criteria

- [ ] Both sensor modes implemented correctly
- [ ] Sensors use templated file paths
- [ ] Appropriate poke_interval for each mode
- [ ] Timeout and soft_fail configured
- [ ] Multiple sensor types demonstrated
- [ ] Logging shows poke attempts
- [ ] DAG completes when file exists
- [ ] Resource usage difference observable

---

Next: [Exercise 11.2: Deferrable Conversion →](exercise_11_2_deferrable_conversion.md)
