# Exercise 11.2: Deferrable Conversion

## Objective

Convert traditional sensors to deferrable operators to achieve maximum resource efficiency with zero worker usage during waits.

## Background

Deferrable operators release worker slots completely by using async triggers:

```
Traditional Sensor (reschedule):
  [Worker] → sleep → [Worker] → sleep → [Worker] → done
             ↑ releases ↑        ↑ releases ↑

Deferrable Operator:
  [Worker] → defer → [Triggerer async] → [Worker] → done
             ↑ 0% worker usage ↑
```

### Key Components

1. **Deferrable Operator**: Task that can defer itself
2. **Trigger**: Async class that monitors the condition
3. **Triggerer**: Separate process that runs triggers

## Requirements

### Part 1: Enable Deferrable Mode

Convert existing sensors to deferrable:

```python
# Before (traditional)
sensor = FileSensor(
    task_id="wait",
    filepath="/path/to/file",
    mode="reschedule",
)

# After (deferrable)
sensor = FileSensor(
    task_id="wait",
    filepath="/path/to/file",
    deferrable=True,  # Key change!
)
```

### Part 2: Multiple Deferrable Sensors

Create deferrable versions of:
- FileSensor
- DateTimeSensor
- TimeDeltaSensor

### Part 3: Resource Monitoring

Implement monitoring to verify:
- Worker slots are released during defer
- Trigger is running in triggerer process
- Task resumes correctly after condition met

### Part 4: Error Handling

Handle deferrable-specific scenarios:
- Triggerer not running
- Trigger timeout
- Trigger errors

## Starter Code

See `exercise_11_2_deferrable_conversion_starter.py`

## Verification

```bash
# Ensure triggerer is running
airflow triggerer

# Check active triggers
airflow triggers list

# Monitor trigger events
tail -f $AIRFLOW_HOME/logs/triggerer/*.log
```

## Hints

<details>
<summary>Hint 1: Enabling deferrable mode</summary>

```python
from airflow.sensors.filesystem import FileSensor

# Simply add deferrable=True
wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath="/data/{{ ds }}/input.csv",
    deferrable=True,
    poke_interval=60,  # Still used as trigger poll interval
    timeout=3600,
)
```

</details>

<details>
<summary>Hint 2: Checking if deferrable is supported</summary>

```python
# Not all sensors support deferrable mode
# Check the documentation or source code

# Sensors with deferrable support in Airflow 3.x:
# - FileSensor
# - DateTimeSensor
# - TimeDeltaSensor
# - HttpSensor (with deferrable=True)
# - ExternalTaskSensor
# - S3KeySensor (in AWS provider)
```

</details>

<details>
<summary>Hint 3: Triggerer configuration</summary>

```yaml
# docker-compose.yml
triggerer:
  image: apache/airflow:3.1.5
  command: triggerer
  depends_on:
    - scheduler
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor

# Or start manually:
# airflow triggerer
```

</details>

## Success Criteria

- [ ] All sensors converted to deferrable mode
- [ ] Triggerer process is running
- [ ] Worker slots released during defer (verify in logs)
- [ ] Tasks resume correctly after trigger fires
- [ ] Timeout handling works correctly
- [ ] Error scenarios handled gracefully

---

Next: [Exercise 11.3: Custom Trigger →](exercise_11_3_custom_trigger.md)
