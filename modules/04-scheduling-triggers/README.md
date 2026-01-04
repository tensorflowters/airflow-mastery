# Module 04: Scheduling & Triggers

## 🎯 Learning Objectives

By the end of this module, you will:
- Master cron expressions and Airflow schedule syntax
- Understand Data Intervals and Logical Dates
- Use timetables for complex scheduling
- Configure trigger rules for conditional task execution
- Handle timezones correctly
- Use manual triggers and trigger parameters

## ⏱️ Estimated Time: 3-4 hours

---

## 1. Schedule Syntax Options

### Preset Schedules

```python
with DAG(
    dag_id="scheduled_dag",
    schedule="@daily",  # Preset schedule
    start_date=datetime(2024, 1, 1),
):
    ...
```

| Preset | Equivalent Cron | Runs At |
|--------|----------------|---------|
| `@once` | - | Once only |
| `@hourly` | `0 * * * *` | Every hour at :00 |
| `@daily` | `0 0 * * *` | Midnight daily |
| `@weekly` | `0 0 * * 0` | Midnight Sunday |
| `@monthly` | `0 0 1 * *` | 1st of month midnight |
| `@yearly` | `0 0 1 1 *` | Jan 1st midnight |

### Cron Expressions

```python
# Format: minute hour day-of-month month day-of-week
with DAG(
    dag_id="cron_dag",
    schedule="30 6 * * 1-5",  # 6:30 AM on weekdays
    ...
):
```

**Cron Fields:**
```
┌───────────── minute (0 - 59)
│ ┌───────────── hour (0 - 23)
│ │ ┌───────────── day of the month (1 - 31)
│ │ │ ┌───────────── month (1 - 12)
│ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday = 0)
│ │ │ │ │
* * * * *
```

**Common Patterns:**
```python
"0 */2 * * *"     # Every 2 hours
"0 9-17 * * 1-5"  # Every hour 9 AM - 5 PM, weekdays
"*/15 * * * *"    # Every 15 minutes
"0 0 L * *"       # Last day of month (if supported)
```

### No Schedule (Manual Only)

```python
with DAG(
    dag_id="manual_only",
    schedule=None,  # Only triggered manually
    ...
):
```

---

## 2. Understanding Data Intervals

This is often confusing for newcomers. Let's clarify:

### The Timeline

For a **daily** DAG starting 2024-01-01:

```
Time:        00:00          00:00          00:00          00:00
Date:        Jan 1          Jan 2          Jan 3          Jan 4
             │              │              │              │
             ▼              ▼              ▼              ▼
Run 1:       ├──────────────┤              │              │
             │   Interval 1 │              │              │
             │              │              │              │
             │              ├──────────────┤              │
Run 2:       │              │   Interval 2 │              │
             │              │              │              │
             │              │              ├──────────────┤
Run 3:       │              │              │   Interval 3 │
```

**Key Insight**: Each run processes data FROM its interval. The run executes at the END of its interval.

### Context Variables

```python
@task
def show_dates(**context):
    # What date/time this run is "for"
    logical_date = context["logical_date"]
    
    # The data interval this run covers
    start = context["data_interval_start"]
    end = context["data_interval_end"]
    
    # ds is a string shortcut
    ds = context["ds"]  # "2024-01-01"
    ds_nodash = context["ds_nodash"]  # "20240101"
    
    print(f"Logical date: {logical_date}")
    print(f"Processing data from {start} to {end}")
```

### Example: Daily DAG Running on Jan 2

```
logical_date:        2024-01-01T00:00:00
data_interval_start: 2024-01-01T00:00:00  
data_interval_end:   2024-01-02T00:00:00
Actual execution:    2024-01-02T00:00:00 (or shortly after)
```

---

## 3. Trigger Rules

Trigger rules determine when a task runs based on upstream task states.

```python
from airflow.sdk import task
from airflow.utils.trigger_rule import TriggerRule

@task(trigger_rule=TriggerRule.ALL_SUCCESS)  # Default
def default_task():
    pass

@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup_task():
    """Runs regardless of upstream success/failure"""
    pass

@task(trigger_rule=TriggerRule.ONE_SUCCESS)
def needs_any_success():
    """Runs if ANY upstream task succeeds"""
    pass
```

### Available Trigger Rules

| Rule | Behavior |
|------|----------|
| `ALL_SUCCESS` | All upstream tasks succeeded (default) |
| `ALL_FAILED` | All upstream tasks failed |
| `ALL_DONE` | All upstream tasks completed (any state) |
| `ALL_SKIPPED` | All upstream tasks were skipped |
| `ONE_SUCCESS` | At least one upstream succeeded |
| `ONE_FAILED` | At least one upstream failed |
| `ONE_DONE` | At least one upstream completed |
| `NONE_FAILED` | No upstream failed (includes skipped) |
| `NONE_FAILED_MIN_ONE_SUCCESS` | No failures AND at least one success |
| `NONE_SKIPPED` | No upstream was skipped |
| `ALWAYS` | Run regardless of upstream states |

### Common Patterns

```python
# Cleanup that always runs
@task(trigger_rule=TriggerRule.ALL_DONE)
def cleanup():
    """Clean up temp files regardless of pipeline success"""
    pass

# Notification on any failure
@task(trigger_rule=TriggerRule.ONE_FAILED)
def notify_failure():
    """Alert team if any step fails"""
    pass

# Run if main path skipped
@task(trigger_rule=TriggerRule.ALL_SKIPPED)
def fallback():
    """Alternative path if primary was skipped"""
    pass
```

---

## 4. Catchup and Backfilling

### Catchup Configuration

```python
with DAG(
    dag_id="catchup_example",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=True,   # Default is False in Airflow 3!
):
    ...
```

**With `catchup=True`**: If DAG is first enabled on Jan 10, Airflow will create runs for Jan 1-9.

**With `catchup=False`**: Only future runs are scheduled.

### Manual Backfilling

```bash
# Backfill specific date range
airflow dags backfill \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    my_dag_id
```

---

## 5. Timezones

### Setting DAG Timezone

```python
import pendulum

with DAG(
    dag_id="timezone_aware",
    start_date=pendulum.datetime(2024, 1, 1, tz="America/New_York"),
    schedule="0 9 * * *",  # 9 AM Eastern Time
):
    ...
```

### Timezone-Aware Tasks

```python
@task
def timezone_task(**context):
    import pendulum
    
    # Logical date is always UTC
    logical_date_utc = context["logical_date"]
    
    # Convert to local timezone
    local_tz = pendulum.timezone("America/New_York")
    local_time = logical_date_utc.in_timezone(local_tz)
    
    print(f"UTC: {logical_date_utc}")
    print(f"Local: {local_time}")
```

---

## 6. Manual Triggers with Parameters

### DAG Parameters

```python
from airflow.models.param import Param

with DAG(
    dag_id="parameterized_dag",
    schedule=None,
    params={
        "environment": Param(
            default="dev",
            type="string",
            enum=["dev", "staging", "prod"],
        ),
        "batch_size": Param(
            default=100,
            type="integer",
            minimum=1,
            maximum=10000,
        ),
        "dry_run": Param(
            default=True,
            type="boolean",
        ),
    },
):
    @task
    def use_params(**context):
        params = context["params"]
        env = params["environment"]
        batch = params["batch_size"]
        dry_run = params["dry_run"]
        
        print(f"Running in {env} with batch {batch}")
        if dry_run:
            print("DRY RUN - no changes made")
```

### Triggering with Parameters (CLI)

```bash
airflow dags trigger parameterized_dag \
    --conf '{"environment": "prod", "batch_size": 500, "dry_run": false}'
```

---

## 7. Custom Timetables

For complex schedules that cron can't express:

```python
from airflow.timetables.base import DagRunInfo, DataInterval, Timetable
from pendulum import DateTime, Duration

class BusinessDaysTimetable(Timetable):
    """Run only on business days (Mon-Fri)"""
    
    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction,
    ) -> DagRunInfo | None:
        
        if last_automated_data_interval is None:
            # First run
            next_start = restriction.earliest
        else:
            next_start = last_automated_data_interval.end
        
        # Skip to next business day
        while next_start.weekday() >= 5:  # Saturday=5, Sunday=6
            next_start = next_start.add(days=1)
        
        next_end = next_start.add(days=1)
        
        return DagRunInfo(
            run_after=next_end,
            data_interval=DataInterval(start=next_start, end=next_end),
        )

# Register in plugins
# Then use in DAG:
with DAG(
    dag_id="business_days_only",
    timetable=BusinessDaysTimetable(),
):
    ...
```

---

## 📝 Exercises

### Exercise 4.1: Schedule Interpretation
Create a DAG with `schedule="0 */6 * * 1-5"` and add a task that prints:
- The logical date
- The data interval start and end
- What date's data this run is processing

Run `airflow dags test` for a few different dates and verify your understanding.

### Exercise 4.2: Trigger Rules Pipeline
Create a DAG with this structure:
- 3 parallel tasks (one always succeeds, one always fails, one is random)
- A task that runs only if ALL complete (regardless of state)
- A task that runs only if AT LEAST ONE failed
- A final cleanup task that ALWAYS runs

### Exercise 4.3: Parameterized DAG
Create a data processing DAG with parameters:
- `source_table`: string, required
- `target_table`: string, required  
- `mode`: enum ["append", "overwrite"], default "append"
- `limit`: integer, optional, default null (no limit)

---

## ✅ Checkpoint

Before moving to Module 05, ensure you can:

- [ ] Write cron expressions for various schedules
- [ ] Explain data intervals vs logical date
- [ ] Use trigger rules for conditional execution
- [ ] Configure catchup behavior appropriately
- [ ] Handle timezones in DAGs
- [ ] Create parameterized DAGs

---

Next: [Module 05: Assets & Data-Aware Scheduling →](../05-assets-data-aware/README.md)
