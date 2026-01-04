# Exercise 2.2: Context Usage in TaskFlow

## Objective

Create a DAG that demonstrates how to access Airflow's runtime context within TaskFlow tasks, including logical dates, data intervals, and task instance information.

## Requirements

Your DAG should:
1. Be named `exercise_2_2_context_usage`
2. Have a start date of January 1, 2024
3. Not be scheduled (manual trigger only)
4. Include tags: `["exercise", "module-02", "context"]`

### Task Requirements

1. **get_execution_info** task:
   - Access `logical_date` from context
   - Access `data_interval_start` and `data_interval_end`
   - Format the logical_date as "YYYY-MM-DD"
   - Return execution metadata as a dictionary

2. **generate_filename** task:
   - Takes the execution info from upstream
   - Creates a filename pattern: `data_YYYY-MM-DD_HHmmss.csv`
   - Returns the filename and full path

3. **simulate_file_write** task:
   - Takes the file info from upstream
   - Simulates writing data to the file (just print)
   - Returns metadata about the operation (bytes written, etc.)

4. **log_summary** task:
   - Takes the write metadata
   - Logs a summary of the entire operation
   - Returns final status

## Starter Code

Create a file `dags/playground/exercise_2_2_context_usage.py`:

```python
"""
Exercise 2.2: Context Usage in TaskFlow
=======================================
Learn to access Airflow context within TaskFlow tasks.
"""

from datetime import datetime
# TODO: Import dag, task from airflow.sdk
# TODO: Import get_current_context if needed


# TODO: Define the DAG
def context_usage_dag():

    # TODO: Create get_execution_info task
    # Access context using **context parameter or get_current_context()
    # Return: {"logical_date": "...", "interval_start": "...", "interval_end": "..."}


    # TODO: Create generate_filename task
    # Take execution_info dict as parameter
    # Return: {"filename": "data_2024-01-01_120000.csv", "path": "/data/output/..."}


    # TODO: Create simulate_file_write task
    # Take file_info dict as parameter
    # Print simulation message
    # Return: {"bytes_written": 1024, "rows": 100, "status": "simulated"}


    # TODO: Create log_summary task
    # Take write_result dict as parameter
    # Print summary
    # Return: {"overall_status": "success", "completed_at": "..."}


    # TODO: Wire up the tasks
    pass


# TODO: Instantiate the DAG
```

## Verification

1. Save and wait for DAG to appear
2. Trigger a manual run
3. Check each task's logs:
   - `get_execution_info`: Should show the logical_date and intervals
   - `generate_filename`: Should show generated filename with date
   - `simulate_file_write`: Should show file write simulation
   - `log_summary`: Should show complete summary

## Hints

<details>
<summary>Hint 1: Accessing context with **context</summary>

```python
from airflow.sdk import task

@task
def my_task(**context):
    logical_date = context["logical_date"]
    ti = context["ti"]
    dag_run = context["dag_run"]
    return {"date": str(logical_date)}
```

</details>

<details>
<summary>Hint 2: Using get_current_context()</summary>

```python
from airflow.sdk import task, get_current_context

@task
def my_task():
    context = get_current_context()
    logical_date = context["logical_date"]
    return {"date": str(logical_date)}
```

</details>

<details>
<summary>Hint 3: Formatting dates</summary>

```python
# logical_date is a pendulum DateTime object
formatted = logical_date.strftime("%Y-%m-%d")
filename_date = logical_date.strftime("%Y-%m-%d_%H%M%S")
```

</details>

<details>
<summary>Hint 4: Data intervals</summary>

```python
@task
def show_intervals(**context):
    # Data intervals define the period this run covers
    start = context["data_interval_start"]
    end = context["data_interval_end"]

    print(f"This run covers: {start} to {end}")
    return {
        "interval_start": str(start),
        "interval_end": str(end),
    }
```

</details>

## Success Criteria

- [ ] DAG runs without errors
- [ ] get_execution_info correctly extracts context values
- [ ] Filename includes the logical_date formatted correctly
- [ ] All tasks pass data through XCom properly
- [ ] Final summary includes all operation metadata

## Context Variables Reference

| Variable | Description |
|----------|-------------|
| `logical_date` | The logical date/time for this DAG run |
| `data_interval_start` | Start of the data interval |
| `data_interval_end` | End of the data interval |
| `dag_run` | The DagRun object |
| `ti` | TaskInstance object |
| `params` | DAG parameters |
| `run_id` | Unique run identifier |
