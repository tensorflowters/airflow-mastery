# Exercise 4.1: Schedule Interpretation

## Objective

Create a DAG that helps you understand Airflow's scheduling concepts, including cron expressions, logical dates, and data intervals.

## Requirements

Your DAG should:
1. Be named `exercise_4_1_schedule_interpretation`
2. Have a start date of January 1, 2024
3. Use schedule `"0 */6 * * 1-5"` (every 6 hours on weekdays)
4. Include tags: `["exercise", "module-04", "scheduling"]`

### Task Requirements

1. **explain_schedule** task:
   - Print the schedule in human-readable format
   - Explain what the cron expression means

2. **show_execution_context** task:
   - Print the `logical_date`
   - Print `data_interval_start` and `data_interval_end`
   - Calculate and print the interval duration
   - Explain what data this run processes

3. **demonstrate_templating** task:
   - Use Jinja templates: `{{ ds }}`, `{{ data_interval_start }}`
   - Show how templating works with scheduling

## Key Concepts

| Concept | Description |
|---------|-------------|
| `logical_date` | The logical execution time (formerly `execution_date`) |
| `data_interval_start` | Start of the data period this run covers |
| `data_interval_end` | End of the data period (equals logical_date for scheduled runs) |
| `ds` | Date stamp: `YYYY-MM-DD` format of logical_date |

## Cron Expression Reference

`0 */6 * * 1-5` means:
- `0` - At minute 0
- `*/6` - Every 6 hours (0, 6, 12, 18)
- `*` - Every day of month
- `*` - Every month
- `1-5` - Monday through Friday

## Starter Code

Create a file `dags/playground/exercise_4_1_schedule_interpretation.py`:

```python
"""
Exercise 4.1: Schedule Interpretation
=====================================
Understand Airflow scheduling concepts.
"""

from datetime import datetime
# TODO: Import dag and task from airflow.sdk


# TODO: Create the DAG with the specified schedule
# Note: Use @dag decorator or context manager

    # TODO: Create explain_schedule task
    # Print what the schedule means

    # TODO: Create show_execution_context task
    # Access and print context variables

    # TODO: Create demonstrate_templating task
    # Show Jinja template usage
```

## Testing Your DAG

Use `airflow dags test` to simulate runs for different dates:

```bash
# Test for a Monday at 6am
airflow dags test exercise_4_1_schedule_interpretation 2024-01-08T06:00:00

# Test for a Friday at midnight
airflow dags test exercise_4_1_schedule_interpretation 2024-01-12T00:00:00

# This should NOT run (Saturday)
airflow dags test exercise_4_1_schedule_interpretation 2024-01-13T06:00:00
```

## Hints

<details>
<summary>Hint 1: Accessing context</summary>

```python
@task
def show_execution_context(**context):
    logical_date = context["logical_date"]
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]

    print(f"Logical date: {logical_date}")
    print(f"Data interval: {interval_start} to {interval_end}")
```

</details>

<details>
<summary>Hint 2: Using BashOperator with templates</summary>

```python
from airflow.providers.standard.operators.bash import BashOperator

templated_task = BashOperator(
    task_id="demonstrate_templating",
    bash_command="""
        echo "Date stamp: {{ ds }}"
        echo "Interval start: {{ data_interval_start }}"
        echo "Interval end: {{ data_interval_end }}"
    """,
)
```

</details>

## Success Criteria

- [ ] DAG has correct cron schedule
- [ ] explain_schedule correctly describes the schedule
- [ ] show_execution_context displays all interval information
- [ ] Templating task shows correct date values
- [ ] You can explain the difference between logical_date and data_interval
