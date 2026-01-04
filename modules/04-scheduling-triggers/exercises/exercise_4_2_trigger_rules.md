# Exercise 4.2: Trigger Rules Pipeline

## Objective

Create a DAG that demonstrates Airflow's trigger rules for controlling task execution based on the states of upstream tasks.

## Background

Trigger rules determine when a task should run based on the status of its upstream dependencies. Understanding trigger rules is essential for building robust pipelines that handle failures gracefully.

### Available Trigger Rules

| Rule | Behavior |
|------|----------|
| `all_success` | (Default) Run if ALL upstream tasks succeeded |
| `all_failed` | Run if ALL upstream tasks failed |
| `all_done` | Run if ALL upstream tasks completed (any state) |
| `all_skipped` | Run if ALL upstream tasks were skipped |
| `one_success` | Run if AT LEAST ONE upstream succeeded |
| `one_failed` | Run if AT LEAST ONE upstream failed |
| `one_done` | Run if AT LEAST ONE upstream completed |
| `none_failed` | Run if NO upstream tasks failed (success or skipped) |
| `none_skipped` | Run if NO upstream tasks were skipped |
| `always` | Run regardless of upstream states |

## Requirements

Your DAG should:
1. Be named `exercise_4_2_trigger_rules`
2. Have a start date of January 1, 2024
3. Use `schedule=None` (manual trigger only)
4. Include tags: `["exercise", "module-04", "trigger-rules"]`

### Task Structure

Create a pipeline with the following structure:

```
                    ┌─────────────────┐
                    │  always_succeed │──┐
                    └─────────────────┘  │
                                         │
┌─────────┐         ┌─────────────────┐  │    ┌──────────────┐    ┌─────────┐
│  start  │────────▶│  always_fail    │──┼───▶│  all_done    │───▶│ cleanup │
└─────────┘         └─────────────────┘  │    └──────────────┘    └─────────┘
                                         │           │
                    ┌─────────────────┐  │    ┌──────────────┐
                    │ random_outcome  │──┘    │  one_failed  │
                    └─────────────────┘       └──────────────┘
```

### Task Requirements

1. **start**: Simple task that marks the beginning of the pipeline

2. **always_succeed**: Always completes successfully
   - Print "This task always succeeds!"

3. **always_fail**: Always raises an exception
   - Raise `AirflowException` with message "This task always fails!"

4. **random_outcome**: Randomly succeeds or fails (50/50 chance)
   - Use `random.random() < 0.5` to determine outcome

5. **all_done_task**: Runs when ALL upstream tasks complete
   - Use `trigger_rule=TriggerRule.ALL_DONE`
   - Print the states of upstream tasks

6. **one_failed_task**: Runs when AT LEAST ONE upstream failed
   - Use `trigger_rule=TriggerRule.ONE_FAILED`
   - Log which tasks failed

7. **cleanup**: ALWAYS runs regardless of upstream states
   - Use `trigger_rule=TriggerRule.ALWAYS`
   - Print "Cleanup completed - this always runs!"

## Key Concepts

### Importing TriggerRule

```python
from airflow.utils.trigger_rule import TriggerRule
```

### Applying Trigger Rules

```python
@task(trigger_rule=TriggerRule.ALL_DONE)
def my_task():
    pass
```

### Handling Expected Failures

When a task is designed to fail, downstream tasks with appropriate trigger rules will still execute.

## Starter Code

See `exercise_4_2_trigger_rules_starter.py`

## Testing Your DAG

```bash
# Run the DAG - observe how trigger rules affect execution
airflow dags test exercise_4_2_trigger_rules 2024-01-15

# Check the UI to see task states and which tasks ran
```

## Hints

<details>
<summary>Hint 1: Raising an exception</summary>

```python
from airflow.exceptions import AirflowException

@task
def always_fail():
    raise AirflowException("This task always fails!")
```

</details>

<details>
<summary>Hint 2: Random task outcome</summary>

```python
import random

@task
def random_outcome():
    if random.random() < 0.5:
        raise AirflowException("Random failure!")
    return "Random success!"
```

</details>

<details>
<summary>Hint 3: Checking upstream states</summary>

```python
@task(trigger_rule=TriggerRule.ALL_DONE)
def all_done_task(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]

    # Get all task instances in this run
    task_instances = dag_run.get_task_instances()

    for task_instance in task_instances:
        print(f"{task_instance.task_id}: {task_instance.state}")
```

</details>

## Success Criteria

- [ ] DAG has correct structure with 7 tasks
- [ ] `always_succeed` always completes successfully
- [ ] `always_fail` always fails with AirflowException
- [ ] `random_outcome` randomly succeeds or fails
- [ ] `all_done_task` runs even when upstream tasks fail
- [ ] `one_failed_task` runs when any upstream task fails
- [ ] `cleanup` ALWAYS runs regardless of other task states
- [ ] You can explain when each trigger rule is useful

## Real-World Applications

| Trigger Rule | Use Case |
|--------------|----------|
| `all_done` | Notification tasks that report on pipeline completion |
| `one_failed` | Alert tasks that notify on any failure |
| `always` | Cleanup tasks, resource deallocation, logging |
| `none_failed` | Tasks that can proceed if nothing explicitly failed |
| `one_success` | Tasks that need at least one successful input |
