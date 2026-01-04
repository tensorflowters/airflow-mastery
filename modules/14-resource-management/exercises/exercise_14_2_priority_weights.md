# Exercise 14.2: Priority Weights

## Objective

Implement priority-based task scheduling to ensure critical tasks execute first.

## Background

Priority weights determine the order in which tasks are scheduled when resources are constrained:
- Higher weight = higher priority
- Critical tasks should run before background tasks
- Weight rules determine how weights are calculated

## Requirements

### Part 1: Priority Constants

Create a priority tier system:

```python
class Priority:
    """Priority weight constants for consistent task scheduling."""

    CRITICAL = 100
    HIGH = 75
    NORMAL = 50
    LOW = 25
    BACKGROUND = 1

    @staticmethod
    def get_tier(weight: int) -> str:
        """Get tier name from weight."""
        pass
```

### Part 2: Priority-Aware DAG

Create a DAG demonstrating priority usage:

```python
@dag(...)
def priority_demo():
    # Critical: Alert checking
    # High: Production ETL
    # Normal: Reporting
    # Low: Data cleanup
    # Background: Archival
```

### Part 3: Priority Calculator

Implement weight rule calculations:

```python
class PriorityCalculator:
    def calculate_downstream(self, dag, task_id: str) -> int:
        """Calculate weight based on downstream tasks."""
        pass

    def calculate_upstream(self, dag, task_id: str) -> int:
        """Calculate weight based on upstream tasks."""
        pass

    def get_execution_order(self, dag) -> list:
        """Get expected task execution order based on priority."""
        pass
```

## Starter Code

See `exercise_14_2_priority_weights_starter.py`

## Hints

<details>
<summary>Hint 1: Weight rules</summary>

```python
from airflow.utils.weight_rule import WeightRule

@task(
    priority_weight=10,
    weight_rule=WeightRule.DOWNSTREAM  # Sum of downstream weights
)
def my_task():
    pass
```

</details>

<details>
<summary>Hint 2: Calculating effective priority</summary>

```python
def calculate_downstream_weight(task, visited=None):
    visited = visited or set()
    if task.task_id in visited:
        return 0
    visited.add(task.task_id)

    weight = task.priority_weight
    for downstream in task.downstream_list:
        weight += calculate_downstream_weight(downstream, visited)
    return weight
```

</details>

## Success Criteria

- [ ] Priority tiers are clearly defined
- [ ] DAG tasks use appropriate priorities
- [ ] Weight calculations are correct
- [ ] Execution order reflects priorities

---

Next: [Exercise 14.3: Concurrency Limits →](exercise_14_3_concurrency_limits.md)
