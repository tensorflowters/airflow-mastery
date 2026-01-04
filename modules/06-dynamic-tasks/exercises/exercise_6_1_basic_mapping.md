# Exercise 6.1: Basic Mapping

## Objective

Learn the fundamentals of Dynamic Task Mapping using `expand()` to process a list of items in parallel.

## Background

Dynamic Task Mapping (DTM) allows you to create tasks at runtime based on data. Instead of defining a fixed number of tasks, you can:

- Process an unknown number of items
- Run tasks in parallel automatically
- Aggregate results from all mapped instances

### The expand() Pattern

```python
@task
def process(item: str):
    return f"Processed: {item}"

# Creates one task instance per item in the list
process.expand(item=["a", "b", "c"])  # 3 parallel tasks
```

## Requirements

Create a DAG that demonstrates the map-reduce pattern:

1. **DAG ID**: `exercise_6_1_basic_mapping`
2. **Schedule**: `None` (manual trigger)
3. **Start date**: January 1, 2024
4. **Tags**: `["exercise", "module-06", "dynamic-tasks"]`

### Tasks

1. **generate_numbers**: Generate a list of 10 random integers (1-100)

2. **square_number**: Receives a single number, returns its square
   - Use `expand()` to create parallel instances
   - Each instance processes one number

3. **sum_squares**: Receives all squared results, calculates the sum
   - This demonstrates result aggregation
   - Receives a list of all mapped task outputs

### Expected Flow

```
generate_numbers → square_number (×10 parallel) → sum_squares
     │                    │                           │
     │                    ├─ square[0]: 4² = 16       │
     │                    ├─ square[1]: 7² = 49       │
     │                    ├─ square[2]: 2² = 4        │
     │                    └─ ...                      │
     │                                                │
     └─ [4, 7, 2, ...]        [16, 49, 4, ...] ──────┘
                                                Sum = ?
```

## Key Concepts

### Expanding Over Task Output

```python
@task
def get_items():
    return [1, 2, 3]

@task
def process(item: int):
    return item * 2

# expand() works with task outputs
items = get_items()
process.expand(item=items)
```

### Aggregating Results

```python
@task
def aggregate(results: list[int]):
    # Receives ALL outputs from mapped tasks as a list
    return sum(results)

# Wire up aggregation
squared_results = square.expand(x=numbers)
aggregate(squared_results)  # Gets [result1, result2, ...]
```

## Starter Code

See `exercise_6_1_basic_mapping_starter.py`

## Testing Your DAG

```bash
# Test the DAG
airflow dags test exercise_6_1_basic_mapping 2024-01-15

# In the UI, you'll see:
# - generate_numbers: 1 instance
# - square_number: 10 instances (one per number)
# - sum_squares: 1 instance
```

## Hints

<details>
<summary>Hint 1: Generating random numbers</summary>

```python
import random

@task
def generate_numbers() -> list[int]:
    return [random.randint(1, 100) for _ in range(10)]
```

</details>

<details>
<summary>Hint 2: Using expand()</summary>

```python
@task
def square_number(x: int) -> int:
    return x ** 2

numbers = generate_numbers()
squared = square_number.expand(x=numbers)
```

</details>

<details>
<summary>Hint 3: Aggregating results</summary>

```python
@task
def sum_squares(squares: list[int]) -> int:
    total = sum(squares)
    print(f"Sum of squares: {total}")
    return total

# squared is the mapped task output - passes as list
sum_squares(squared)
```

</details>

## Success Criteria

- [ ] DAG generates 10 random numbers
- [ ] 10 parallel task instances process the numbers
- [ ] Each instance correctly squares its input
- [ ] Aggregation task receives all results as a list
- [ ] Final sum is calculated correctly
- [ ] You can view individual mapped instances in the UI

## Understanding the UI

When you run this DAG in the UI:

1. **Grid View**: Shows `square_number` as a single row, but with 10 instances
2. **Click the task**: See `[0]`, `[1]`, `[2]` etc. for each mapped instance
3. **Each instance has**: Its own logs, XCom, and status

## Real-World Applications

| Scenario | Pattern |
|----------|---------|
| Process files | expand over file list |
| API pagination | expand over page numbers |
| Database shards | expand over shard IDs |
| ML hyperparameters | expand over param combinations |
