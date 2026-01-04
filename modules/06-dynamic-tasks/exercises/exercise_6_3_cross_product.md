# Exercise 6.3: Cross-Product Processing

## Objective

Learn to use `partial()` with `expand()` for Cartesian product patterns, and `expand_kwargs()` for explicit combinations.

## Background

When you need to run tasks for every combination of multiple variables (like region × product type), Dynamic Task Mapping provides two approaches:

### Approach 1: Cartesian Product with partial() + expand()

```python
@task
def process(region: str, product: str):
    pass

# Expands to ALL combinations (3 × 2 = 6 tasks)
process.partial().expand(
    region=["us", "eu", "apac"],
    product=["widget", "gadget"]
)
```

### Approach 2: Explicit Combinations with expand_kwargs()

```python
# Define specific combinations (3 tasks, not 6)
combinations = [
    {"region": "us", "product": "widget"},
    {"region": "eu", "product": "gadget"},
    {"region": "apac", "product": "widget"},
]
process.expand_kwargs(combinations)
```

## Requirements

Create a DAG that processes regional product data:

1. **DAG ID**: `exercise_6_3_cross_product`
2. **Schedule**: `None` (manual trigger)
3. **Start date**: January 1, 2024
4. **Tags**: `["exercise", "module-06", "dynamic-tasks", "cross-product"]`

### Configuration

- **Regions**: `["us", "eu", "apac"]`
- **Products**: `["widget", "gadget", "doohickey", "thingamajig"]`
- **Total combinations**: 3 × 4 = 12 task instances

### Tasks

1. **get_config**: Return the regions and products lists

2. **process_combination**: Process a single region + product
   - Use `partial()` and `expand()` for cross-product
   - Simulate fetching and processing data
   - Return result with simulated metrics

3. **aggregate_results**: Combine all results
   - Summarize by region
   - Summarize by product
   - Calculate totals

### Expected Flow

```
get_config → process_combination (×12) → aggregate_results
                     │
                     ├─ [0]: us + widget
                     ├─ [1]: us + gadget
                     ├─ [2]: us + doohickey
                     ├─ [3]: us + thingamajig
                     ├─ [4]: eu + widget
                     ├─ [5]: eu + gadget
                     ├─ ...
                     └─ [11]: apac + thingamajig
```

## Key Concepts

### Using partial() with expand()

```python
@task
def process(region: str, product: str, mode: str = "full"):
    pass

# partial() fixes some args, expand() varies others
process.partial(
    mode="incremental"  # Same for all
).expand(
    region=["us", "eu", "apac"],
    product=["a", "b"]  # Creates 6 combinations
)
```

### Understanding Cartesian Product

When expanding multiple arguments:
- `expand(a=[1,2], b=[x,y])` creates: `(1,x), (1,y), (2,x), (2,y)`
- Order: First varies slowest, last varies fastest

### Using expand_kwargs() for Explicit Control

```python
# When you don't want all combinations
combos = [
    {"region": "us", "product": "premium"},  # Only some regions
    {"region": "eu", "product": "premium"},  # get premium product
]
process.expand_kwargs(combos)
```

## Starter Code

See `exercise_6_3_cross_product_starter.py`

## Testing Your DAG

```bash
# Test the DAG
airflow dags test exercise_6_3_cross_product 2024-01-15

# You should see 12 parallel task instances
# Each with a unique region + product combination
```

## Hints

<details>
<summary>Hint 1: Returning config</summary>

```python
@task
def get_config() -> dict:
    return {
        "regions": ["us", "eu", "apac"],
        "products": ["widget", "gadget", "doohickey", "thingamajig"],
    }
```

</details>

<details>
<summary>Hint 2: Cross-product expansion</summary>

```python
config = get_config()

# Extract lists from config
# Then use partial().expand() for cross-product
process_combination.partial().expand(
    region=config["regions"],
    product=config["products"]
)

# Or expand from separate upstream tasks
# regions = get_regions()
# products = get_products()
# process.expand(region=regions, product=products)
```

</details>

<details>
<summary>Hint 3: Processing combination</summary>

```python
@task
def process_combination(region: str, product: str) -> dict:
    import random

    return {
        "region": region,
        "product": product,
        "sales": random.randint(1000, 50000),
        "units": random.randint(100, 5000),
        "status": "processed"
    }
```

</details>

<details>
<summary>Hint 4: Aggregating by dimension</summary>

```python
@task
def aggregate_results(results: list[dict]) -> dict:
    # Group by region
    by_region = {}
    for r in results:
        region = r["region"]
        by_region[region] = by_region.get(region, 0) + r["sales"]

    # Group by product
    by_product = {}
    for r in results:
        product = r["product"]
        by_product[product] = by_product.get(product, 0) + r["sales"]

    return {
        "by_region": by_region,
        "by_product": by_product,
        "total": sum(r["sales"] for r in results)
    }
```

</details>

## Success Criteria

- [ ] DAG creates exactly 12 task instances (3 × 4)
- [ ] Each combination is processed once
- [ ] Results are correctly aggregated by region
- [ ] Results are correctly aggregated by product
- [ ] You understand partial() vs expand_kwargs()
- [ ] You can predict task count from input dimensions

## When to Use Each Pattern

| Pattern | Use Case |
|---------|----------|
| `expand()` with single arg | Process list of items |
| `partial().expand()` multi-arg | All combinations needed |
| `expand_kwargs()` | Specific combinations only |

## Extension Challenge

Try both approaches:

1. **Cartesian product** (current exercise): All 12 combinations
2. **Selective combinations** with `expand_kwargs()`: Only 5 specific pairs

```python
# Only process premium products in select regions
selective_combos = [
    {"region": "us", "product": "widget"},
    {"region": "us", "product": "gadget"},
    {"region": "eu", "product": "widget"},
    {"region": "apac", "product": "thingamajig"},
    {"region": "apac", "product": "doohickey"},
]
process.expand_kwargs(selective_combos)
```
