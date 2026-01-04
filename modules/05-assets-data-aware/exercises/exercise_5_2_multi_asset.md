# Exercise 5.2: Multi-Asset Dependencies

## Objective

Create a pipeline with multiple producer DAGs and a consumer that waits for ALL data sources to be ready before processing.

## Background

Real-world data pipelines often need data from multiple sources before processing can begin. Airflow's Asset system supports multi-Asset dependencies with both AND and OR logic.

### Multi-Asset Behavior

```python
# AND logic (default) - waits for ALL Assets
schedule = [asset_a, asset_b, asset_c]

# OR logic - triggers when ANY is updated
schedule = Asset.any(asset_a, asset_b, asset_c)

# Complex combinations
schedule = Asset.all(asset_a, Asset.any(asset_b, asset_c))
```

## Requirements

### Three Producer DAGs

Create three separate producer DAGs:

1. **orders_producer**
   - DAG ID: `exercise_5_2_orders_producer`
   - Asset: `postgres://warehouse/orders`
   - Schedule: `None` (manual trigger)

2. **products_producer**
   - DAG ID: `exercise_5_2_products_producer`
   - Asset: `postgres://warehouse/products`
   - Schedule: `None` (manual trigger)

3. **customers_producer**
   - DAG ID: `exercise_5_2_customers_producer`
   - Asset: `postgres://warehouse/customers`
   - Schedule: `None` (manual trigger)

### Analytics Consumer DAG

1. DAG ID: `exercise_5_2_analytics_consumer`
2. Schedule: All three Assets (AND logic)
3. Contains a task that:
   - Logs which Assets triggered the run
   - Simulates running analytics on the combined data
   - Reports completion status

## Key Concepts

### Default AND Logic

```python
# Consumer runs only when ALL Assets are updated
@dag(
    schedule=[orders_asset, products_asset, customers_asset],
)
```

### Accessing Multiple Triggering Events

```python
@task
def analyze(**context):
    events = context.get("triggering_asset_events", {})
    for asset, event_list in events.items():
        print(f"Asset: {asset.uri} - {len(event_list)} events")
```

## Starter Code

See `exercise_5_2_multi_asset_starter.py`

## Testing Your DAGs

```bash
# 1. Trigger each producer one at a time
airflow dags trigger exercise_5_2_orders_producer
airflow dags trigger exercise_5_2_products_producer

# 2. Consumer should NOT run yet (still waiting for customers)

# 3. Trigger the last producer
airflow dags trigger exercise_5_2_customers_producer

# 4. Now the consumer should trigger!
airflow dags list-runs -d exercise_5_2_analytics_consumer

# 5. View Asset dependencies in UI: DAGs → Assets
```

## Hints

<details>
<summary>Hint 1: Defining multiple Assets</summary>

```python
from airflow.sdk import Asset

orders_asset = Asset("postgres://warehouse/orders")
products_asset = Asset("postgres://warehouse/products")
customers_asset = Asset("postgres://warehouse/customers")
```

</details>

<details>
<summary>Hint 2: Consumer with multiple Asset dependencies</summary>

```python
@dag(
    dag_id="exercise_5_2_analytics_consumer",
    schedule=[orders_asset, products_asset, customers_asset],
    # Triggers only when ALL three are updated
)
def analytics_consumer():
    pass
```

</details>

<details>
<summary>Hint 3: Iterating over triggering events</summary>

```python
@task
def run_analytics(**context):
    events = context.get("triggering_asset_events", {})

    print("Triggered by these Assets:")
    for asset, event_list in events.items():
        print(f"  - {asset.uri}")
        for event in event_list:
            print(f"      Updated at: {event.timestamp}")
            print(f"      By: {event.source_dag_id}/{event.source_task_id}")
```

</details>

## Success Criteria

- [ ] Three producer DAGs are registered
- [ ] Consumer DAG is registered with multi-Asset schedule
- [ ] Consumer does NOT run until ALL three Assets are updated
- [ ] Consumer correctly logs all triggering events
- [ ] You understand AND vs OR logic for Asset dependencies
- [ ] Assets view shows the dependency graph

## Challenge Extension

Try modifying the consumer to use OR logic instead:

```python
from airflow.sdk import Asset

# Consumer triggers when ANY Asset is updated
@dag(
    schedule=Asset.any(orders_asset, products_asset, customers_asset),
)
def quick_update_consumer():
    pass
```

## Real-World Applications

| Scenario | Assets | Behavior |
|----------|--------|----------|
| Daily Report | orders + products + customers | Wait for all (AND) |
| Real-time Alert | any data change | Trigger on any (OR) |
| Data Quality | raw_data + validation_rules | Wait for both (AND) |
| ML Training | features + labels | Wait for both (AND) |
