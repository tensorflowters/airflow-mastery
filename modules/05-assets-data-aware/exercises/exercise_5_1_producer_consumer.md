# Exercise 5.1: Basic Producer/Consumer

## Objective

Create a two-DAG pipeline demonstrating the fundamental Asset producer/consumer pattern in Airflow 3.x.

## Background

Assets (formerly Datasets in Airflow 2.x) represent logical data entities that trigger DAG runs when updated. This enables data-aware scheduling where downstream DAGs automatically run when their input data becomes available.

### Key Concepts

| Concept | Description |
|---------|-------------|
| **Asset** | A logical data reference identified by URI |
| **Producer** | A task with `outlets=[asset]` that signals data availability |
| **Consumer** | A DAG with `schedule=[asset]` that triggers on updates |
| **URI** | A unique identifier string (doesn't need to be a real URL) |

### How It Works

```
┌─────────────────┐              ┌─────────────────┐
│   Producer DAG  │    Asset     │  Consumer DAG   │
│                 │   Update     │                 │
│ @task(outlets=  │ ──────────►  │ schedule=[      │
│   [my_asset])   │              │   my_asset]     │
└─────────────────┘              └─────────────────┘
```

## Requirements

### DAG 1: data_producer

1. DAG ID: `exercise_5_1_producer`
2. Schedule: `@hourly` (or cron equivalent)
3. Start date: January 1, 2024
4. Tags: `["exercise", "module-05", "assets", "producer"]`
5. Contains a task that:
   - Simulates data processing
   - Produces an Asset with URI `s3://data-lake/processed_data`
   - Logs the processing timestamp

### DAG 2: data_consumer

1. DAG ID: `exercise_5_1_consumer`
2. Schedule: Triggered by the `processed_data` Asset
3. Start date: January 1, 2024
4. Tags: `["exercise", "module-05", "assets", "consumer"]`
5. Contains a task that:
   - Prints "Data is available for processing!"
   - Logs which Asset triggered the run
   - Accesses triggering event information

## Key Code Patterns

### Defining an Asset

```python
from airflow.sdk import Asset

# Asset URIs are logical identifiers
my_asset = Asset("s3://bucket/path/to/data")
```

### Producer Task

```python
@task(outlets=[my_asset])
def produce_data():
    # Process data...
    return {"status": "complete"}
```

### Consumer DAG Schedule

```python
@dag(
    schedule=[my_asset],  # Triggers when asset is updated
)
def consumer_dag():
    pass
```

### Accessing Triggering Events

```python
@task
def process_data(**context):
    triggering_asset_events = context.get("triggering_asset_events", {})
    for asset, events in triggering_asset_events.items():
        print(f"Triggered by: {asset.uri}")
```

## Starter Code

See `exercise_5_1_producer_consumer_starter.py`

## Testing Your DAGs

```bash
# 1. First, ensure both DAGs are loaded
airflow dags list | grep exercise_5_1

# 2. Trigger the producer DAG
airflow dags trigger exercise_5_1_producer

# 3. Check if consumer was triggered
airflow dags list-runs -d exercise_5_1_consumer

# 4. View Asset in the UI: DAGs → Assets
```

## Hints

<details>
<summary>Hint 1: Defining the Asset</summary>

```python
from airflow.sdk import Asset

# Define the Asset that connects producer and consumer
processed_data = Asset("s3://data-lake/processed_data")
```

Both DAGs must reference the SAME Asset (same URI).

</details>

<details>
<summary>Hint 2: Producer task with outlets</summary>

```python
@task(outlets=[processed_data])
def produce_data():
    print("Processing data...")
    return {"timestamp": datetime.now().isoformat()}
```

The `outlets` parameter signals that this task produces the Asset.

</details>

<details>
<summary>Hint 3: Consumer DAG schedule</summary>

```python
@dag(
    dag_id="exercise_5_1_consumer",
    schedule=[processed_data],  # List of Assets to watch
    start_date=datetime(2024, 1, 1),
)
def consumer_dag():
    pass
```

</details>

<details>
<summary>Hint 4: Accessing triggering event info</summary>

```python
@task
def consume_data(**context):
    events = context.get("triggering_asset_events", {})
    for asset, event_list in events.items():
        print(f"Asset: {asset.uri}")
        for event in event_list:
            print(f"  Timestamp: {event.timestamp}")
```

</details>

## Success Criteria

- [ ] Both DAGs are registered and visible in the UI
- [ ] Producer DAG runs on schedule and produces the Asset
- [ ] Consumer DAG automatically triggers when producer completes
- [ ] Consumer task can access triggering event information
- [ ] You can see the Asset and its relationships in the Assets view
- [ ] You understand the producer/consumer pattern

## Real-World Applications

| Use Case | Producer | Consumer |
|----------|----------|----------|
| ETL Pipeline | Data extraction DAG | Data transformation DAG |
| ML Pipeline | Feature engineering DAG | Model training DAG |
| Reporting | Data aggregation DAG | Report generation DAG |
| Data Quality | Ingestion DAG | Validation DAG |
