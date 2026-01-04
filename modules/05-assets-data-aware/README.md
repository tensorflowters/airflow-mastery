# Module 05: Assets & Data-Aware Scheduling

## 🎯 Learning Objectives

By the end of this module, you will:

- Understand Assets (formerly Datasets) and data-aware scheduling
- Create DAGs that trigger based on data availability
- Use the `@asset` decorator for asset-centric workflows
- Implement Asset Watchers for external event monitoring
- Design multi-DAG pipelines with data dependencies

## ⏱️ Estimated Time: 4-5 hours

---

## 1. What Are Assets?

**Assets** represent logical data entities that can trigger DAG runs when updated. Instead of time-based scheduling, DAGs can be triggered when their input data becomes available.

### Key Concepts

| Term         | Definition                                          |
| ------------ | --------------------------------------------------- |
| **Asset**    | A logical data reference (URI) that represents data |
| **Producer** | A task that creates/updates an Asset                |
| **Consumer** | A DAG that runs when Assets are updated             |
| **Outlet**   | An Asset that a task produces                       |

### Naming Change (Airflow 2.x → 3)

```python
# Airflow 2.x

# Airflow 3
```

---

## 2. Basic Asset Usage

### Defining an Asset

```python
from airflow.sdk import Asset

# Assets are identified by URI
users_table = Asset("postgres://warehouse/users")
daily_report = Asset("s3://reports/daily/{{ ds }}.csv")
api_data = Asset("https://api.example.com/data")

# URIs can be any string - Airflow doesn't validate them
# They're logical identifiers, not actual connections
```

### Producer DAG

```python
from datetime import datetime

from airflow.sdk import DAG, Asset, task

# Define the Asset this DAG produces
users_asset = Asset("postgres://warehouse/users")

with DAG(
    dag_id="etl_users",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
):

    @task(outlets=[users_asset])  # Marks this task as producing the Asset
    def load_users():
        # Your ETL logic here
        print("Loading users to warehouse...")
        return {"rows_loaded": 1000}
```

### Consumer DAG

```python
from datetime import datetime

from airflow.sdk import DAG, Asset, task

# Reference the same Asset
users_asset = Asset("postgres://warehouse/users")

with DAG(
    dag_id="report_users",
    schedule=[users_asset],  # Trigger when Asset is updated!
    start_date=datetime(2024, 1, 1),
):

    @task
    def generate_report():
        print("Generating user report...")
```

### The Flow

```
┌─────────────────┐           ┌─────────────────┐
│   etl_users     │           │  report_users   │
│   (Producer)    │           │  (Consumer)     │
│                 │           │                 │
│  @daily         │  triggers │  schedule=[     │
│  ───────────►   │  ──────►  │    users_asset] │
│  outlets=[      │           │                 │
│    users_asset] │           │                 │
└─────────────────┘           └─────────────────┘
```

---

## 3. Multiple Asset Dependencies

### Waiting for Multiple Assets

```python
from airflow.sdk import DAG, Asset, task

# Define multiple Assets
orders_asset = Asset("postgres://warehouse/orders")
products_asset = Asset("postgres://warehouse/products")
customers_asset = Asset("postgres://warehouse/customers")

with DAG(
    dag_id="sales_analytics",
    schedule=[orders_asset, products_asset, customers_asset],
    # Triggers only when ALL three are updated
):

    @task
    def run_analytics():
        print("All data available, running analytics...")
```

### Logical Operators (AND/OR)

```python
from airflow.sdk import Asset

# Default: AND logic (all must be updated)
schedule = [asset_a, asset_b]  # Wait for both

# OR logic: trigger when ANY is updated
schedule = Asset.any(asset_a, asset_b)  # Wait for either

# Complex combinations
schedule = Asset.all(asset_a, Asset.any(asset_b, asset_c))  # a AND (b OR c)
```

---

## 4. The @asset Decorator (Airflow 3)

Airflow 3 introduces `@asset` for a more Pythonic, asset-centric approach:

```python
from airflow.sdk import asset


# Define an Asset that's also its own producer
@asset(schedule="@daily", uri="s3://data-lake/processed/users")
def processed_users():
    """This function IS the asset AND the task that produces it"""
    raw_data = fetch_raw_users()
    cleaned = clean_data(raw_data)
    save_to_s3(cleaned, "s3://data-lake/processed/users")
    return cleaned


# Another asset that depends on the first
@asset(
    schedule=[processed_users],  # Triggered by processed_users
    uri="s3://data-lake/analytics/user_metrics",
)
def user_metrics(processed_users):  # Receives upstream asset
    """Computes metrics from processed users"""
    metrics = compute_metrics(processed_users)
    save_to_s3(metrics, "s3://data-lake/analytics/user_metrics")
    return metrics
```

### Benefits of @asset

1. **Single definition**: Asset and its producer in one place
2. **Clear dependencies**: Function parameters show data flow
3. **Automatic scheduling**: No separate DAG definition needed
4. **Self-documenting**: Code structure matches data flow

---

## 5. Asset Watchers

Asset Watchers monitor external systems and trigger Assets without running tasks:

```python
from airflow.sdk import Asset, AssetWatcher

# Define an Asset
external_file = Asset("s3://partner-data/daily-feed")

# Create a watcher that monitors for updates
watcher = AssetWatcher(
    asset=external_file,
    trigger_type="s3",  # Built-in S3 watcher
    trigger_kwargs={
        "bucket": "partner-data",
        "prefix": "daily-feed",
        "aws_conn_id": "aws_default",
    },
)

# Consumer DAG
with DAG(
    dag_id="process_partner_data",
    schedule=[external_file],
):

    @task
    def process():
        print("Partner data arrived, processing...")
```

### SQS Integration (Airflow 3.1+)

```python
from airflow.providers.amazon.aws.triggers.sqs import SqsAssetTrigger
from airflow.sdk import Asset

# Asset triggered by SQS messages
sqs_triggered_asset = Asset(
    uri="events://order-events",
    watchers=[
        SqsAssetTrigger(
            sqs_queue="order-events-queue",
            aws_conn_id="aws_default",
        )
    ],
)
```

---

## 6. Asset Aliases and Patterns

### Asset Aliases

Group related Assets under a single identifier:

```python
from airflow.sdk import Asset, AssetAlias

# Individual Assets
raw_orders = Asset("s3://raw/orders")
raw_products = Asset("s3://raw/products")
raw_customers = Asset("s3://raw/customers")

# Alias for all raw data
all_raw_data = AssetAlias("raw-data-complete")


# Producer marks alias as complete
@task(outlets=[all_raw_data])
def mark_raw_complete():
    """Signal that all raw data is loaded"""
    pass


# Consumer waits on alias
with DAG(schedule=[all_raw_data]):
    ...
```

### Dynamic Assets

```python
from airflow.sdk import Asset


def get_partition_asset(date: str) -> Asset:
    """Create date-partitioned Asset"""
    return Asset(f"s3://data/partitions/{date}")


# In a task
@task(outlets=[get_partition_asset("{{ ds }}")])
def process_partition(**context):
    date = context["ds"]
    # Process partition for this date
    ...
```

---

## 7. Viewing Assets in the UI

Airflow 3's new UI has dedicated Asset views:

1. **Assets View**: See all defined Assets
2. **Asset Details**: View producers and consumers
3. **Lineage Graph**: Visualize data flow across DAGs
4. **Update History**: Track when Assets were updated

Navigate: **DAGs** → **Assets** in the top navigation

---

## 8. Best Practices

### DO ✅

```python
# Use descriptive, hierarchical URIs
Asset("s3://data-lake/bronze/orders/v1")
Asset("postgres://warehouse.analytics/user_metrics")

# Document Assets
orders_asset = Asset(uri="s3://data-lake/orders", extra={"owner": "data-team", "sla_hours": 2})

# Keep producer/consumer relationships clean
# One clear producer per Asset when possible
```

### DON'T ❌

```python
# Don't use vague URIs
Asset("data")  # Bad - not descriptive

# Don't have multiple uncoordinated producers
# (Can lead to unexpected triggers)

# Don't use Assets for fine-grained triggering
# (Not a replacement for message queues)
```

---

## 📝 Exercises

### Exercise 5.1: Basic Producer/Consumer

Create a two-DAG pipeline:

1. `data_producer`: Runs hourly, produces a "processed_data" Asset
2. `data_consumer`: Triggers when Asset is updated, prints a message

Test by manually triggering the producer.

### Exercise 5.2: Multi-Asset Dependencies

Create a pipeline where:

1. Three producer DAGs each update their own Asset
2. A consumer DAG only runs when ALL three are updated
3. Add logging to see which triggered

### Exercise 5.3: @asset Pattern

Rewrite a traditional ETL DAG using `@asset`:

- Extract raw data (Asset A)
- Transform data (Asset B, depends on A)
- Load to warehouse (Asset C, depends on B)

---

## ✅ Checkpoint

Before moving to Module 06, ensure you can:

- [ ] Define and reference Assets by URI
- [ ] Create producer tasks with `outlets`
- [ ] Create consumer DAGs with `schedule=[asset]`
- [ ] Use multiple Asset dependencies (AND/OR)
- [ ] Understand the @asset decorator pattern
- [ ] Navigate Asset views in the UI

---

## 🏭 Industry Spotlight: Uber

**How Uber Uses Asset-Driven ML Retraining**

Uber's machine learning platform processes billions of events daily to power features like surge pricing, ETA predictions, and driver matching. Asset-driven scheduling ensures models retrain only when necessary:

| Challenge                   | Asset Solution                                  |
| --------------------------- | ----------------------------------------------- |
| **Feature freshness**       | Feature stores emit Assets when updated         |
| **Training efficiency**     | Models only retrain when dependencies change    |
| **Cross-team coordination** | Clear Asset contracts between data and ML teams |
| **Cost optimization**       | No unnecessary training runs                    |

**Pattern in Use**: Uber-style ML retraining with Assets:

```python
from airflow.sdk import Asset, dag, task

# Feature store assets
ride_features = Asset("feature-store://rides/v3")
driver_features = Asset("feature-store://drivers/v3")


# Model retrains when BOTH feature sets update
@dag(schedule=[ride_features, driver_features])
def retrain_eta_model():
    @task
    def train_model():
        """Only runs when fresh features are available."""
        features = load_features(["rides/v3", "drivers/v3"])
        model = train_eta_predictor(features)
        deploy_model(model)
```

**Key Insight**: Asset-driven scheduling reduced Uber's unnecessary ML training runs by 60%, saving significant compute costs while maintaining model freshness.

📖 **Related Exercise**: [Exercise 5.4: Embedding Assets](exercises/exercise_5_4_embedding_assets.md) - Build Asset-driven AI/ML pipelines

---

## 📚 Further Reading

- [Assets Documentation](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html)
- [Data-Aware Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/howto/dataset-triggered-dags.html)
- [Case Study: Spotify Recommendations](../../docs/case-studies/spotify-recommendations.md)

---

Next: [Module 06: Dynamic Task Mapping →](../06-dynamic-tasks/README.md)
