# Module 14: Resource Management

Master resource management in Airflow 3.x including pools, priorities, queues, and concurrency controls.

## Learning Objectives

By the end of this module, you will:
- Understand and configure Airflow pools
- Implement priority weights for task scheduling
- Manage concurrency at DAG and task levels
- Use queues for worker distribution
- Apply best practices for resource optimization

## Prerequisites

- Module 01: Foundations
- Module 04: Scheduling
- Understanding of task execution

## Resource Management Overview

### Why Resource Management?

```
Without Resource Management:
┌─────────────────────────────────────────────┐
│ 100 tasks all start simultaneously          │
│ Database overwhelmed, API rate limited      │
│ Memory exhausted, workers crash             │
└─────────────────────────────────────────────┘

With Resource Management:
┌─────────────────────────────────────────────┐
│ 10 tasks at a time (pool limit)             │
│ Critical tasks run first (priority)         │
│ Heavy tasks on dedicated workers (queues)   │
└─────────────────────────────────────────────┘
```

### Resource Control Points

| Control | Scope | Purpose |
|---------|-------|---------|
| Pools | Task | Limit concurrent access to shared resources |
| Priority Weight | Task | Order task execution queue |
| Concurrency | DAG/Task | Limit parallel execution |
| Queues | Worker | Route tasks to specific workers |

## Pools

### What Are Pools?

Pools limit concurrent task execution for shared resources:

```python
from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="pool_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
)
def pool_example():

    @task(pool="database_connections", pool_slots=1)
    def query_database():
        """Uses 1 slot from the database_connections pool."""
        pass

    @task(pool="database_connections", pool_slots=2)
    def heavy_etl():
        """Uses 2 slots from the database_connections pool."""
        pass
```

### Pool Configuration

```python
# Create pools programmatically
from airflow.models import Pool
from airflow.utils.session import create_session

def create_pools():
    pools = [
        Pool(pool="database_connections", slots=10, description="Database connection limit"),
        Pool(pool="api_calls", slots=5, description="External API rate limit"),
        Pool(pool="memory_intensive", slots=3, description="High memory tasks"),
    ]

    with create_session() as session:
        for pool in pools:
            existing = session.query(Pool).filter_by(pool=pool.pool).first()
            if not existing:
                session.add(pool)
        session.commit()
```

### Pool Slots

```python
@task(pool="api_calls", pool_slots=1)
def light_api_call():
    """Single API call - uses 1 slot."""
    pass

@task(pool="api_calls", pool_slots=3)
def batch_api_calls():
    """Batch of API calls - reserves 3 slots."""
    pass
```

### Pool Monitoring

```python
from airflow.models import Pool

def get_pool_status(pool_name: str) -> dict:
    """Get current pool utilization."""
    pool = Pool.get_pool(pool_name)
    return {
        "name": pool.pool,
        "total_slots": pool.slots,
        "running_slots": pool.running_slots(),
        "queued_slots": pool.queued_slots(),
        "open_slots": pool.open_slots(),
    }
```

### Common Pool Patterns

#### Database Connection Pool

```python
# Limit database connections
@task(pool="postgres_connections", pool_slots=1)
def run_query(query: str):
    """Each query uses one connection."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook()
    return hook.get_records(query)
```

#### API Rate Limiting Pool

```python
# Respect external API rate limits
@task(pool="external_api", pool_slots=1)
def call_api(endpoint: str):
    """Rate limited API calls."""
    import requests
    return requests.get(f"https://api.example.com/{endpoint}").json()
```

#### Resource-Intensive Tasks

```python
# Limit memory-heavy tasks
@task(pool="high_memory", pool_slots=2)
def process_large_file(file_path: str):
    """Memory intensive - uses 2 slots."""
    import pandas as pd
    df = pd.read_csv(file_path)
    return df.describe().to_dict()
```

## Priority Weight

### Task Priority

Higher priority weight = runs sooner:

```python
@task(priority_weight=10)
def critical_task():
    """Runs before lower priority tasks."""
    pass

@task(priority_weight=1)
def normal_task():
    """Standard priority."""
    pass

@task(priority_weight=0)
def background_task():
    """Runs last."""
    pass
```

### Priority Weight Rules

```python
from airflow.utils.weight_rule import WeightRule

@dag(
    dag_id="priority_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
)
def priority_example():

    # Downstream: Priority = sum of downstream task weights
    @task(priority_weight=5, weight_rule=WeightRule.DOWNSTREAM)
    def upstream_task():
        pass

    # Upstream: Priority = sum of upstream task weights
    @task(priority_weight=3, weight_rule=WeightRule.UPSTREAM)
    def downstream_task():
        pass

    # Absolute: Use exact weight specified
    @task(priority_weight=10, weight_rule=WeightRule.ABSOLUTE)
    def fixed_priority_task():
        pass
```

### Priority Calculation Example

```
DAG Structure:
    A (weight=1)
    ├── B (weight=2)
    │   └── D (weight=1)
    └── C (weight=3)
        └── D (weight=1)

With DOWNSTREAM rule:
- A: 1 + (2+1) + (3+1) = 8
- B: 2 + 1 = 3
- C: 3 + 1 = 4
- D: 1

Execution order: A → C → B → D
```

## Concurrency Controls

### DAG Concurrency

```python
@dag(
    dag_id="concurrency_example",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    max_active_runs=3,  # Max 3 DAG runs at once
)
def concurrency_example():

    @task(max_active_tis_per_dag=5)  # Max 5 instances across all runs
    def limited_task():
        pass
```

### Task Concurrency

```python
@task(
    max_active_tis_per_dag=10,     # Max 10 across all DAG runs
    max_active_tis_per_dagrun=2,  # Max 2 per single DAG run
)
def controlled_concurrency_task():
    """Carefully controlled concurrency."""
    pass
```

### Global Concurrency Settings

```ini
# airflow.cfg
[core]
# Maximum active tasks across all DAGs
parallelism = 32

# Maximum active tasks per DAG
max_active_tasks_per_dag = 16

# Maximum active DAG runs per DAG
max_active_runs_per_dag = 16
```

### Dynamic Concurrency

```python
from airflow.models import Variable

def get_dynamic_concurrency():
    """Get concurrency based on current load."""
    base = int(Variable.get("base_concurrency", 5))
    load = float(Variable.get("current_load", 0.5))

    # Reduce concurrency under high load
    if load > 0.8:
        return max(1, base // 2)
    return base
```

## Queues

### Worker Queues

Route tasks to specific Celery workers:

```python
@task(queue="default")
def standard_task():
    """Runs on default workers."""
    pass

@task(queue="high_memory")
def memory_task():
    """Runs on high-memory workers."""
    pass

@task(queue="gpu")
def ml_training():
    """Runs on GPU workers."""
    pass
```

### Worker Configuration

```bash
# Start worker for specific queue
airflow celery worker -q default,high_priority

# Start specialized worker
airflow celery worker -q gpu --concurrency 2

# Start high-memory worker
airflow celery worker -q high_memory --concurrency 4
```

### Queue-Based Scaling

```yaml
# docker-compose.yml
services:
  worker-default:
    command: airflow celery worker -q default
    deploy:
      replicas: 4

  worker-heavy:
    command: airflow celery worker -q heavy
    deploy:
      resources:
        limits:
          memory: 16G
      replicas: 2

  worker-gpu:
    command: airflow celery worker -q gpu
    deploy:
      resources:
        reservations:
          devices:
            - capabilities: [gpu]
      replicas: 1
```

## Resource Management Patterns

### Pattern 1: Tiered Priority

```python
class Priority:
    CRITICAL = 100
    HIGH = 75
    NORMAL = 50
    LOW = 25
    BACKGROUND = 1

@task(priority_weight=Priority.CRITICAL)
def alert_check():
    """Must run immediately."""
    pass

@task(priority_weight=Priority.LOW)
def report_generation():
    """Can wait."""
    pass
```

### Pattern 2: Resource Pools by Tier

```python
POOLS = {
    "tier1_critical": {"slots": 20, "description": "Production critical"},
    "tier2_standard": {"slots": 10, "description": "Standard workloads"},
    "tier3_batch": {"slots": 5, "description": "Batch processing"},
}

@task(pool="tier1_critical", priority_weight=100)
def production_etl():
    pass

@task(pool="tier3_batch", priority_weight=10)
def historical_backfill():
    pass
```

### Pattern 3: Hybrid Pool + Queue

```python
@task(
    pool="database_connections",
    pool_slots=2,
    queue="high_memory",
    priority_weight=50,
)
def complex_etl():
    """
    - Uses 2 database connection slots
    - Runs on high-memory workers
    - Medium priority
    """
    pass
```

### Pattern 4: SLA-Based Prioritization

```python
from datetime import timedelta

@task(
    priority_weight=100,
    sla=timedelta(minutes=30),
)
def sla_critical_task():
    """Must complete within 30 minutes."""
    pass

def sla_miss_callback(context):
    """Alert when SLA is missed."""
    task = context['task_instance']
    # Send alert
    send_alert(f"SLA missed for {task.task_id}")
```

## Best Practices

### Pool Design

```python
# Good: Specific, purpose-driven pools
POOLS = {
    "postgres_analytics": 5,   # Analytics DB connections
    "postgres_production": 10, # Production DB connections
    "s3_downloads": 20,        # S3 download bandwidth
    "api_partner_x": 3,        # Partner X rate limit
}

# Bad: Generic, overlapping pools
BAD_POOLS = {
    "database": 15,   # Which database?
    "external": 10,   # Too vague
    "limited": 5,     # Limited by what?
}
```

### Priority Strategy

```python
# Establish clear priority tiers
PRIORITY_TIERS = {
    # Tier 1: Business Critical (90-100)
    "revenue_impacting": 100,
    "customer_facing": 95,
    "sla_bound": 90,

    # Tier 2: Operational (60-80)
    "monitoring": 80,
    "reporting": 70,
    "analytics": 60,

    # Tier 3: Background (1-40)
    "archival": 40,
    "cleanup": 20,
    "experimental": 1,
}
```

### Monitoring Resources

```python
from airflow.models import Pool, TaskInstance
from airflow.utils.state import TaskInstanceState

def get_resource_utilization() -> dict:
    """Get current resource utilization."""
    pools = Pool.get_pools()

    utilization = {}
    for pool in pools:
        if pool.slots > 0:
            running = pool.running_slots()
            utilization[pool.pool] = {
                "slots": pool.slots,
                "running": running,
                "utilization": running / pool.slots * 100,
            }

    return utilization
```

## Exercises

### Exercise 14.1: Pool Configuration
Configure and manage pools for different resource types.

[Start Exercise 14.1 →](exercises/exercise_14_1_pool_configuration.md)

### Exercise 14.2: Priority Weights
Implement priority-based task scheduling.

[Start Exercise 14.2 →](exercises/exercise_14_2_priority_weights.md)

### Exercise 14.3: Concurrency Limits
Manage concurrency at multiple levels.

[Start Exercise 14.3 →](exercises/exercise_14_3_concurrency_limits.md)

## Key Takeaways

1. **Pools** prevent resource exhaustion by limiting concurrent access
2. **Priority weights** ensure critical tasks run first
3. **Concurrency limits** prevent system overload
4. **Queues** route tasks to appropriate workers
5. **Combine controls** for fine-grained resource management

## Next Steps

You've completed all core modules! Consider:
- [Example DAGs](../../dags/examples/) for real-world patterns
- [Production Patterns](../09-production-patterns/) for deployment
- [Advanced Topics](../10-advanced-topics/) for deeper exploration

## Additional Resources

- [Airflow Pools Documentation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html)
- [Priority Weights](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/priority-weight.html)
- [Executor Configuration](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/index.html)
