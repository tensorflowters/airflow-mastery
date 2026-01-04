# Exercise 14.1: Pool Configuration

## Objective

Learn to configure and manage Airflow pools for controlling concurrent access to shared resources.

## Background

Pools are essential for:
- Limiting database connections
- Respecting API rate limits
- Managing memory-intensive tasks
- Preventing resource exhaustion

## Requirements

### Part 1: Pool Manager

Create a pool management utility:

```python
class PoolManager:
    def create_pool(self, name: str, slots: int, description: str) -> bool:
        """Create or update a pool."""
        pass

    def delete_pool(self, name: str) -> bool:
        """Delete a pool."""
        pass

    def get_pool_status(self, name: str) -> dict:
        """Get pool utilization status."""
        pass

    def list_pools(self) -> list:
        """List all pools with status."""
        pass
```

### Part 2: Pool-Aware DAG

Create a DAG that uses pools effectively:

```python
@dag(...)
def pool_aware_etl():
    # Tasks using different pools
    # - database_pool: for DB queries
    # - api_pool: for external API calls
    # - compute_pool: for heavy computation
```

### Part 3: Dynamic Pool Sizing

Implement dynamic pool slot allocation:

```python
class DynamicPoolManager:
    def adjust_pool_size(self, name: str, current_load: float) -> int:
        """Adjust pool size based on system load."""
        pass

    def scale_pool(self, name: str, factor: float) -> int:
        """Scale pool by factor."""
        pass
```

## Starter Code

See `exercise_14_1_pool_configuration_starter.py`

## Hints

<details>
<summary>Hint 1: Pool creation</summary>

```python
from airflow.models import Pool
from airflow.utils.session import create_session

def create_pool(name, slots, description):
    with create_session() as session:
        pool = Pool(pool=name, slots=slots, description=description)
        session.merge(pool)  # merge handles insert/update
        session.commit()
```

</details>

<details>
<summary>Hint 2: Pool slots in tasks</summary>

```python
@task(pool="my_pool", pool_slots=2)
def heavy_task():
    """Uses 2 slots from the pool."""
    pass
```

</details>

## Success Criteria

- [ ] Pool manager creates pools correctly
- [ ] Pool status reflects actual usage
- [ ] DAG tasks respect pool limits
- [ ] Dynamic sizing adjusts appropriately

---

Next: [Exercise 14.2: Priority Weights →](exercise_14_2_priority_weights.md)
