# Module 06: Dynamic Task Mapping

## 🎯 Learning Objectives

By the end of this module, you will:

- Create dynamic tasks that expand at runtime
- Use `expand()` for parallel processing over collections
- Combine `partial()` with `expand()` for complex patterns
- Aggregate results from mapped tasks
- Handle real-world scenarios like file processing and API pagination

## ⏱️ Estimated Time: 4-5 hours

---

## 1. Why Dynamic Tasks?

Sometimes you don't know at DAG authoring time how many tasks you need:

- Process each file in a directory (unknown count)
- Call API for each item in a list
- Run analysis for each partition
- Process each record from a query

**Before Airflow 2.3**: You had to use hacky workarounds or know counts upfront.
**Now**: Dynamic Task Mapping (DTM) handles this elegantly.

---

## 2. Basic expand() Usage

### Simple Expansion

```python
from datetime import datetime

from airflow.sdk import DAG, task

with DAG(dag_id="dynamic_basic", start_date=datetime(2024, 1, 1), schedule=None):

    @task
    def get_files():
        """Returns a list of files to process"""
        return ["file1.csv", "file2.csv", "file3.csv", "file4.csv"]

    @task
    def process_file(filename: str):
        """Process a single file - runs once per file"""
        print(f"Processing: {filename}")
        return {"file": filename, "status": "done"}

    # The magic: expand() creates one task instance per item
    files = get_files()
    process_file.expand(filename=files)
```

**Result**: 4 parallel task instances, one for each file.

### Expanding Over Multiple Arguments

```python
@task
def process_item(item_id: int):
    return item_id * 2


# Expand over a list directly
process_item.expand(item_id=[1, 2, 3, 4, 5])


# Or from upstream task output
@task
def get_ids():
    return [10, 20, 30]


ids = get_ids()
process_item.expand(item_id=ids)
```

---

## 3. partial() + expand() Pattern

Use `partial()` to fix some arguments while expanding others:

```python
@task
def process_data(table: str, partition_date: str, mode: str):
    """Process a specific table partition"""
    print(f"Processing {table} for {partition_date} in {mode} mode")
    return {"table": table, "date": partition_date}


with DAG(...):
    # Fixed arguments + dynamic arguments
    process_data.partial(
        mode="incremental"  # Same for all
    ).expand(
        table=["users", "orders", "products"],  # One task per table
        partition_date=["2024-01-01", "2024-01-02"],  # Cross-product!
    )
```

⚠️ **Warning**: When expanding multiple arguments, you get the **Cartesian product**:

- 3 tables × 2 dates = 6 task instances

### Explicit Combinations with expand_kwargs

To avoid Cartesian product, use `expand_kwargs`:

```python
@task
def process(table: str, date: str, priority: int):
    return f"{table}:{date}:{priority}"


# Define explicit combinations
combinations = [
    {"table": "users", "date": "2024-01-01", "priority": 1},
    {"table": "orders", "date": "2024-01-01", "priority": 2},
    {"table": "products", "date": "2024-01-02", "priority": 1},
]

process.expand_kwargs(combinations)
# Creates exactly 3 task instances
```

---

## 4. Aggregating Mapped Results

After parallel processing, you often need to aggregate:

```python
from datetime import datetime

from airflow.sdk import DAG, task

with DAG(dag_id="aggregate_example", start_date=datetime(2024, 1, 1), schedule=None):

    @task
    def get_numbers():
        return [1, 2, 3, 4, 5]

    @task
    def square(x: int) -> int:
        return x**2

    @task
    def sum_results(results: list[int]) -> int:
        """Receives ALL mapped outputs as a list"""
        total = sum(results)
        print(f"Sum of squares: {total}")
        return total

    numbers = get_numbers()
    squared = square.expand(x=numbers)  # 5 parallel tasks
    sum_results(squared)  # Gets [1, 4, 9, 16, 25], returns 55
```

### Aggregation Rules

When a downstream task receives a mapped task's output:

- **Unmapped downstream**: Receives a list of all outputs
- **Mapped downstream**: Each instance receives one output (zip behavior)

---

## 5. Map Index and Task Context

Access information about which map instance you're in:

```python
from airflow.sdk import get_current_context, task


@task
def indexed_task(item: str):
    context = get_current_context()
    map_index = context["ti"].map_index

    print(f"Processing item {map_index}: {item}")
    return {"index": map_index, "item": item}
```

### Using map_index for Partitioning

```python
@task
def process_partition(partition_id: int):
    context = get_current_context()
    total_partitions = context["ti"].map_count or 1

    print(f"Processing partition {partition_id + 1} of {total_partitions}")
    return partition_id


# Create 10 partitions
process_partition.expand(partition_id=list(range(10)))
```

---

## 6. Real-World Patterns

### Pattern 1: Process Files in S3

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import DAG, task


@task
def list_s3_files(bucket: str, prefix: str) -> list[str]:
    """List all files matching prefix"""
    hook = S3Hook(aws_conn_id="aws_default")
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
    return keys or []


@task
def process_s3_file(bucket: str, key: str) -> dict:
    """Process a single S3 file"""
    hook = S3Hook(aws_conn_id="aws_default")
    content = hook.read_key(key=key, bucket_name=bucket)
    # Process content...
    return {"key": key, "size": len(content)}


@task
def summarize(results: list[dict]):
    """Aggregate processing results"""
    total_size = sum(r["size"] for r in results)
    print(f"Processed {len(results)} files, total {total_size} bytes")


with DAG(...):
    bucket = "my-bucket"
    files = list_s3_files(bucket, "data/incoming/")
    processed = process_s3_file.partial(bucket=bucket).expand(key=files)
    summarize(processed)
```

### Pattern 2: Paginated API Calls

```python
@task
def get_page_count() -> int:
    """Determine how many pages to fetch"""
    response = requests.head("https://api.example.com/users")
    total = int(response.headers.get("X-Total-Count", 100))
    page_size = 50
    return (total + page_size - 1) // page_size


@task
def fetch_page(page: int) -> list[dict]:
    """Fetch a single page"""
    response = requests.get("https://api.example.com/users", params={"page": page, "limit": 50})
    return response.json()


@task
def combine_results(pages: list[list[dict]]) -> list[dict]:
    """Flatten all pages into single list"""
    return [item for page in pages for item in page]


with DAG(...):
    page_count = get_page_count()
    # Generate page numbers dynamically
    page_numbers = list(range(1, page_count + 1))  # Note: This won't work!

    # Instead, use expand with range from task output
    pages = fetch_page.expand(page=...)  # See note below
    combine_results(pages)
```

**Note**: For truly dynamic range generation, you need:

```python
@task
def generate_page_range(count: int) -> list[int]:
    return list(range(1, count + 1))


page_count = get_page_count()
pages = generate_page_range(page_count)
fetch_page.expand(page=pages)
```

### Pattern 3: Database Partitions

```python
@task
def get_partitions() -> list[str]:
    """Get list of date partitions to process"""
    hook = PostgresHook(postgres_conn_id="warehouse")
    result = hook.get_records("""
        SELECT DISTINCT partition_date
        FROM events
        WHERE processed = false
        ORDER BY partition_date
    """)
    return [row[0] for row in result]


@task
def process_partition(partition_date: str):
    """Process a single partition"""
    hook = PostgresHook(postgres_conn_id="warehouse")
    hook.run(f"""
        INSERT INTO events_processed
        SELECT * FROM events WHERE partition_date = '{partition_date}'
    """)
    return partition_date


with DAG(...):
    partitions = get_partitions()
    process_partition.expand(partition_date=partitions)
```

---

## 7. Limits and Best Practices

### Setting Concurrency Limits

```python
# Limit parallel mapped instances
@task(max_active_tis_per_dag=10)  # Max 10 concurrent instances
def process(item: str):
    pass


process.expand(item=large_list)
```

### Empty List Handling

```python
@task
def might_be_empty() -> list[str]:
    return []  # Returns empty list


@task
def process(item: str):
    pass


# With empty list: task is marked as skipped
process.expand(item=might_be_empty())
```

### Best Practices

1. **Keep mapped tasks idempotent** - They may be retried individually
2. **Limit expansion size** - Thousands of mapped tasks impact scheduler
3. **Use appropriate parallelism** - Consider pool limits
4. **Handle empty expansions** - Use trigger rules appropriately

---

## 📝 Exercises

### Exercise 6.1: Basic Mapping

Create a DAG that:

1. Returns a list of 10 random numbers
2. Squares each number in parallel
3. Sums all the squares

### Exercise 6.2: File Processing Simulation

Create a DAG that:

1. Simulates listing files (return list of filenames)
2. Processes each file (add metadata like size, type)
3. Generates a summary report

### Exercise 6.3: Cross-Product Processing

Create a DAG with:

- 3 regions: ["us", "eu", "apac"]
- 4 product types: ["widget", "gadget", "doohickey", "thingamajig"]
- A task that runs for each combination (12 total)
- An aggregation task that summarizes all results

---

## ✅ Checkpoint

Before moving to Module 07, ensure you can:

- [ ] Use `expand()` for parallel task execution
- [ ] Combine `partial()` with `expand()` for mixed parameters
- [ ] Use `expand_kwargs()` for explicit combinations
- [ ] Aggregate results from mapped tasks
- [ ] Access map_index in task context
- [ ] Handle edge cases (empty lists, limits)

---

## 🏭 Industry Spotlight: Netflix

**How Netflix Uses Dynamic Mapping for Video Encoding**

Netflix processes thousands of video assets daily, each requiring encoding into multiple resolutions and formats. Dynamic task mapping enables horizontal scaling without code changes:

| Challenge                 | Dynamic Mapping Solution                      |
| ------------------------- | --------------------------------------------- |
| **Variable workloads**    | `expand()` creates tasks per video/format     |
| **Resource optimization** | Parallel encoding maximizes GPU utilization   |
| **Format explosion**      | One task definition, unlimited combinations   |
| **Failure isolation**     | Individual encode failures don't block others |

**Pattern in Use**: Netflix-style video encoding pipeline:

```python
@task
def get_encoding_jobs(video_id: str) -> list[dict]:
    """Generate encoding configurations for one video."""
    formats = ["4k_hdr", "1080p", "720p", "480p"]
    codecs = ["h265", "av1"]
    return [{"video_id": video_id, "format": f, "codec": c} for f in formats for c in codecs]


@task
def encode_video(job: dict) -> dict:
    """Encode single video/format combination."""
    return run_encoder(job["video_id"], job["format"], job["codec"])


# Dynamic expansion: 1 video → 8 parallel encode tasks
jobs = get_encoding_jobs("movie_12345")
results = encode_video.expand(job=jobs)
```

**Key Insight**: Dynamic mapping reduced Netflix's encoding pipeline complexity by 80% while improving throughput 3x through automatic parallelization.

📖 **Related Exercise**: [Exercise 6.4: Parallel Embeddings](exercises/exercise_6_4_parallel_embeddings.md) - Apply dynamic mapping to AI/ML workloads

---

## 📚 Further Reading

- [Dynamic Task Mapping](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html)
- [Task Expansion Patterns](https://airflow.apache.org/docs/apache-airflow/stable/howto/dynamic-dags.html)
- [Case Study: Spotify Recommendations](../../docs/case-studies/spotify-recommendations.md)

---

Next: [Module 07: Testing & Debugging →](../07-testing-debugging/README.md)
