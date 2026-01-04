# Exercise 6.2: File Processing Simulation

## Objective

Create a DAG that simulates a real-world file processing pipeline using Dynamic Task Mapping.

## Background

One of the most common use cases for Dynamic Task Mapping is processing files:
- You don't know how many files exist until runtime
- Each file can be processed independently
- Results need to be combined into a summary

### Real-World Scenarios

| Scenario | Files | Processing |
|----------|-------|------------|
| Data Lake Ingestion | CSV/Parquet files | Validate and load |
| Log Analysis | Log files | Parse and aggregate |
| Image Processing | Images | Resize/compress |
| Report Generation | Data files | Transform and merge |

## Requirements

Create a DAG that simulates file processing:

1. **DAG ID**: `exercise_6_2_file_processing`
2. **Schedule**: `None` (manual trigger)
3. **Start date**: January 1, 2024
4. **Tags**: `["exercise", "module-06", "dynamic-tasks", "files"]`

### Tasks

1. **list_files**: Simulate listing files from a directory
   - Return a list of file names (e.g., `["file_001.csv", "file_002.csv", ...]`)
   - Generate 5-10 random file names

2. **process_file**: Process a single file (mapped task)
   - Receives one filename
   - Simulates extracting metadata (size, type, rows)
   - Returns a dict with file info

3. **generate_report**: Aggregate all processed files
   - Receives all file processing results
   - Generates a summary report
   - Calculates totals (total size, total rows, etc.)

### Expected Output

```python
# generate_report should produce something like:
{
    "total_files": 8,
    "total_size_mb": 156.4,
    "total_rows": 45000,
    "file_types": {"csv": 5, "json": 3},
    "processing_time": "2.3s"
}
```

## Key Concepts

### Accessing Map Index

Each mapped task instance knows its index:

```python
@task
def process_file(filename: str, **context):
    # Access the map index
    map_index = context.get("map_index", 0)
    print(f"Processing file {map_index}: {filename}")
```

### Returning Rich Data

Mapped tasks can return complex dictionaries:

```python
@task
def process_file(filename: str) -> dict:
    return {
        "filename": filename,
        "size_mb": 12.5,
        "row_count": 5000,
        "status": "success"
    }
```

## Starter Code

See `exercise_6_2_file_processing_starter.py`

## Testing Your DAG

```bash
# Test the DAG
airflow dags test exercise_6_2_file_processing 2024-01-15

# Check logs for each file processing instance
# The report task should show aggregated statistics
```

## Hints

<details>
<summary>Hint 1: Generating file list</summary>

```python
import random

@task
def list_files() -> list[str]:
    num_files = random.randint(5, 10)
    extensions = ["csv", "json", "parquet"]

    files = []
    for i in range(num_files):
        ext = random.choice(extensions)
        files.append(f"data_{i:03d}.{ext}")

    return files
```

</details>

<details>
<summary>Hint 2: Processing with metadata</summary>

```python
import random

@task
def process_file(filename: str) -> dict:
    # Simulate processing
    extension = filename.split(".")[-1]

    return {
        "filename": filename,
        "extension": extension,
        "size_mb": round(random.uniform(1, 50), 2),
        "row_count": random.randint(100, 10000),
        "status": "processed",
    }
```

</details>

<details>
<summary>Hint 3: Aggregating file results</summary>

```python
@task
def generate_report(file_results: list[dict]) -> dict:
    total_size = sum(f["size_mb"] for f in file_results)
    total_rows = sum(f["row_count"] for f in file_results)

    # Count by extension
    ext_counts = {}
    for f in file_results:
        ext = f["extension"]
        ext_counts[ext] = ext_counts.get(ext, 0) + 1

    return {
        "total_files": len(file_results),
        "total_size_mb": round(total_size, 2),
        "total_rows": total_rows,
        "by_extension": ext_counts,
    }
```

</details>

## Success Criteria

- [ ] DAG generates a variable number of files (5-10)
- [ ] Each file is processed by a separate task instance
- [ ] File processing extracts simulated metadata
- [ ] Report correctly aggregates all file statistics
- [ ] You understand how to access map_index
- [ ] Results are properly typed and structured

## Extension Challenge

Try adding error handling:

```python
@task
def process_file(filename: str) -> dict:
    import random

    # Simulate occasional failures
    if random.random() < 0.1:
        raise ValueError(f"Failed to process {filename}")

    # Normal processing...
```

Then add a downstream task with `trigger_rule=TriggerRule.ALL_DONE` to handle the report even when some files fail.
