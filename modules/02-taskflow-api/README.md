# Module 02: TaskFlow API

## 🎯 Learning Objectives

By the end of this module, you will:

- Master the `@task` decorator and its parameters
- Understand automatic XCom handling with TaskFlow
- Use multiple outputs and complex return types
- Handle task dependencies elegantly
- Know when to use TaskFlow vs traditional Operators

## ⏱️ Estimated Time: 4-5 hours

---

## 1. The TaskFlow Philosophy

TaskFlow is Airflow's Pythonic way of writing tasks. Instead of writing operators and managing XCom manually, you write Python functions that just work.

### Traditional Operator Approach vs TaskFlow

```python
# ❌ Traditional (verbose, explicit XCom management)
def extract_func(**context):
    data = {"value": 42}
    context["ti"].xcom_push(key="extracted_data", value=data)


def transform_func(**context):
    data = context["ti"].xcom_pull(key="extracted_data", task_ids="extract_task")
    result = data["value"] * 2
    context["ti"].xcom_push(key="transformed_data", value=result)


extract_task = PythonOperator(task_id="extract_task", python_callable=extract_func, provide_context=True)
transform_task = PythonOperator(task_id="transform_task", python_callable=transform_func, provide_context=True)
extract_task >> transform_task


# ✅ TaskFlow (clean, automatic XCom)
@task
def extract():
    return {"value": 42}


@task
def transform(data: dict):
    return data["value"] * 2


# Dependencies created by function calls
result = extract()
transformed = transform(result)  # Automatic dependency + XCom passing
```

---

## 2. The @task Decorator Deep Dive

### Basic Usage

```python
from datetime import datetime

from airflow.sdk import DAG, task

with DAG(dag_id="taskflow_demo", start_date=datetime(2024, 1, 1), schedule=None):

    @task
    def simple_task():
        """A basic task with no inputs or outputs"""
        print("Executing simple task")

    @task
    def task_with_return():
        """Return value automatically becomes XCom"""
        return {"status": "complete", "count": 42}

    @task
    def task_with_input(data: dict):
        """Receives XCom from upstream task"""
        print(f"Received: {data}")
        return data["count"] * 2

    # Wire up
    data = task_with_return()
    result = task_with_input(data)
```

### Task Decorator Parameters

```python
@task(
    task_id="custom_task_id",  # Override auto-generated ID
    multiple_outputs=True,  # Each dict key becomes separate XCom
    retries=3,  # Number of retry attempts
    retry_delay=timedelta(minutes=5),  # Delay between retries
    trigger_rule="all_success",  # When to run (see Module 04)
    pool="default_pool",  # Resource pool
    queue="default",  # Queue for CeleryExecutor
    execution_timeout=timedelta(hours=1),  # Max execution time
    do_xcom_push=True,  # Push return value to XCom (default True)
)
def my_configured_task():
    pass
```

---

## 3. XCom Patterns with TaskFlow

### Automatic XCom (Default Behavior)

```python
@task
def extract():
    # This entire dict is stored as a single XCom value
    return {"users": [1, 2, 3], "metadata": {"source": "api"}}


@task
def process(data: dict):
    # Access the full dict
    users = data["users"]
    source = data["metadata"]["source"]
    return len(users)
```

### Multiple Outputs

When you return a dictionary and want each key as a separate XCom:

```python
@task(multiple_outputs=True)
def extract_multiple():
    # Each key becomes a separate XCom entry
    return {"user_count": 100, "event_count": 5000, "timestamp": "2024-01-01T00:00:00"}


@task
def process_users(count: int):
    print(f"Processing {count} users")


@task
def process_events(count: int):
    print(f"Processing {count} events")


# Access individual outputs
result = extract_multiple()
process_users(result["user_count"])
process_events(result["event_count"])
```

### Passing Large Data (Anti-Pattern Warning)

```python
# ❌ DON'T: Pass large data through XCom
@task
def bad_practice():
    large_dataframe = pd.read_csv("huge_file.csv")  # 10GB file
    return large_dataframe.to_dict()  # Will crash or slow down


# ✅ DO: Pass references, not data
@task
def good_practice():
    # Process and save to external storage
    df = pd.read_csv("huge_file.csv")
    output_path = "/data/processed/output.parquet"
    df.to_parquet(output_path)
    # Only return the reference
    return {"path": output_path, "row_count": len(df)}


@task
def consume(metadata: dict):
    # Load from path when needed
    df = pd.read_parquet(metadata["path"])
```

---

## 4. Context Access in TaskFlow

Access Airflow runtime context within TaskFlow tasks:

```python
from airflow.sdk import task
from airflow.sdk.types import Context


@task
def context_aware_task(**context: Context):
    """Access full Airflow context"""
    # Common context variables
    logical_date = context["logical_date"]
    dag_run = context["dag_run"]
    task_instance = context["ti"]
    params = context["params"]

    # Data interval (Airflow 3)
    data_interval_start = context["data_interval_start"]
    data_interval_end = context["data_interval_end"]

    print(f"Running for: {logical_date}")
    print(f"Data interval: {data_interval_start} to {data_interval_end}")

    return {"processed_date": str(logical_date)}
```

### Get Current Context Programmatically

```python
from airflow.sdk import get_current_context, task


@task
def task_with_context():
    context = get_current_context()
    ti = context["ti"]

    # You can also pull XCom explicitly if needed
    upstream_data = ti.xcom_pull(task_ids="other_task", key="custom_key")

    return {"dag_run_id": context["dag_run"].run_id}
```

---

## 5. Task Dependencies and Control Flow

### Chaining Tasks

```python
@task
def a():
    return 1


@task
def b(x):
    return x + 1


@task
def c(x):
    return x + 1


@task
def d(x):
    return x + 1


# Method 1: Nested calls (creates linear chain)
result = d(c(b(a())))

# Method 2: Intermediate variables (more readable)
a_result = a()
b_result = b(a_result)
c_result = c(b_result)
d_result = d(c_result)
```

### Parallel Execution

```python
@task
def source_a():
    return "A"


@task
def source_b():
    return "B"


@task
def source_c():
    return "C"


@task
def combine(a, b, c):
    return f"{a}+{b}+{c}"


# a, b, c run in parallel; combine waits for all
result = combine(source_a(), source_b(), source_c())
```

### Mixing TaskFlow and Operators

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

with DAG(...):

    @task
    def prepare_config():
        return {"file": "/tmp/data.csv"}

    # Traditional operator
    download = BashOperator(task_id="download_data", bash_command="curl -o /tmp/data.csv https://example.com/data.csv")

    @task
    def process_file(config: dict):
        print(f"Processing {config['file']}")

    # Mix dependencies
    config = prepare_config()
    config >> download  # TaskFlow output can set dependency on operator
    download >> process_file(config)  # Or use explicit
```

### Using output() for Operator XCom

```python
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task

with DAG(...):
    bash_task = BashOperator(task_id="generate_value", bash_command="echo 'hello world'")

    @task
    def consume_bash_output(value: str):
        print(f"Bash said: {value}")

    # Use .output to get the XCom reference
    consume_bash_output(bash_task.output)
```

---

## 6. Best Practices

### DO ✅

```python
# Keep tasks focused and single-purpose
@task
def extract_users():
    return fetch_from_api("/users")


@task
def extract_orders():
    return fetch_from_api("/orders")


# Use type hints for clarity
@task
def process(data: list[dict]) -> dict:
    return {"count": len(data)}


# Use meaningful task IDs
@task(task_id="validate_user_permissions")
def validate():
    pass
```

### DON'T ❌

```python
# Don't do everything in one task
@task
def do_everything():
    data = extract()
    cleaned = transform(data)
    load(cleaned)
    send_notification()
    # If any step fails, you restart from scratch


# Don't pass large data through XCom
@task
def bad_return():
    return load_giant_dataframe().to_dict()


# Don't use side effects for dependencies
@task
def task_a():
    write_to_file("/tmp/done.txt")


@task
def task_b():
    # DON'T: Check file existence for dependency
    while not os.path.exists("/tmp/done.txt"):
        time.sleep(1)
```

---

## 📝 Exercises

### Exercise 2.1: ETL Pipeline

Build a TaskFlow ETL pipeline that:

1. Extracts data from two mock sources (return dictionaries)
2. Transforms each source separately
3. Combines the transformed data
4. Loads to a "destination" (just print)

Requirements:

- Use `multiple_outputs=True` where appropriate
- Include proper type hints
- Add retry configuration to the extract tasks

### Exercise 2.2: Context Usage

Create a DAG that:

1. Accesses the `logical_date` and formats it
2. Creates a filename based on the date
3. Simulates writing to that file
4. Returns metadata about the operation

### Exercise 2.3: Mixed Operators and TaskFlow

Create a DAG that:

1. Uses a BashOperator to create a temp file
2. Uses a TaskFlow task to read the file path from XCom
3. Uses another TaskFlow task to "process" the file

---

## ✅ Checkpoint

Before moving to Module 03, ensure you can:

- [ ] Write tasks using the `@task` decorator
- [ ] Return and consume data between tasks automatically
- [ ] Use `multiple_outputs=True` correctly
- [ ] Access Airflow context within TaskFlow tasks
- [ ] Mix TaskFlow tasks with traditional Operators
- [ ] Explain why large data shouldn't go through XCom

---

## 🏭 Industry Spotlight: Stripe

**How Stripe Uses TaskFlow for Payment Processing**

Stripe processes billions of dollars in payments, requiring reliable, testable pipelines that handle complex financial data transformations. TaskFlow's Python-native approach provides significant advantages:

| Challenge              | TaskFlow Solution                                       |
| ---------------------- | ------------------------------------------------------- |
| **Financial accuracy** | Type hints + return values make data contracts explicit |
| **Auditability**       | XCom history provides complete data lineage             |
| **Testing**            | Pure Python functions are easily unit-tested            |
| **Retry handling**     | Built-in retry decorators handle transient failures     |

**Pattern in Use**: Stripe-style payment reconciliation with TaskFlow:

```python
@task(retries=3, retry_delay=timedelta(minutes=1))
def extract_transactions(date: str) -> list[dict]:
    """Extract with automatic retry on API failures."""
    return payment_api.get_transactions(date)


@task
def validate_amounts(transactions: list[dict]) -> dict:
    """Type-safe validation with clear data contracts."""
    total = sum(t["amount"] for t in transactions)
    return {"count": len(transactions), "total": total}
```

**Key Insight**: TaskFlow's automatic XCom handling eliminates manual serialization bugs that caused reconciliation issues in legacy systems.

📖 **Related Exercise**: [Exercise 2.4: LLM ETL Pipeline](exercises/exercise_2_4_llm_etl.md) - Apply TaskFlow patterns to AI/ML workloads

---

## 📚 Further Reading

- [TaskFlow Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)
- [XCom Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Case Study: Stripe Fraud Detection](../../docs/case-studies/stripe-fraud-detection.md)

---

Next: [Module 03: Operators & Hooks →](../03-operators-hooks/README.md)
