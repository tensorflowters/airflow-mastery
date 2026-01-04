# Module 03: Operators & Hooks

## 🎯 Learning Objectives

By the end of this module, you will:
- Understand the Operator/Hook architecture
- Use built-in operators from `apache-airflow-providers-standard`
- Know when to use operators vs TaskFlow
- Create custom operators for reusable logic
- Understand Hooks and connection management

## ⏱️ Estimated Time: 4-5 hours

---

## 1. Operators vs TaskFlow: When to Use Which

| Use Case | Recommended Approach |
|----------|---------------------|
| Custom Python logic | TaskFlow `@task` |
| Bash commands | `BashOperator` |
| SQL queries | Database-specific operators |
| Cloud services (S3, GCS, BigQuery) | Provider operators |
| Reusable logic across DAGs | Custom Operator |
| Simple data transformation | TaskFlow |
| External API calls | TaskFlow or HTTP operators |

**Rule of thumb**: Use TaskFlow for Python logic, operators for external system interactions.

---

## 2. Standard Operators (apache-airflow-providers-standard)

### Installation
```bash
pip install apache-airflow-providers-standard
```

### BashOperator

```python
from airflow.providers.standard.operators.bash import BashOperator

# Simple command
print_date = BashOperator(
    task_id="print_date",
    bash_command="date",
)

# With environment variables
run_script = BashOperator(
    task_id="run_script",
    bash_command="python /scripts/process.py ",
    env={"DATA_PATH": "/data", "MODE": "production"},
)

# With templating (Jinja2)
templated_command = BashOperator(
    task_id="templated",
    bash_command="""
        echo "Processing for {{ ds }}"
        echo "Logical date: {{ logical_date }}"
    """,
)

# Capture output via XCom
capture_output = BashOperator(
    task_id="capture_output",
    bash_command="echo 'Hello World'",
    do_xcom_push=True,  # Last line of stdout becomes XCom
)
```

### PythonOperator

When you can't use TaskFlow (rare cases):

```python
from airflow.providers.standard.operators.python import PythonOperator

def my_function(param1, param2, **context):
    print(f"Params: {param1}, {param2}")
    print(f"Logical date: {context['logical_date']}")
    return "result"

python_task = PythonOperator(
    task_id="python_task",
    python_callable=my_function,
    op_kwargs={"param1": "value1", "param2": "value2"},
)
```

### EmptyOperator (formerly DummyOperator)

For control flow and grouping:

```python
from airflow.providers.standard.operators.empty import EmptyOperator

start = EmptyOperator(task_id="start")
end = EmptyOperator(task_id="end")

# Use for fan-in/fan-out patterns
start >> [task_a, task_b, task_c] >> end
```

### BranchPythonOperator

For conditional execution:

```python
from airflow.providers.standard.operators.python import BranchPythonOperator

def choose_branch(**context):
    if context["logical_date"].weekday() < 5:
        return "weekday_task"
    return "weekend_task"

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=choose_branch,
)

branch >> [weekday_task, weekend_task]
```

---

## 3. Common Provider Operators

### HTTP Operator

```python
from airflow.providers.http.operators.http import HttpOperator

call_api = HttpOperator(
    task_id="call_api",
    http_conn_id="my_api",  # Configured in Admin > Connections
    endpoint="/users",
    method="GET",
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.status_code == 200,
)
```

### Postgres Operator

```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

create_table = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="my_postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100)
        );
    """,
)

# Or use SQL files
run_query = PostgresOperator(
    task_id="run_query",
    postgres_conn_id="my_postgres",
    sql="sql/transform_users.sql",  # Path relative to DAG folder
)
```

### S3 Operators

```python
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteObjectsOperator,
    S3ListOperator,
)
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

upload_file = LocalFilesystemToS3Operator(
    task_id="upload_to_s3",
    filename="/local/path/file.csv",
    dest_key="data/file.csv",
    dest_bucket="my-bucket",
    aws_conn_id="aws_default",
    replace=True,
)
```

---

## 4. Understanding Hooks

**Hooks** are the interface to external systems. Operators use hooks under the hood.

```
Operator (what to do) 
    └── Hook (how to connect)
            └── Connection (credentials)
```

### Using Hooks Directly in TaskFlow

```python
from airflow.sdk import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task
def query_database():
    hook = PostgresHook(postgres_conn_id="my_postgres")
    
    # Get records
    records = hook.get_records("SELECT * FROM users LIMIT 10")
    
    # Get pandas DataFrame
    df = hook.get_pandas_df("SELECT * FROM users")
    
    # Execute command
    hook.run("UPDATE users SET active = true WHERE id = 1")
    
    return {"row_count": len(records)}
```

### Common Hooks

| Hook | Use Case |
|------|----------|
| `PostgresHook` | PostgreSQL database |
| `MySqlHook` | MySQL database |
| `S3Hook` | AWS S3 |
| `GCSHook` | Google Cloud Storage |
| `HttpHook` | REST APIs |
| `SlackHook` | Slack messaging |

---

## 5. Connections Management

Connections store credentials and are referenced by `conn_id`.

### Creating Connections

**Via UI**: Admin → Connections → Add

**Via CLI**:
```bash
airflow connections add 'my_postgres' \
    --conn-type 'postgres' \
    --conn-host 'localhost' \
    --conn-schema 'mydb' \
    --conn-login 'user' \
    --conn-password 'pass' \
    --conn-port 5432
```

**Via Environment Variable**:
```bash
export AIRFLOW_CONN_MY_POSTGRES='postgresql://user:pass@localhost:5432/mydb'
```

### Connection String Format

```
<conn-type>://<login>:<password>@<host>:<port>/<schema>?param1=value1
```

---

## 6. Building Custom Operators

For reusable, tested logic across DAGs:

```python
from airflow.models import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from typing import Any

class FetchAndStoreOperator(BaseOperator):
    """
    Fetches data from an API and stores it in S3.
    
    :param http_conn_id: Connection ID for the API
    :param endpoint: API endpoint to call
    :param s3_bucket: Target S3 bucket
    :param s3_key: Target S3 key
    """
    
    # Fields to template (Jinja2)
    template_fields = ("endpoint", "s3_key")
    
    def __init__(
        self,
        http_conn_id: str,
        endpoint: str,
        s3_bucket: str,
        s3_key: str,
        **kwargs
    ):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
    
    def execute(self, context: Any):
        """Main execution method"""
        # Fetch from API
        http_hook = HttpHook(http_conn_id=self.http_conn_id)
        response = http_hook.run(self.endpoint)
        data = response.json()
        
        # Store to S3
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        s3_hook = S3Hook()
        s3_hook.load_string(
            string_data=json.dumps(data),
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True,
        )
        
        self.log.info(f"Stored {len(data)} records to s3://{self.s3_bucket}/{self.s3_key}")
        
        return {"records": len(data), "location": f"s3://{self.s3_bucket}/{self.s3_key}"}
```

### Using Custom Operators

```python
from my_operators import FetchAndStoreOperator

fetch_users = FetchAndStoreOperator(
    task_id="fetch_users",
    http_conn_id="my_api",
    endpoint="/users",
    s3_bucket="my-data-lake",
    s3_key="raw/users/{{ ds }}/data.json",  # Templated!
)
```

---

## 7. Sensors: Waiting for Conditions

Sensors are operators that wait for something to happen:

```python
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Wait for local file
wait_for_file = FileSensor(
    task_id="wait_for_file",
    filepath="/data/incoming/daily_export.csv",
    poke_interval=60,  # Check every 60 seconds
    timeout=3600,      # Give up after 1 hour
    mode="poke",       # or "reschedule" to free up worker
)

# Wait for S3 object
wait_for_s3 = S3KeySensor(
    task_id="wait_for_s3",
    bucket_name="upstream-bucket",
    bucket_key="exports/{{ ds }}/complete.flag",
    aws_conn_id="aws_default",
    mode="reschedule",  # Recommended for long waits
)
```

### Sensor Modes

| Mode | Behavior | Use When |
|------|----------|----------|
| `poke` | Keeps worker slot occupied | Short waits (< 5 min) |
| `reschedule` | Releases worker, reschedules | Long waits (> 5 min) |

---

## 📝 Exercises

### Exercise 3.1: Operator Exploration
Create a DAG that uses:
- BashOperator to create a temp file with random data
- PythonOperator to read and process the file
- BranchPythonOperator to branch based on data content
- Two EmptyOperators as endpoints for each branch

### Exercise 3.2: Custom Operator
Create a custom `DataValidationOperator` that:
- Takes a file path and validation rules as parameters
- Reads the file and validates each rule
- Returns validation results as XCom
- Logs validation failures

### Exercise 3.3: Hook Usage
Create a TaskFlow DAG that:
- Uses HttpHook to fetch data from a public API
- Processes the data
- Uses a FileSystemHook (or simple file write) to store results

---

## ✅ Checkpoint

Before moving to Module 04, ensure you can:

- [ ] Choose between TaskFlow and Operators appropriately
- [ ] Use BashOperator, PythonOperator, and BranchPythonOperator
- [ ] Configure and use Connections
- [ ] Use Hooks in TaskFlow tasks
- [ ] Create a basic custom Operator
- [ ] Understand Sensor modes (poke vs reschedule)

---

Next: [Module 04: Scheduling & Triggers →](../04-scheduling-triggers/README.md)
