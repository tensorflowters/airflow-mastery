# Module 12: REST API

## Overview

This module covers Airflow's REST API v2 for programmatic access to DAGs, runs, tasks, and system information. Learn to build integrations, automation scripts, and monitoring tools.

**Learning Time**: 3-4 hours

## Learning Objectives

By the end of this module, you will be able to:

1. Understand REST API v2 architecture and authentication
2. Query and manage DAGs programmatically
3. Trigger and monitor DAG runs via API
4. Access task instance details and XCom values
5. Build automation scripts and integrations
6. Implement proper error handling and pagination

## Prerequisites

- Completed Modules 01-06
- Basic understanding of REST APIs
- Familiarity with Python `requests` library

## Key Concepts

### 1. API Overview

Airflow 3.x provides a comprehensive REST API:

```
Base URL: http://<host>:<port>/api/v2/

Main Resources:
├── /dags                    # DAG management
├── /dags/{dag_id}/dagRuns   # DAG run operations
├── /dags/{dag_id}/tasks     # Task definitions
├── /taskInstances           # Task execution details
├── /variables               # Variable management
├── /connections             # Connection management
├── /pools                   # Pool management
├── /config                  # Configuration info
└── /health                  # Health check endpoint
```

### 2. Authentication Methods

```python
import requests

# Method 1: Basic Authentication (default)
session = requests.Session()
session.auth = ("admin", "admin")

# Method 2: Token Authentication
session.headers["Authorization"] = "Bearer <token>"

# Method 3: Session Authentication (after login)
login_response = session.post(
    "http://localhost:8080/api/v2/auth/login",
    json={"username": "admin", "password": "admin"}
)
```

### 3. Common Operations

```python
BASE_URL = "http://localhost:8080/api/v2"

# List all DAGs
response = session.get(f"{BASE_URL}/dags")
dags = response.json()["dags"]

# Get specific DAG
response = session.get(f"{BASE_URL}/dags/my_dag")
dag_details = response.json()

# Trigger DAG run
response = session.post(
    f"{BASE_URL}/dags/my_dag/dagRuns",
    json={"conf": {"key": "value"}}
)
run_id = response.json()["dag_run_id"]

# Get DAG run status
response = session.get(
    f"{BASE_URL}/dags/my_dag/dagRuns/{run_id}"
)
state = response.json()["state"]
```

### 4. Pagination

API responses may be paginated:

```python
def get_all_dags(session):
    """Handle pagination to get all DAGs."""
    dags = []
    offset = 0
    limit = 100

    while True:
        response = session.get(
            f"{BASE_URL}/dags",
            params={"offset": offset, "limit": limit}
        )
        data = response.json()
        dags.extend(data["dags"])

        if len(data["dags"]) < limit:
            break
        offset += limit

    return dags
```

### 5. Error Handling

```python
def safe_api_call(session, url, method="GET", **kwargs):
    """Make API call with proper error handling."""
    try:
        response = session.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as e:
        if response.status_code == 404:
            return None  # Resource not found
        elif response.status_code == 401:
            raise AuthenticationError("Invalid credentials")
        elif response.status_code == 403:
            raise PermissionError("Insufficient permissions")
        else:
            raise APIError(f"HTTP {response.status_code}: {e}")

    except requests.exceptions.ConnectionError:
        raise ConnectionError("Cannot connect to Airflow API")
```

## API Reference Quick Guide

### DAG Operations

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/dags` | GET | List all DAGs |
| `/dags/{dag_id}` | GET | Get DAG details |
| `/dags/{dag_id}` | PATCH | Update DAG (pause/unpause) |
| `/dags/{dag_id}/dagRuns` | GET | List DAG runs |
| `/dags/{dag_id}/dagRuns` | POST | Trigger new run |
| `/dags/{dag_id}/dagRuns/{run_id}` | GET | Get run details |
| `/dags/{dag_id}/dagRuns/{run_id}` | DELETE | Delete run |

### Task Operations

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/dags/{dag_id}/tasks` | GET | List tasks in DAG |
| `/dags/{dag_id}/tasks/{task_id}` | GET | Get task details |
| `/taskInstances` | GET | List task instances |
| `/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}` | GET | Get specific task instance |

### System Operations

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/version` | GET | Airflow version |
| `/variables` | GET/POST | List/create variables |
| `/variables/{key}` | GET/PATCH/DELETE | Variable CRUD |
| `/connections` | GET/POST | List/create connections |
| `/pools` | GET/POST | List/create pools |

## Exercises

### Exercise 12.1: API Basics
Learn API authentication and basic operations:
- Set up authenticated session
- List and query DAGs
- Understand response structure

### Exercise 12.2: DAG Management
Build DAG management operations:
- Trigger DAG runs with configuration
- Monitor run progress
- Handle task instances

### Exercise 12.3: Automation Script
Create a comprehensive automation tool:
- Find stale DAGs
- Trigger maintenance runs
- Generate health reports

## Solutions

Complete solutions are in the `solutions/` directory.

## Common Patterns

### Pattern 1: DAG Health Checker

```python
def check_dag_health(session, dag_id, lookback_hours=24):
    """Check recent DAG run health."""
    from datetime import datetime, timedelta

    cutoff = datetime.utcnow() - timedelta(hours=lookback_hours)

    response = session.get(
        f"{BASE_URL}/dags/{dag_id}/dagRuns",
        params={
            "start_date_gte": cutoff.isoformat(),
            "order_by": "-start_date",
            "limit": 100,
        }
    )

    runs = response.json()["dag_runs"]

    stats = {
        "total": len(runs),
        "success": sum(1 for r in runs if r["state"] == "success"),
        "failed": sum(1 for r in runs if r["state"] == "failed"),
        "running": sum(1 for r in runs if r["state"] == "running"),
    }
    stats["success_rate"] = (
        stats["success"] / stats["total"] * 100
        if stats["total"] > 0 else 0
    )

    return stats
```

### Pattern 2: Bulk DAG Operations

```python
def pause_dags_by_tag(session, tag, pause=True):
    """Pause/unpause all DAGs with a specific tag."""
    # Get DAGs with tag
    response = session.get(
        f"{BASE_URL}/dags",
        params={"tags": tag, "limit": 1000}
    )
    dags = response.json()["dags"]

    results = []
    for dag in dags:
        dag_id = dag["dag_id"]
        response = session.patch(
            f"{BASE_URL}/dags/{dag_id}",
            json={"is_paused": pause}
        )
        results.append({
            "dag_id": dag_id,
            "status": "paused" if pause else "unpaused",
            "success": response.status_code == 200,
        })

    return results
```

### Pattern 3: Trigger and Wait

```python
def trigger_and_wait(session, dag_id, conf=None, timeout=3600):
    """Trigger DAG and wait for completion."""
    import time

    # Trigger
    response = session.post(
        f"{BASE_URL}/dags/{dag_id}/dagRuns",
        json={"conf": conf or {}}
    )
    response.raise_for_status()
    run_id = response.json()["dag_run_id"]

    # Poll for completion
    start = time.time()
    while time.time() - start < timeout:
        response = session.get(
            f"{BASE_URL}/dags/{dag_id}/dagRuns/{run_id}"
        )
        state = response.json()["state"]

        if state in ("success", "failed"):
            return {"run_id": run_id, "state": state}

        time.sleep(10)

    return {"run_id": run_id, "state": "timeout"}
```

## Best Practices

1. **Use sessions for connection pooling**:
   ```python
   session = requests.Session()
   session.auth = (username, password)
   # Reuse session for multiple requests
   ```

2. **Handle pagination properly**:
   ```python
   # Always check if more pages exist
   while len(results) < total_entries:
       # Fetch next page
   ```

3. **Implement retry logic**:
   ```python
   from requests.adapters import HTTPAdapter
   from urllib3.util.retry import Retry

   retry = Retry(total=3, backoff_factor=0.5)
   adapter = HTTPAdapter(max_retries=retry)
   session.mount("http://", adapter)
   ```

4. **Use appropriate timeouts**:
   ```python
   response = session.get(url, timeout=(5, 30))  # (connect, read)
   ```

5. **Log API interactions**:
   ```python
   logging.debug(f"API call: {method} {url}")
   logging.debug(f"Response: {response.status_code}")
   ```

## Troubleshooting

### Authentication Issues

```python
# Check API is accessible
response = session.get(f"{BASE_URL}/health")
print(f"Health check: {response.status_code}")

# Verify credentials
response = session.get(f"{BASE_URL}/dags?limit=1")
if response.status_code == 401:
    print("Invalid credentials")
elif response.status_code == 403:
    print("Insufficient permissions")
```

### Rate Limiting

```python
# Implement exponential backoff
import time

def api_call_with_backoff(session, url, max_retries=5):
    for attempt in range(max_retries):
        response = session.get(url)
        if response.status_code == 429:  # Too Many Requests
            wait = 2 ** attempt
            time.sleep(wait)
            continue
        return response
    raise Exception("Max retries exceeded")
```

## Next Steps

After completing this module:
1. Review Module 13: Connections & Secrets
2. Explore Module 14: Resource Management
3. Build your own automation tools

## References

- [Airflow REST API Reference](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
- [API Authentication Guide](https://airflow.apache.org/docs/apache-airflow/stable/security/api.html)
- [Python Requests Library](https://requests.readthedocs.io/)
