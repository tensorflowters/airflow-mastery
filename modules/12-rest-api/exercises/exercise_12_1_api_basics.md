# Exercise 12.1: API Basics

## Objective

Learn Airflow REST API v2 fundamentals including authentication, request/response handling, and basic operations.

## Background

The REST API v2 provides programmatic access to Airflow:

```
http://<airflow-host>/api/v2/
```

Key endpoints:
- `/health` - System health
- `/dags` - DAG listing and management
- `/version` - Airflow version info

## Requirements

### Part 1: Authentication Setup

Create an API client with proper authentication:

```python
class AirflowAPIClient:
    def __init__(self, base_url, username, password):
        # Set up authenticated session
        pass

    def health_check(self):
        # GET /health
        pass
```

### Part 2: Basic Queries

Implement these operations:
1. List all DAGs (handle pagination)
2. Get specific DAG details
3. Get Airflow version

### Part 3: Response Handling

Handle API responses properly:
- Parse JSON responses
- Handle errors (401, 403, 404, 500)
- Implement retry logic

### Part 4: Request Headers

Configure proper headers:
- Content-Type: application/json
- Accept: application/json

## Starter Code

See `exercise_12_1_api_basics_starter.py`

## Hints

<details>
<summary>Hint 1: Session setup</summary>

```python
import requests

session = requests.Session()
session.auth = (username, password)
session.headers.update({
    "Content-Type": "application/json",
    "Accept": "application/json",
})
```

</details>

<details>
<summary>Hint 2: Error handling</summary>

```python
try:
    response = session.get(url)
    response.raise_for_status()
    return response.json()
except requests.exceptions.HTTPError as e:
    if response.status_code == 404:
        return None
    raise
```

</details>

## Success Criteria

- [ ] Client authenticates successfully
- [ ] Health check endpoint works
- [ ] DAG listing handles pagination
- [ ] Errors are handled gracefully
- [ ] Proper headers configured

---

Next: [Exercise 12.2: DAG Management →](exercise_12_2_dag_management.md)
