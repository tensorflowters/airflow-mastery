"""
Example DAG: REST API Integration

Demonstrates how to interact with Airflow's REST API
programmatically for automation and monitoring.

Module: 12-rest-api
"""

from datetime import datetime
import json

from airflow.sdk import DAG, task
from airflow.operators.empty import EmptyOperator


# =============================================================================
# DAG 1: API Client Demonstration
# =============================================================================

with DAG(
    dag_id="13_api_client_demo",
    description="Shows REST API client patterns",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-12", "rest-api"],
):
    """
    This DAG demonstrates patterns for REST API integration.

    In practice, you would call the API from:
    - External scripts for automation
    - CI/CD pipelines for deployment
    - Monitoring systems for health checks
    - Admin tools for bulk operations
    """

    @task
    def show_api_endpoints():
        """Display common API endpoints and their uses."""
        endpoints = {
            "DAG Operations": {
                "GET /dags": "List all DAGs",
                "GET /dags/{dag_id}": "Get DAG details",
                "PATCH /dags/{dag_id}": "Update DAG (pause/unpause)",
                "DELETE /dags/{dag_id}": "Delete a DAG",
            },
            "DAG Run Operations": {
                "GET /dags/{dag_id}/dagRuns": "List DAG runs",
                "POST /dags/{dag_id}/dagRuns": "Trigger new DAG run",
                "GET /dags/{dag_id}/dagRuns/{run_id}": "Get run details",
                "DELETE /dags/{dag_id}/dagRuns/{run_id}": "Delete a run",
                "POST /dags/{dag_id}/clearTaskInstances": "Clear tasks",
            },
            "Task Instance Operations": {
                "GET /dags/{dag_id}/dagRuns/{run_id}/taskInstances": "List tasks",
                "GET /dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}": "Get task",
                "GET /dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs": "Get logs",
            },
            "Pool Operations": {
                "GET /pools": "List all pools",
                "POST /pools": "Create a pool",
                "PATCH /pools/{pool_name}": "Update pool",
                "DELETE /pools/{pool_name}": "Delete pool",
            },
            "Variable Operations": {
                "GET /variables": "List all variables",
                "POST /variables": "Create variable",
                "PATCH /variables/{key}": "Update variable",
                "DELETE /variables/{key}": "Delete variable",
            },
            "Connection Operations": {
                "GET /connections": "List connections",
                "POST /connections": "Create connection",
                "GET /connections/{conn_id}": "Get connection",
                "DELETE /connections/{conn_id}": "Delete connection",
            },
            "Health & Info": {
                "GET /health": "Health check",
                "GET /version": "Airflow version",
                "GET /config": "Current configuration",
            },
        }

        print("=" * 60)
        print("AIRFLOW REST API v2 ENDPOINTS")
        print("=" * 60)

        for category, ops in endpoints.items():
            print(f"\n{category}:")
            print("-" * 40)
            for endpoint, description in ops.items():
                print(f"  {endpoint}")
                print(f"    → {description}")

        return endpoints

    @task
    def show_client_example():
        """Show example Python client code."""
        example_code = '''
# Example: Python REST API Client

import requests
from requests.auth import HTTPBasicAuth

class AirflowClient:
    """Simple Airflow REST API client."""

    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip('/')
        self.auth = HTTPBasicAuth(username, password)
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })

    def trigger_dag(self, dag_id, conf=None):
        """Trigger a DAG run."""
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns"
        payload = {"conf": conf or {}}
        response = self.session.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def get_dag_run(self, dag_id, run_id):
        """Get DAG run status."""
        url = f"{self.base_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def wait_for_dag(self, dag_id, run_id, timeout=3600, interval=10):
        """Wait for DAG run to complete."""
        import time
        terminal_states = {"success", "failed"}
        start = time.time()

        while time.time() - start < timeout:
            run = self.get_dag_run(dag_id, run_id)
            if run["state"] in terminal_states:
                return run
            time.sleep(interval)

        raise TimeoutError(f"DAG run did not complete in {timeout}s")

# Usage:
client = AirflowClient(
    base_url="http://localhost:8080",
    username="admin",
    password="admin"
)

# Trigger and wait
run = client.trigger_dag("my_dag", conf={"key": "value"})
result = client.wait_for_dag("my_dag", run["dag_run_id"])
print(f"DAG completed with state: {result['state']}")
'''

        print(example_code)
        return {"example": "displayed"}

    show_api_endpoints() >> show_client_example()


# =============================================================================
# DAG 2: Self-Monitoring DAG
# =============================================================================

with DAG(
    dag_id="13_self_monitoring",
    description="DAG that monitors its own history via API patterns",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-12", "rest-api", "monitoring"],
):
    @task
    def get_own_history():
        """
        Get this DAG's run history.

        In practice, use the REST API. Here we show the pattern.
        """
        # Simulated API response
        history = [
            {"run_id": "run_001", "state": "success", "duration": 45},
            {"run_id": "run_002", "state": "success", "duration": 52},
            {"run_id": "run_003", "state": "failed", "duration": 30},
            {"run_id": "run_004", "state": "success", "duration": 48},
            {"run_id": "run_005", "state": "success", "duration": 55},
        ]

        print(f"Found {len(history)} historical runs")
        return history

    @task
    def analyze_performance(history: list):
        """Analyze DAG performance metrics."""
        successful_runs = [r for r in history if r["state"] == "success"]
        failed_runs = [r for r in history if r["state"] == "failed"]

        if successful_runs:
            avg_duration = sum(r["duration"] for r in successful_runs) / len(successful_runs)
        else:
            avg_duration = 0

        metrics = {
            "total_runs": len(history),
            "successful_runs": len(successful_runs),
            "failed_runs": len(failed_runs),
            "success_rate": len(successful_runs) / len(history) * 100 if history else 0,
            "avg_duration_seconds": round(avg_duration, 2),
        }

        print("Performance Metrics:")
        print(json.dumps(metrics, indent=2))
        return metrics

    @task
    def check_thresholds(metrics: dict):
        """Check if metrics exceed thresholds."""
        alerts = []

        if metrics["success_rate"] < 90:
            alerts.append(f"Low success rate: {metrics['success_rate']:.1f}%")

        if metrics["avg_duration_seconds"] > 60:
            alerts.append(f"High avg duration: {metrics['avg_duration_seconds']}s")

        if metrics["failed_runs"] > 2:
            alerts.append(f"Multiple failures: {metrics['failed_runs']} runs")

        if alerts:
            print("ALERTS:")
            for alert in alerts:
                print(f"  ⚠️ {alert}")
        else:
            print("✅ All metrics within thresholds")

        return {"alerts": alerts, "healthy": len(alerts) == 0}

    history = get_own_history()
    metrics = analyze_performance(history)
    check_thresholds(metrics)


# =============================================================================
# DAG 3: Bulk Operations Pattern
# =============================================================================

with DAG(
    dag_id="13_bulk_operations",
    description="Patterns for bulk API operations",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-12", "rest-api", "bulk"],
):
    @task
    def show_bulk_trigger_pattern():
        """Show pattern for triggering multiple DAGs."""
        pattern = '''
# Bulk DAG Trigger Pattern

def trigger_pipeline(client, dag_configs):
    """Trigger multiple DAGs with dependencies."""
    results = {}

    for config in dag_configs:
        dag_id = config["dag_id"]
        depends_on = config.get("depends_on", [])

        # Wait for dependencies
        for dep in depends_on:
            if dep in results:
                client.wait_for_dag(dep, results[dep]["dag_run_id"])

        # Trigger this DAG
        run = client.trigger_dag(dag_id, conf=config.get("conf", {}))
        results[dag_id] = run
        print(f"Triggered {dag_id}: {run['dag_run_id']}")

    return results

# Usage:
configs = [
    {"dag_id": "extract_dag", "conf": {"date": "2024-01-15"}},
    {"dag_id": "transform_dag", "depends_on": ["extract_dag"]},
    {"dag_id": "load_dag", "depends_on": ["transform_dag"]},
]

results = trigger_pipeline(client, configs)
'''
        print(pattern)
        return {"pattern": "bulk_trigger"}

    @task
    def show_bulk_pause_pattern():
        """Show pattern for bulk pausing DAGs."""
        pattern = '''
# Bulk Pause Pattern

def pause_dags_by_tag(client, tag):
    """Pause all DAGs with a specific tag."""
    dags = client.list_dags(tags=[tag])
    paused = []

    for dag in dags:
        if not dag["is_paused"]:
            client.patch_dag(dag["dag_id"], {"is_paused": True})
            paused.append(dag["dag_id"])
            print(f"Paused: {dag['dag_id']}")

    return paused

# Usage:
paused = pause_dags_by_tag(client, "maintenance")
print(f"Paused {len(paused)} DAGs")
'''
        print(pattern)
        return {"pattern": "bulk_pause"}

    @task
    def show_bulk_clear_pattern():
        """Show pattern for bulk clearing failed tasks."""
        pattern = '''
# Bulk Clear Failed Tasks Pattern

def clear_all_failed(client, dag_id, start_date, end_date):
    """Clear all failed tasks in date range."""
    runs = client.list_dag_runs(
        dag_id,
        start_date_gte=start_date,
        end_date_lte=end_date,
        state="failed"
    )

    cleared = []
    for run in runs:
        result = client.clear_task_instances(
            dag_id,
            dag_run_id=run["dag_run_id"],
            only_failed=True
        )
        cleared.append({
            "run_id": run["dag_run_id"],
            "cleared_count": len(result.get("task_instances", []))
        })
        print(f"Cleared {run['dag_run_id']}")

    return cleared

# Usage:
cleared = clear_all_failed(
    client,
    "my_dag",
    start_date="2024-01-01",
    end_date="2024-01-31"
)
'''
        print(pattern)
        return {"pattern": "bulk_clear"}

    [show_bulk_trigger_pattern(), show_bulk_pause_pattern(), show_bulk_clear_pattern()]


# =============================================================================
# DAG 4: API Authentication Patterns
# =============================================================================

with DAG(
    dag_id="13_api_authentication",
    description="Different API authentication patterns",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-12", "rest-api", "auth"],
):
    @task
    def show_auth_patterns():
        """Display different authentication patterns."""
        patterns = '''
# API Authentication Patterns

## 1. Basic Authentication (Development)
```python
from requests.auth import HTTPBasicAuth

client = requests.Session()
client.auth = HTTPBasicAuth("username", "password")
```

## 2. Token Authentication (Recommended)
```python
# Get token first
response = requests.post(
    f"{base_url}/api/v2/token",
    json={"username": "user", "password": "pass"}
)
token = response.json()["access_token"]

# Use token in requests
headers = {"Authorization": f"Bearer {token}"}
requests.get(f"{base_url}/api/v2/dags", headers=headers)
```

## 3. API Key Authentication (Service Accounts)
```python
# Configure in airflow.cfg:
# [api]
# auth_backends = airflow.api.auth.backend.api_key

headers = {"X-Api-Key": "your-api-key"}
requests.get(f"{base_url}/api/v2/dags", headers=headers)
```

## 4. OAuth2/OIDC (Enterprise)
```python
# Configure OAuth provider in webserver_config.py
# Use standard OAuth2 flow to get token
from authlib.integrations.requests_client import OAuth2Session

oauth = OAuth2Session(client_id, client_secret)
token = oauth.fetch_token(token_endpoint)
oauth.get(f"{base_url}/api/v2/dags")
```

## Security Best Practices:
- Use environment variables for credentials
- Rotate API keys regularly
- Use service accounts for automation
- Enable HTTPS in production
- Log and monitor API access
'''
        print(patterns)
        return {"patterns": "displayed"}

    show_auth_patterns()
