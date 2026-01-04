# Exercise 10.1: REST API Automation

## Objective

Build a Python automation script that uses Airflow's REST API v2 to manage DAGs, including listing DAGs, monitoring execution, and triggering maintenance runs.

## Background

Airflow 3's REST API v2 provides programmatic access to:
- DAG management (list, trigger, pause)
- DAG Run monitoring
- Task Instance inspection
- XCom access
- Connection and Variable management

### API Base URL

```
https://your-airflow-host/api/v2/
```

### Authentication Methods

1. **Basic Auth**: Username/password
2. **Session Auth**: Login cookie
3. **Token Auth**: API tokens (if configured)

## Requirements

Create a Python script that:

### Part 1: List and Analyze DAGs

1. Connect to Airflow API
2. List all DAGs with their properties
3. Filter to find DAGs that:
   - Haven't run in the last 7 days
   - Are not paused
   - Have a schedule defined

### Part 2: DAG Run Analysis

1. For each inactive DAG, get the last run details
2. Calculate time since last successful run
3. Check for any failed recent runs

### Part 3: Automated Trigger

1. Trigger each stale DAG with a configuration flag
2. Monitor the run status until completion
3. Report results

### Part 4: Health Report

Generate a report showing:
- Total DAGs
- Active vs paused DAGs
- DAGs with recent failures
- Stale DAGs that were triggered

## Starter Code

See `exercise_10_1_api_automation_starter.py`

## Example Output

```
=== Airflow DAG Maintenance Report ===
Generated: 2024-01-15 10:00:00

DAG Summary:
- Total DAGs: 45
- Active: 38
- Paused: 7

Stale DAGs (no run in 7 days):
1. data_archive_monthly - Last run: 2024-01-01
2. legacy_report_sync - Last run: 2024-01-05
3. cleanup_temp_files - Last run: 2024-01-08

Triggered Runs:
✅ data_archive_monthly - run_id: manual__2024-01-15T10:00:00
✅ legacy_report_sync - run_id: manual__2024-01-15T10:00:01
❌ cleanup_temp_files - FAILED: DAG has import errors

Recent Failures (last 24h):
- daily_metrics: 2 failures
- api_sync: 1 failure
```

## Hints

<details>
<summary>Hint 1: API Client Setup</summary>

```python
import requests
from datetime import datetime, timedelta

class AirflowClient:
    def __init__(self, base_url, username, password):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })

    def get(self, endpoint):
        response = self.session.get(f"{self.base_url}{endpoint}")
        response.raise_for_status()
        return response.json()
```

</details>

<details>
<summary>Hint 2: Pagination handling</summary>

```python
def get_all_dags(self):
    """Handle API pagination."""
    dags = []
    offset = 0
    limit = 100

    while True:
        response = self.get(f"/dags?offset={offset}&limit={limit}")
        dags.extend(response['dags'])

        if len(response['dags']) < limit:
            break
        offset += limit

    return dags
```

</details>

<details>
<summary>Hint 3: Triggering DAG runs</summary>

```python
def trigger_dag(self, dag_id, conf=None):
    """Trigger a DAG run."""
    payload = {
        "conf": conf or {},
        "note": "Triggered by maintenance script"
    }
    response = self.session.post(
        f"{self.base_url}/dags/{dag_id}/dagRuns",
        json=payload
    )
    response.raise_for_status()
    return response.json()
```

</details>

## Success Criteria

- [ ] Successfully connects to Airflow API
- [ ] Lists all DAGs with relevant properties
- [ ] Correctly identifies stale DAGs (no run in 7 days)
- [ ] Triggers DAG runs with maintenance flag
- [ ] Monitors run status until completion
- [ ] Generates comprehensive report
- [ ] Handles errors gracefully (import errors, API errors)
- [ ] Code is well-documented and reusable

---

Next: [Exercise 10.2: Custom Timetable →](exercise_10_2_custom_timetable.md)
