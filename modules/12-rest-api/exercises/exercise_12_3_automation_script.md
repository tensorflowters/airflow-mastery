# Exercise 12.3: Automation Script

## Objective

Build a comprehensive automation script that orchestrates Airflow operations, including deployment validation, batch triggering, health monitoring, and reporting.

## Background

Production Airflow environments require automation for:
- **Deployment validation**: Verify DAGs after deployment
- **Batch operations**: Trigger multiple DAGs in sequence or parallel
- **Health monitoring**: Continuous health checks with alerting
- **Reporting**: Generate operational reports

## Requirements

### Part 1: Deployment Validator

Create a validator that checks DAG health after deployments:

```python
class DeploymentValidator:
    def validate_dags(self, expected_dags: list) -> ValidationResult:
        """
        Validate that expected DAGs exist and are healthy.

        Checks:
        - DAG exists in Airflow
        - DAG is not in error state
        - DAG has expected tasks
        - DAG can be parsed without errors
        """
        pass

    def run_smoke_test(self, dag_id: str) -> bool:
        """
        Run a quick test of the DAG.
        Trigger with test config and verify completion.
        """
        pass
```

### Part 2: Batch Orchestrator

Create an orchestrator for running multiple DAGs:

```python
class BatchOrchestrator:
    def run_sequence(self, dag_configs: list) -> BatchResult:
        """
        Run DAGs in sequence, waiting for each to complete.

        dag_configs = [
            {"dag_id": "extract", "conf": {...}},
            {"dag_id": "transform", "conf": {...}},
            {"dag_id": "load", "conf": {...}},
        ]
        """
        pass

    def run_parallel(self, dag_configs: list, max_concurrent: int) -> BatchResult:
        """
        Run DAGs in parallel with concurrency limit.
        """
        pass
```

### Part 3: Health Monitor

Create a continuous health monitoring system:

```python
class HealthMonitor:
    def check_system_health(self) -> HealthReport:
        """
        Comprehensive health check:
        - Scheduler status
        - Worker connectivity
        - Database health
        - Recent failure rate
        """
        pass

    def get_failure_summary(self, hours: int = 24) -> dict:
        """
        Get summary of failures in the last N hours.
        """
        pass
```

### Part 4: Operations Reporter

Generate operational reports:

```python
class OperationsReporter:
    def daily_summary(self) -> str:
        """
        Generate daily operations summary.

        Includes:
        - Total runs by state
        - Failed DAGs with details
        - Long-running tasks
        - Resource utilization
        """
        pass

    def export_metrics(self, format: str = "json") -> str:
        """
        Export metrics in specified format.
        """
        pass
```

## Starter Code

See `exercise_12_3_automation_script_starter.py`

## Hints

<details>
<summary>Hint 1: Parallel execution</summary>

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_parallel(self, dag_configs, max_concurrent):
    results = []
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        futures = {
            executor.submit(self._run_single, cfg): cfg
            for cfg in dag_configs
        }
        for future in as_completed(futures):
            config = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                results.append({"error": str(e)})
    return results
```

</details>

<details>
<summary>Hint 2: Health check aggregation</summary>

```python
def check_system_health(self):
    checks = {
        "api": self._check_api(),
        "scheduler": self._check_scheduler(),
        "database": self._check_database(),
    }
    overall = all(c["healthy"] for c in checks.values())
    return {"healthy": overall, "checks": checks}
```

</details>

<details>
<summary>Hint 3: Failure rate calculation</summary>

```python
def _get_failure_rate(self, dag_id, hours):
    runs = self._get_recent_runs(dag_id, hours)
    if not runs:
        return 0.0
    failed = sum(1 for r in runs if r["state"] == "failed")
    return failed / len(runs)
```

</details>

## Success Criteria

- [ ] Deployment validator checks DAG health
- [ ] Smoke tests can be triggered
- [ ] Batch orchestrator handles sequences
- [ ] Parallel execution respects concurrency
- [ ] Health monitor provides comprehensive status
- [ ] Reporter generates useful summaries
- [ ] Error handling is robust

## Testing

```bash
# Run the automation script
cd modules/12-rest-api/solutions
python solution_12_3_automation_script.py

# Expected output:
# - Health check results
# - Validation results
# - Sample batch execution
# - Operations report
```

---

Next: [Module 13: Connections & Secrets →](../../13-connections-secrets/README.md)
