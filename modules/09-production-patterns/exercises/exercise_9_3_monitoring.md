# Exercise 9.3: Monitoring Dashboard

## Objective

Design and document a comprehensive monitoring strategy for Airflow, including key metrics, alert thresholds, and dashboard layouts.

## Background

Effective Airflow monitoring covers:

| Category | Focus Area |
|----------|------------|
| **Health** | Scheduler heartbeat, database connections |
| **Performance** | DAG parsing time, task execution latency |
| **Capacity** | Queue depth, pool usage, concurrent tasks |
| **Reliability** | Task failure rate, retry counts |
| **Business** | DAG completion SLAs, data freshness |

## Requirements

### Part 1: Identify Key Metrics

Document the following metrics and their significance:

#### Scheduler Metrics
| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `scheduler_heartbeat` | | |
| `dag_processing.total_parse_time` | | |
| `scheduler.tasks.running` | | |
| `scheduler.tasks.pending` | | |

#### Executor Metrics
| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `executor.queued_tasks` | | |
| `executor.running_tasks` | | |
| `executor.open_slots` | | |

#### Task Metrics
| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `ti.finish.<dag_id>.<task_id>.<state>` | | |
| `ti.duration.<dag_id>.<task_id>` | | |
| `task_instance_created-<state>` | | |

#### Pool Metrics
| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `pool.open_slots.<pool_name>` | | |
| `pool.used_slots.<pool_name>` | | |
| `pool.queued_slots.<pool_name>` | | |

### Part 2: Design Dashboard Layout

Create a Grafana dashboard design with these panels:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        AIRFLOW MONITORING DASHBOARD                      │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │  Scheduler   │  │    Tasks     │  │   Queued     │  │   Failed     │ │
│  │  Heartbeat   │  │   Running    │  │    Tasks     │  │    Tasks     │ │
│  │     ✅       │  │     12       │  │      3       │  │      0       │ │
│  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────┘ │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐│
│  │     Task Execution Trend        │  │      Pool Usage                 ││
│  │   (Line chart over time)        │  │   (Stacked bar chart)          ││
│  │                                 │  │                                 ││
│  └─────────────────────────────────┘  └─────────────────────────────────┘│
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                    DAG Run Duration by DAG                          │ │
│  │                    (Heatmap or bar chart)                           │ │
│  │                                                                     │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────┐  ┌─────────────────────────────────┐│
│  │     DAG Parsing Time            │  │     Task Failure Rate          ││
│  │   (Gauge: target < 30s)         │  │   (Line chart with threshold)  ││
│  │                                 │  │                                 ││
│  └─────────────────────────────────┘  └─────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

### Part 3: Create Alert Rules

Define alerting rules in Prometheus AlertManager format:

```yaml
# alerts.yaml
groups:
  - name: airflow-critical
    rules:
      - alert: AirflowSchedulerDown
        expr: # TODO
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Airflow scheduler is down"
          description: "No heartbeat for 2 minutes"

      - alert: AirflowHighTaskFailureRate
        expr: # TODO
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High task failure rate"
          description: "More than 10% of tasks failing"

      # TODO: Add more alert rules
```

### Part 4: Implement Health Check DAG

Create a DAG that monitors Airflow health:

```python
"""Health check DAG that verifies system connectivity."""

from airflow.sdk import dag, task
from datetime import datetime

@dag(
    dag_id="system_health_check",
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring"],
)
def health_check():
    @task
    def check_database():
        """Verify database connectivity."""
        # TODO: Implement database health check
        pass

    @task
    def check_connections():
        """Verify critical connections are working."""
        # TODO: Check important Airflow connections
        pass

    @task
    def check_variables():
        """Verify critical variables exist."""
        # TODO: Check important Airflow variables
        pass

    @task
    def report_health(db_ok, conn_ok, vars_ok):
        """Report overall health status."""
        # TODO: Aggregate and report
        pass

health_check()
```

## Deliverables

1. **`metrics_specification.md`** - Complete metric documentation
2. **`dashboard_design.json`** - Grafana dashboard JSON (optional)
3. **`alerts.yaml`** - Prometheus alert rules
4. **`health_check_dag.py`** - Health monitoring DAG

## Hints

<details>
<summary>Hint 1: StatsD metric names</summary>

Airflow uses StatsD for metrics. Common metric patterns:
```
# Task metrics
ti.finish.<dag_id>.<task_id>.success
ti.finish.<dag_id>.<task_id>.failed

# Scheduler metrics
scheduler.heartbeat
scheduler.critical_section_duration

# Executor metrics
executor.queued_tasks
executor.running_tasks
```

</details>

<details>
<summary>Hint 2: Grafana panel query</summary>

```promql
# Tasks completed in last hour by state
sum(rate(airflow_ti_finish_total[1h])) by (state)

# Average DAG parse time
avg(airflow_dag_processing_total_parse_time)
```

</details>

<details>
<summary>Hint 3: PagerDuty integration</summary>

```yaml
alertmanager.yml:
receivers:
  - name: pagerduty
    pagerduty_configs:
      - service_key: <your-key>
        severity: '{{ .CommonLabels.severity }}'
```

</details>

## Success Criteria

- [ ] All key metrics documented with descriptions
- [ ] Alert thresholds defined with rationale
- [ ] Dashboard design covers all monitoring categories
- [ ] Prometheus alert rules are syntactically correct
- [ ] Health check DAG verifies critical components
- [ ] Escalation paths defined for critical alerts

## Bonus Challenges

1. **Create actual Grafana dashboard** using the design
2. **Implement custom metrics** for business KPIs
3. **Set up log-based alerts** using Loki/CloudWatch
4. **Create runbook** for each alert type

---

## References

- [Airflow Metrics](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/metrics.html)
- [Prometheus AlertManager](https://prometheus.io/docs/alerting/latest/configuration/)
- [Grafana Dashboard Design](https://grafana.com/docs/grafana/latest/dashboards/)
