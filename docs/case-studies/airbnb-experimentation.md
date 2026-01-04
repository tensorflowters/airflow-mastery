# Case Study: Airbnb's Experimentation Platform

## Company Context

**Scale**: 7+ million listings, 150+ million users, thousands of concurrent experiments
**Challenge**: Orchestrate A/B tests across search, pricing, and booking flows
**Requirements**: Statistically rigorous analysis, rapid experiment iteration, cross-platform consistency

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       AIRBNB EXPERIMENTATION                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │   Experiment │───▶│   User       │───▶│   Event      │                  │
│  │   Config     │    │   Assignment │    │   Collection │                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│        │                                        │                           │
│        ▼                                        ▼                           │
│  ┌──────────────┐                         ┌──────────────┐                  │
│  │   Feature    │                         │   Data       │                  │
│  │   Flags      │                         │   Warehouse  │                  │
│  └──────────────┘                         └──────────────┘                  │
│                                                 │                           │
│                                                 ▼                           │
│  ┌──────────────────────────────────────────────────────────────┐          │
│  │                    AIRFLOW ORCHESTRATION                      │          │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐              │          │
│  │  │  Metrics   │  │ Analysis   │  │ Decision   │              │          │
│  │  │  Pipeline  │──│  Pipeline  │──│  Pipeline  │              │          │
│  │  └────────────┘  └────────────┘  └────────────┘              │          │
│  └──────────────────────────────────────────────────────────────┘          │
│                                                 │                           │
│                                                 ▼                           │
│                                          ┌──────────────┐                  │
│                                          │   Results    │                  │
│                                          │   Dashboard  │                  │
│                                          └──────────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Patterns Used

### 1. Dynamic Task Mapping for Experiment Analysis

Each experiment requires independent statistical analysis. Dynamic mapping scales automatically:

```python
from datetime import datetime

from airflow.sdk import dag, task


@dag(schedule="@daily", start_date=datetime(2024, 1, 1))
def experiment_analysis_pipeline():
    @task
    def get_active_experiments() -> list[dict]:
        """Fetch all active experiments requiring analysis."""
        return [
            {
                "id": "search_ranking_v2",
                "variants": ["control", "treatment_a", "treatment_b"],
                "metrics": ["bookings", "revenue", "search_clicks"],
            },
            {
                "id": "pricing_algorithm",
                "variants": ["control", "ml_pricing"],
                "metrics": ["bookings", "revenue", "host_earnings"],
            },
            {
                "id": "checkout_flow",
                "variants": ["control", "simplified"],
                "metrics": ["conversion", "time_to_book", "abandonment"],
            },
        ]

    @task
    def analyze_experiment(experiment: dict) -> dict:
        """Run statistical analysis for a single experiment."""
        experiment_id = experiment["id"]
        variants = experiment["variants"]
        metrics = experiment["metrics"]

        results = {
            "experiment_id": experiment_id,
            "analysis_date": datetime.now().isoformat(),
            "metrics": {},
        }

        for metric in metrics:
            # Calculate per-metric statistics
            stats = calculate_metric_statistics(
                experiment_id=experiment_id,
                variants=variants,
                metric=metric,
            )

            results["metrics"][metric] = {
                "control_mean": stats["control_mean"],
                "treatment_mean": stats["treatment_mean"],
                "lift": stats["lift"],
                "p_value": stats["p_value"],
                "significant": stats["p_value"] < 0.05,
                "confidence_interval": stats["ci"],
            }

        return results

    @task
    def aggregate_results(analyses: list[dict]) -> dict:
        """Combine all experiment analyses into summary."""
        significant_count = sum(1 for a in analyses if any(m["significant"] for m in a["metrics"].values()))

        return {
            "total_experiments": len(analyses),
            "significant_results": significant_count,
            "experiments": analyses,
        }

    @task
    def publish_results(summary: dict):
        """Publish to dashboard and notify stakeholders."""
        # Update experimentation dashboard
        update_dashboard(summary)

        # Notify teams with significant results
        for exp in summary["experiments"]:
            if any(m["significant"] for m in exp["metrics"].values()):
                notify_team(exp["experiment_id"], exp)

    # Dynamic mapping: analyze each experiment in parallel
    experiments = get_active_experiments()
    analyses = analyze_experiment.expand(experiment=experiments)
    summary = aggregate_results(analyses)
    publish_results(summary)
```

### 2. Metrics Pipeline with Quality Gates

Experiment metrics require validation before analysis:

```python
from airflow.sdk import dag, task


@dag(schedule="@daily")
def experiment_metrics_pipeline():
    @task
    def extract_raw_events(experiment_id: str, date: str) -> dict:
        """Extract raw experiment events from data warehouse."""
        events = query_warehouse(
            f"""
            SELECT user_id, variant, event_type, timestamp, properties
            FROM events
            WHERE experiment_id = '{experiment_id}'
              AND date = '{date}'
            """
        )
        return {"experiment_id": experiment_id, "event_count": len(events)}

    @task
    def validate_data_quality(extraction_result: dict) -> dict:
        """Validate data meets quality thresholds."""
        checks = {
            "sufficient_sample_size": extraction_result["event_count"] >= 1000,
            "balanced_variants": check_variant_balance(extraction_result["experiment_id"]),
            "no_assignment_bias": check_assignment_randomness(extraction_result["experiment_id"]),
        }

        passed = all(checks.values())

        return {
            "experiment_id": extraction_result["experiment_id"],
            "quality_passed": passed,
            "checks": checks,
        }

    def decide_analysis_path(validation_result: dict) -> str:
        """Branch based on data quality validation."""
        if validation_result["quality_passed"]:
            return "run_full_analysis"
        return "flag_for_review"

    @task
    def run_full_analysis(validation_result: dict) -> dict:
        """Execute statistical analysis on validated data."""
        return calculate_experiment_statistics(validation_result["experiment_id"])

    @task
    def flag_for_review(validation_result: dict):
        """Flag experiment for manual data quality review."""
        create_review_ticket(
            experiment_id=validation_result["experiment_id"],
            failed_checks=[k for k, v in validation_result["checks"].items() if not v],
        )
```

### 3. Cross-Experiment Interaction Detection

Running multiple experiments requires detecting interactions:

```python
@dag(schedule="@weekly")
def experiment_interaction_detection():
    @task
    def get_experiment_pairs() -> list[tuple[str, str]]:
        """Generate pairs of experiments to check for interactions."""
        active_experiments = get_active_experiments()
        pairs = []

        for i, exp1 in enumerate(active_experiments):
            for exp2 in active_experiments[i + 1 :]:
                # Only check overlapping experiments
                if experiments_overlap(exp1, exp2):
                    pairs.append((exp1["id"], exp2["id"]))

        return pairs

    @task
    def detect_interaction(pair: tuple[str, str]) -> dict:
        """Detect statistical interaction between experiment pair."""
        exp1_id, exp2_id = pair

        # Get users in both experiments
        overlap_users = get_overlapping_users(exp1_id, exp2_id)

        if len(overlap_users) < 100:
            return {
                "pair": pair,
                "interaction_detected": False,
                "reason": "insufficient_overlap",
            }

        # Run interaction analysis
        interaction_stats = calculate_interaction_effect(exp1_id, exp2_id, overlap_users)

        return {
            "pair": pair,
            "interaction_detected": interaction_stats["significant"],
            "interaction_effect": interaction_stats["effect_size"],
            "p_value": interaction_stats["p_value"],
        }

    @task
    def report_interactions(results: list[dict]) -> dict:
        """Report any detected interactions to experiment owners."""
        interactions = [r for r in results if r["interaction_detected"]]

        for interaction in interactions:
            alert_experiment_owners(
                experiments=interaction["pair"],
                effect=interaction["interaction_effect"],
            )

        return {
            "pairs_analyzed": len(results),
            "interactions_found": len(interactions),
        }

    pairs = get_experiment_pairs()
    interaction_results = detect_interaction.expand(pair=pairs)
    report_interactions(interaction_results)
```

### 4. Experiment Lifecycle Management

Experiments have defined lifecycles managed through Airflow:

```python
from datetime import datetime

from airflow.sdk import dag, task


@dag(schedule="@hourly")
def experiment_lifecycle_manager():
    @task
    def check_experiment_status() -> list[dict]:
        """Check status of all experiments."""
        experiments = get_all_experiments()
        status_updates = []

        for exp in experiments:
            if exp["state"] == "ramping":
                # Check if ramp-up complete
                if datetime.now() > exp["ramp_end_date"]:
                    status_updates.append(
                        {
                            "id": exp["id"],
                            "action": "complete_ramp",
                            "new_allocation": 1.0,
                        }
                    )

            elif exp["state"] == "running":
                # Check if experiment has reached statistical power
                power = calculate_current_power(exp["id"])
                if power >= 0.8:
                    status_updates.append(
                        {
                            "id": exp["id"],
                            "action": "mark_powered",
                            "power": power,
                        }
                    )

                # Check for early stopping (harm detection)
                if detect_significant_harm(exp["id"]):
                    status_updates.append(
                        {
                            "id": exp["id"],
                            "action": "emergency_stop",
                            "reason": "significant_harm_detected",
                        }
                    )

            elif exp["state"] == "analyzing":
                # Check if analysis window complete
                if datetime.now() > exp["analysis_end_date"]:
                    status_updates.append(
                        {
                            "id": exp["id"],
                            "action": "complete_analysis",
                        }
                    )

        return status_updates

    @task
    def apply_status_updates(updates: list[dict]):
        """Apply lifecycle updates to experiments."""
        for update in updates:
            exp_id = update["id"]
            action = update["action"]

            if action == "complete_ramp":
                update_experiment_allocation(exp_id, 1.0)
                transition_experiment_state(exp_id, "running")

            elif action == "mark_powered":
                log_experiment_powered(exp_id, update["power"])

            elif action == "emergency_stop":
                stop_experiment(exp_id, reason=update["reason"])
                notify_experiment_owners(exp_id, "EMERGENCY_STOP", update)

            elif action == "complete_analysis":
                transition_experiment_state(exp_id, "complete")
                generate_final_report(exp_id)

    updates = check_experiment_status()
    apply_status_updates(updates)
```

## Lessons Learned

### What Worked

1. **Dynamic mapping for experiment parallelism** - Scaled from 10 to 1000+ concurrent experiments
2. **Quality gates before analysis** - Prevented bad data from polluting results
3. **Automated interaction detection** - Caught cross-experiment effects early
4. **Lifecycle automation** - Reduced manual experiment management overhead

### Challenges Encountered

1. **Backfill complexity** - Historical analysis required careful date handling
2. **Metric definition consistency** - Required centralized metric catalog
3. **Real-time vs. batch tradeoffs** - Some decisions needed faster signals

### Key Metrics

| Metric                  | Before Airflow     | After Airflow     |
| ----------------------- | ------------------ | ----------------- |
| Experiments per quarter | 50                 | 500+              |
| Analysis latency        | 48 hours           | 4 hours           |
| Data quality issues     | 15% of experiments | 2% of experiments |
| Interaction detection   | Manual             | Automated         |

## Code Patterns

### Pattern: Parallel Experiment Analysis

```python
# From Module 06: Dynamic Task Mapping
@task
def get_experiments() -> list[dict]:
    return [{"id": "exp1"}, {"id": "exp2"}, {"id": "exp3"}]


@task
def analyze(exp: dict) -> dict:
    """Parallel analysis for each experiment."""
    return {
        "id": exp["id"],
        "result": run_analysis(exp["id"]),
    }


# Automatic parallelization
experiments = get_experiments()
results = analyze.expand(exp=experiments)
```

### Pattern: Statistical Quality Gates

```python
@task
def validate_before_analysis(experiment_id: str) -> bool:
    """Gate analysis on data quality."""
    checks = [
        check_sample_size(experiment_id) >= 1000,
        check_variant_balance(experiment_id) > 0.95,
        check_srm_test(experiment_id)["passed"],
    ]
    return all(checks)
```

## Related Exercises

| Exercise                                                                                                              | Concepts Applied                        |
| --------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| [Exercise 6.4: Parallel Embeddings](../../modules/06-dynamic-tasks/exercises/exercise_6_4_parallel_embeddings.md)     | Dynamic mapping for parallel processing |
| [Exercise 9.4: LLM Retry Patterns](../../modules/09-production-patterns/exercises/exercise_9_4_llm_retry_patterns.md) | Quality gates and validation patterns   |
| [Exercise 15.3: Data Prep](../../modules/15-ai-ml-orchestration/exercises/exercise_15_3_data_prep.md)                 | Data quality pipelines                  |

## Further Reading

- [Airbnb Engineering: Experimentation Platform](https://medium.com/airbnb-engineering)
- [Building a Scalable Experimentation Platform](https://airbnb.io/experiments/)
- [Statistical Rigor in A/B Testing](https://www.experimentcalculator.com/)

---

[← Stripe Fraud Detection](stripe-fraud-detection.md) | [Modern RAG Architecture →](modern-rag-architecture.md)
