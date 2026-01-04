# Case Study: Stripe's Fraud Detection Pipelines

## Company Context

**Scale**: Processes billions of dollars in payments annually
**Challenge**: Detect fraudulent transactions in real-time while minimizing false positives
**Requirements**: Sub-100ms inference latency, continuous model updates, regulatory compliance

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STRIPE FRAUD DETECTION                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │   Payment    │───▶│   Feature    │───▶│   Real-Time  │                  │
│  │   Events     │    │   Extraction │    │   Scoring    │                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│        │                                        │                           │
│        ▼                                        ▼                           │
│  ┌──────────────┐                         ┌──────────────┐                  │
│  │   Batch      │                         │   Decision   │                  │
│  │   Pipeline   │                         │   Engine     │                  │
│  │   (Airflow)  │                         │              │                  │
│  └──────────────┘                         └──────────────┘                  │
│        │                                                                    │
│        ▼                                                                    │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │   Feature    │───▶│   Model      │───▶│   Model      │                  │
│  │   Store      │    │   Training   │    │   Validation │                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│                                                │                           │
│                                                ▼                           │
│                                          ┌──────────────┐                  │
│                                          │   Canary     │                  │
│                                          │   Deployment │                  │
│                                          └──────────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Patterns Used

### 1. Production Retry Patterns for ML Inference

Fraud scoring requires high reliability with graceful degradation:

```python
from datetime import timedelta

from airflow.sdk import dag, task


@dag(schedule="@hourly")
def fraud_model_pipeline():
    @task(
        retries=5,
        retry_delay=timedelta(seconds=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
    )
    def score_transactions(batch: list[dict]) -> list[dict]:
        """Score transactions with production-grade retry logic."""
        results = []

        for txn in batch:
            try:
                score = call_fraud_model(txn)
                results.append({"id": txn["id"], "score": score})
            except RateLimitError:
                # Exponential backoff handles this
                raise
            except ModelTimeoutError:
                # Fallback to rule-based scoring
                score = rule_based_fallback(txn)
                results.append(
                    {
                        "id": txn["id"],
                        "score": score,
                        "fallback": True,
                    }
                )

        return results

    @task
    def aggregate_results(scores: list[dict]) -> dict:
        """Aggregate scoring results with SLA tracking."""
        total = len(scores)
        fallbacks = sum(1 for s in scores if s.get("fallback"))

        return {
            "total_scored": total,
            "fallback_rate": fallbacks / total if total else 0,
            "sla_met": fallbacks / total < 0.05,  # 5% threshold
        }
```

### 2. Deferrable Sensors for Model Training Completion

Training jobs run on dedicated infrastructure. Deferrable sensors wait efficiently:

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent


class ModelTrainingTrigger(BaseTrigger):
    """Async trigger for ML training job completion."""

    def __init__(self, job_id: str, poll_interval: float = 30.0):
        super().__init__()
        self.job_id = job_id
        self.poll_interval = poll_interval

    def serialize(self):
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {"job_id": self.job_id, "poll_interval": self.poll_interval},
        )

    async def run(self):
        import asyncio

        while True:
            status = await self._check_job_status()

            if status["state"] == "COMPLETED":
                yield TriggerEvent(
                    {
                        "status": "success",
                        "job_id": self.job_id,
                        "metrics": status["metrics"],
                    }
                )
                return

            if status["state"] == "FAILED":
                yield TriggerEvent(
                    {
                        "status": "error",
                        "message": status.get("error"),
                    }
                )
                return

            await asyncio.sleep(self.poll_interval)


class ModelTrainingSensor(BaseSensorOperator):
    """Deferrable sensor for ML training jobs."""

    def __init__(self, job_id: str, **kwargs):
        super().__init__(**kwargs)
        self.job_id = job_id

    def execute(self, context):
        # Quick check first
        status = check_training_status(self.job_id)
        if status["state"] == "COMPLETED":
            return status["metrics"]

        # Defer to trigger (releases worker slot)
        self.defer(
            trigger=ModelTrainingTrigger(job_id=self.job_id),
            method_name="execute_complete",
        )

    def execute_complete(self, context, event):
        if event["status"] == "success":
            return event["metrics"]
        raise AirflowException(f"Training failed: {event['message']}")
```

### 3. Feature Engineering with Cost Tracking

ML feature computation is expensive. Cost tracking prevents budget overruns:

```python
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class FeatureComputeTracker:
    """Track feature computation costs and performance."""

    budget_limit: float = 1000.0  # Daily compute budget
    current_spend: float = 0.0
    compute_history: list = field(default_factory=list)

    def track_computation(
        self,
        feature_name: str,
        compute_fn: Callable,
        cost_per_row: float,
        row_count: int,
    ):
        """Execute computation with cost tracking."""
        estimated_cost = cost_per_row * row_count

        if self.current_spend + estimated_cost > self.budget_limit:
            raise BudgetExceededError(
                f"Feature {feature_name} would exceed budget: "
                f"${self.current_spend:.2f} + ${estimated_cost:.2f} > "
                f"${self.budget_limit:.2f}"
            )

        # Execute computation
        result = compute_fn()

        # Track actual cost
        self.current_spend += estimated_cost
        self.compute_history.append(
            {
                "feature": feature_name,
                "cost": estimated_cost,
                "rows": row_count,
            }
        )

        return result


@task
def compute_velocity_features(transactions: list[dict]) -> dict:
    """Compute transaction velocity features with cost tracking."""
    tracker = FeatureComputeTracker(budget_limit=500.0)

    # Track each feature computation
    hourly_velocity = tracker.track_computation(
        feature_name="hourly_velocity",
        compute_fn=lambda: calculate_hourly_velocity(transactions),
        cost_per_row=0.001,
        row_count=len(transactions),
    )

    merchant_velocity = tracker.track_computation(
        feature_name="merchant_velocity",
        compute_fn=lambda: calculate_merchant_velocity(transactions),
        cost_per_row=0.002,
        row_count=len(transactions),
    )

    return {
        "features": {
            "hourly_velocity": hourly_velocity,
            "merchant_velocity": merchant_velocity,
        },
        "compute_cost": tracker.current_spend,
    }
```

### 4. Circuit Breaker for External Services

Fraud detection relies on external data sources. Circuit breakers prevent cascading failures:

```python
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class CircuitBreaker:
    """Prevent cascading failures to external services."""

    failure_threshold: int = 5
    reset_timeout: timedelta = timedelta(minutes=5)
    failure_count: int = 0
    last_failure_time: datetime | None = None
    state: str = "closed"  # closed, open, half-open

    def can_execute(self) -> bool:
        """Check if circuit allows execution."""
        if self.state == "closed":
            return True

        if self.state == "open":
            if datetime.now() - self.last_failure_time > self.reset_timeout:
                self.state = "half-open"
                return True
            return False

        return True  # half-open allows one attempt

    def record_success(self):
        """Record successful execution."""
        self.failure_count = 0
        self.state = "closed"

    def record_failure(self):
        """Record failed execution."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = "open"


# Usage in feature enrichment
identity_service_breaker = CircuitBreaker()


@task
def enrich_with_identity_data(transactions: list[dict]) -> list[dict]:
    """Enrich transactions with identity verification data."""
    enriched = []

    for txn in transactions:
        if identity_service_breaker.can_execute():
            try:
                identity_data = call_identity_service(txn["user_id"])
                txn["identity"] = identity_data
                identity_service_breaker.record_success()
            except ServiceError:
                identity_service_breaker.record_failure()
                txn["identity"] = {"status": "unavailable"}
        else:
            # Circuit is open, use cached/default data
            txn["identity"] = get_cached_identity(txn["user_id"])

        enriched.append(txn)

    return enriched
```

## Lessons Learned

### What Worked

1. **Deferrable sensors for training jobs** - Training can take hours; deferrable mode freed worker slots
2. **Circuit breakers for external services** - Prevented identity service outages from blocking all scoring
3. **Cost tracking on feature computation** - Caught runaway queries before they impacted budget
4. **Gradual rollout with shadow scoring** - New models ran alongside production before full deployment

### Challenges Encountered

1. **Latency requirements** - Batch Airflow unsuitable for real-time; used hybrid architecture
2. **Feature consistency** - Training/serving skew required careful feature store design
3. **Regulatory compliance** - Model explainability requirements added pipeline complexity

### Key Metrics

| Metric                         | Before Patterns | After Patterns |
| ------------------------------ | --------------- | -------------- |
| Model training reliability     | 87%             | 99.2%          |
| Feature pipeline cost overruns | 12/month        | 0/month        |
| Service dependency failures    | 8% impact       | <1% impact     |
| False positive rate            | 2.3%            | 1.8%           |

## Code Patterns

### Pattern: Exponential Backoff with Jitter

```python
import random
from datetime import timedelta


def calculate_backoff(attempt: int, base: float = 1.0, max_delay: float = 60.0) -> float:
    """Calculate delay with exponential backoff and jitter."""
    delay = min(base * (2**attempt), max_delay)
    jitter = random.uniform(0, delay * 0.1)  # 10% jitter
    return delay + jitter


# In task decorator
@task(
    retries=5,
    retry_delay=timedelta(seconds=1),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=2),
)
def reliable_scoring(data):
    """Score with automatic exponential backoff."""
    pass
```

### Pattern: Fallback Chain

```python
def score_with_fallback(transaction: dict) -> dict:
    """Try multiple scoring methods in order."""
    providers = [
        {"name": "primary_model", "fn": primary_model_score},
        {"name": "secondary_model", "fn": secondary_model_score},
        {"name": "rule_based", "fn": rule_based_score},
    ]

    for provider in providers:
        try:
            score = provider["fn"](transaction)
            return {
                "score": score,
                "provider": provider["name"],
                "fallback_used": provider != providers[0],
            }
        except Exception as e:
            logger.warning(f"{provider['name']} failed: {e}")
            continue

    raise AllProvidersFailedError("All scoring methods failed")
```

## Related Exercises

| Exercise                                                                                                                 | Concepts Applied                                |
| ------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------- |
| [Exercise 9.4: LLM Retry Patterns](../../modules/09-production-patterns/exercises/exercise_9_4_llm_retry_patterns.md)    | Retry patterns, circuit breakers, cost tracking |
| [Exercise 11.4: Vector Store Sensor](../../modules/11-sensors-deferrable/exercises/exercise_11_4_vector_store_sensor.md) | Deferrable sensors for long operations          |
| [Exercise 15.2: LLM Chain](../../modules/15-ai-ml-orchestration/exercises/exercise_15_2_llm_chain.md)                    | Multi-step ML workflows                         |

## Further Reading

- [Stripe Engineering: Machine Learning Infrastructure](https://stripe.com/blog/engineering)
- [Building Fraud Detection Systems at Scale](https://stripe.com/guides)
- [Apache Airflow for ML Pipelines](https://airflow.apache.org/docs/)

---

[← Spotify Recommendations](spotify-recommendations.md) | [Airbnb Experimentation →](airbnb-experimentation.md)
