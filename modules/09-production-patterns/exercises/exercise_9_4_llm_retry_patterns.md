# Exercise 9.4: LLM Retry Patterns

## Objective

Build production-grade retry and rate limiting patterns for LLM API calls, demonstrating exponential backoff, cost tracking, fallback routing, and circuit breaker patterns.

## Background

LLM APIs present unique operational challenges that require specialized patterns:

| Challenge          | Impact                            | Solution Pattern                |
| ------------------ | --------------------------------- | ------------------------------- |
| Rate Limits        | 429 errors, blocked requests      | Token bucket rate limiting      |
| API Costs          | Unexpected bills, budget overruns | Cost tracking with alerts       |
| Transient Failures | Timeouts, 5xx errors              | Exponential backoff with jitter |
| Provider Outages   | Complete workflow failures        | Multi-provider fallback chains  |
| Token Limits       | Context window exceeded           | Request chunking and validation |

## Requirements

### Task 1: Intelligent Rate Limiting

Implement a token bucket rate limiter that:

- Tracks tokens per minute (TPM) and requests per minute (RPM)
- Waits automatically when approaching limits
- Provides backpressure to upstream tasks
- Logs rate limit status for monitoring

### Task 2: Exponential Backoff with Jitter

Configure retry behavior that:

- Uses exponential backoff (1s, 2s, 4s, 8s...)
- Adds random jitter to prevent thundering herd
- Distinguishes retryable vs non-retryable errors
- Caps maximum retry delay

### Task 3: Cost Tracking Callbacks

Implement cost monitoring that:

- Tracks token usage per request
- Calculates estimated costs using model pricing
- Alerts when approaching budget thresholds
- Provides per-task and per-DAG cost aggregation

### Task 4: Multi-Provider Fallback

Create a fallback chain that:

- Tries primary provider first
- Falls back to secondary on failure
- Logs which provider succeeded
- Maintains consistent response format

### Task 5: Circuit Breaker Pattern

Implement circuit breaker logic that:

- Opens circuit after N consecutive failures
- Prevents further API calls while open
- Resets after cooldown period
- Provides health status for monitoring

## Starter Code

See `exercise_9_4_llm_retry_patterns_starter.py`

## Hints

<details>
<summary>Hint 1: Token Bucket Implementation</summary>

```python
import time
from dataclasses import dataclass


@dataclass
class TokenBucket:
    capacity: int
    tokens: float
    refill_rate: float  # tokens per second
    last_refill: float

    def consume(self, tokens: int) -> bool:
        self._refill()
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False

    def _refill(self):
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def wait_time(self, tokens: int) -> float:
        self._refill()
        if self.tokens >= tokens:
            return 0
        return (tokens - self.tokens) / self.refill_rate
```

</details>

<details>
<summary>Hint 2: Exponential Backoff with Jitter</summary>

```python
import random
from datetime import timedelta


def calculate_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
    """Calculate delay with exponential backoff and jitter."""
    delay = min(base_delay * (2**attempt), max_delay)
    jitter = random.uniform(0, delay * 0.1)  # 10% jitter
    return delay + jitter


# In task decorator:
@task(
    retries=5,
    retry_delay=timedelta(seconds=1),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=2),
)
def llm_task(): ...
```

</details>

<details>
<summary>Hint 3: Cost Tracking Callback</summary>

```python
def track_llm_cost(context, response_data: dict):
    """Track LLM costs and alert on threshold."""
    from airflow.models import Variable

    # Model pricing (per 1K tokens)
    PRICING = {
        "gpt-4": {"input": 0.03, "output": 0.06},
        "gpt-3.5-turbo": {"input": 0.0015, "output": 0.002},
    }

    model = response_data.get("model", "gpt-3.5-turbo")
    input_tokens = response_data.get("usage", {}).get("prompt_tokens", 0)
    output_tokens = response_data.get("usage", {}).get("completion_tokens", 0)

    pricing = PRICING.get(model, PRICING["gpt-3.5-turbo"])
    cost = (input_tokens * pricing["input"] + output_tokens * pricing["output"]) / 1000

    # Accumulate and check threshold
    total_cost = float(Variable.get("llm_total_cost", default_var="0"))
    total_cost += cost
    Variable.set("llm_total_cost", str(total_cost))

    budget = float(Variable.get("llm_budget", default_var="100"))
    if total_cost > budget * 0.8:
        logger.warning(f"LLM costs at {total_cost / budget:.0%} of budget!")
```

</details>

<details>
<summary>Hint 4: Fallback Chain Pattern</summary>

```python
def call_with_fallback(prompt: str, providers: list[dict]) -> dict:
    """Try providers in order until one succeeds."""
    last_error = None

    for provider in providers:
        try:
            logger.info(f"Trying provider: {provider['name']}")
            response = provider["call_fn"](prompt)
            logger.info(f"Success with provider: {provider['name']}")
            return {
                "provider": provider["name"],
                "response": response,
                "fallback_used": provider != providers[0],
            }
        except Exception as e:
            logger.warning(f"Provider {provider['name']} failed: {e}")
            last_error = e
            continue

    raise Exception(f"All providers failed. Last error: {last_error}")
```

</details>

<details>
<summary>Hint 5: Circuit Breaker</summary>

```python
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass
class CircuitBreaker:
    failure_threshold: int = 5
    reset_timeout: timedelta = timedelta(minutes=5)
    failure_count: int = 0
    last_failure_time: datetime | None = None
    state: str = "closed"  # closed, open, half-open

    def can_execute(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open":
            if datetime.now() - self.last_failure_time > self.reset_timeout:
                self.state = "half-open"
                return True
            return False
        return True  # half-open allows one attempt

    def record_success(self):
        self.failure_count = 0
        self.state = "closed"

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        if self.failure_count >= self.failure_threshold:
            self.state = "open"
```

</details>

## Success Criteria

- [ ] Rate limiter prevents exceeding API limits
- [ ] Retries use exponential backoff with jitter
- [ ] Cost tracking logs usage and alerts on threshold
- [ ] Fallback chain tries multiple providers
- [ ] Circuit breaker prevents cascading failures
- [ ] All patterns are production-ready with logging
- [ ] Error handling distinguishes retryable vs non-retryable errors

## Files

- **Starter**: `exercise_9_4_llm_retry_patterns_starter.py`
- **Solution**: `../solutions/solution_9_4_llm_retry_patterns.py`

## Estimated Time

60-90 minutes

---

Next: [Exercise 9.5 →](exercise_9_5.md) | [Module 11: Sensors →](../../11-sensors-deferrable/)
