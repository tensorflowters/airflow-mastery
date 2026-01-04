# Exercise 15.2: LLM Chain Orchestration

Build a multi-step LLM workflow with rate limiting, cost tracking, fallback model routing, and comprehensive observability.

## Learning Goals

- Design multi-step LLM processing chains
- Implement rate limiting and retry strategies for LLM APIs
- Track and budget LLM costs at the task level
- Build fallback model routing for reliability
- Apply observability patterns for LLM workflows

## Scenario

You're building a content analysis pipeline that processes articles through multiple LLM steps:

1. **Extract**: Pull key entities and facts from the text
2. **Summarize**: Create a concise summary
3. **Classify**: Categorize the content into predefined topics
4. **Score**: Rate the content quality and relevance

Each step should handle rate limits, track costs, and fall back to cheaper models when appropriate.

## Requirements

### Task 1: LLM Client with Rate Limiting

Create a reusable LLM client task that:

- Implements token bucket rate limiting
- Tracks input/output tokens for cost calculation
- Supports configurable retry with exponential backoff
- Logs all API interactions for debugging

### Task 2: Multi-Step Chain

Create a chain of LLM tasks:

- **extract_entities**: Identify people, places, organizations, dates
- **generate_summary**: Create a 2-3 sentence summary
- **classify_content**: Assign category labels (tech, business, science, etc.)
- **score_quality**: Rate relevance and quality (1-10)

Each task should pass its output to the next via XCom.

### Task 3: Cost Tracking Callbacks

Implement callbacks that:

- Track token usage per task
- Calculate cost using model-specific pricing
- Store cumulative costs in Airflow Variables
- Alert when daily budget threshold is reached

### Task 4: Fallback Model Routing

Implement intelligent model selection:

- Route simple tasks to cheaper models (gpt-3.5-turbo)
- Use expensive models (gpt-4) only for complex analysis
- Fallback to backup model on primary model failure
- Track model usage distribution

### Task 5: Pipeline Summary

Create a final task that aggregates:

- Total processing time per step
- Token usage breakdown by step
- Total cost with per-step attribution
- Success/failure rates
- Model usage distribution

## Success Criteria

- [ ] Rate limiting prevents API quota exhaustion
- [ ] Retry logic handles transient failures gracefully
- [ ] Cost tracking accurately reflects token usage
- [ ] Fallback routing maintains pipeline reliability
- [ ] All LLM interactions are logged for debugging
- [ ] Pipeline completes within configured time budget

## Hints

<details>
<summary>Hint 1: Token Bucket Rate Limiter</summary>

```python
import time
from threading import Lock


class TokenBucketRateLimiter:
    """Simple token bucket rate limiter."""

    def __init__(self, tokens_per_second: float, max_tokens: int = 10):
        self.tokens_per_second = tokens_per_second
        self.max_tokens = max_tokens
        self.tokens = max_tokens
        self.last_update = time.time()
        self.lock = Lock()

    def acquire(self, tokens: int = 1) -> float:
        """Acquire tokens, blocking if necessary. Returns wait time."""
        with self.lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0
            wait_time = (tokens - self.tokens) / self.tokens_per_second
            time.sleep(wait_time)
            self._refill()
            self.tokens -= tokens
            return wait_time

    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.tokens_per_second)
        self.last_update = now
```

</details>

<details>
<summary>Hint 2: Cost Calculation</summary>

```python
# OpenAI pricing (as of 2024)
MODEL_PRICING = {
    "gpt-4-turbo": {"input": 0.01, "output": 0.03},  # per 1K tokens
    "gpt-4": {"input": 0.03, "output": 0.06},
    "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
}


def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate cost in USD for an LLM call."""
    pricing = MODEL_PRICING.get(model, MODEL_PRICING["gpt-3.5-turbo"])
    input_cost = (input_tokens / 1000) * pricing["input"]
    output_cost = (output_tokens / 1000) * pricing["output"]
    return input_cost + output_cost
```

</details>

<details>
<summary>Hint 3: Fallback Model Logic</summary>

```python
def select_model(task_complexity: str, primary_failed: bool = False) -> str:
    """Select model based on task complexity and failure state."""
    if primary_failed:
        # Fallback chain: gpt-4 -> gpt-3.5-turbo -> local
        return "gpt-3.5-turbo"

    complexity_map = {
        "simple": "gpt-3.5-turbo",
        "moderate": "gpt-3.5-turbo",
        "complex": "gpt-4",
        "critical": "gpt-4-turbo",
    }
    return complexity_map.get(task_complexity, "gpt-3.5-turbo")
```

</details>

<details>
<summary>Hint 4: Task-Level Cost Tracking Callback</summary>

```python
def track_llm_cost(context):
    """On-success callback to track LLM costs."""
    ti = context["task_instance"]
    usage = ti.xcom_pull(key="token_usage")

    if usage:
        # Update daily spend variable
        daily_key = f"llm_spend_{datetime.now().strftime('%Y-%m-%d')}"
        current_spend = Variable.get(daily_key, default_var=0.0, deserialize_json=True)
        new_spend = current_spend + usage.get("cost", 0)
        Variable.set(daily_key, new_spend, serialize_json=True)

        # Check budget
        if new_spend > DAILY_BUDGET:
            # Trigger alert (webhook, email, etc.)
            send_budget_alert(new_spend, DAILY_BUDGET)
```

</details>

## Files

- **Starter**: `exercise_15_2_llm_chain_starter.py`
- **Solution**: `../solutions/solution_15_2_llm_chain.py`

## Estimated Time

60-90 minutes

## Next Steps

After completing this exercise:

1. Integrate with a real LLM API (OpenAI, Anthropic)
2. Add prompt versioning and A/B testing
3. Implement caching for repeated queries
4. Build a dashboard for cost monitoring
