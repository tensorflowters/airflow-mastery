"""
LLM Retry Patterns Starter Code.

Build production-grade retry and rate limiting patterns for LLM API calls.

TODO List:
1. Implement TokenBucketRateLimiter class
2. Configure exponential backoff with jitter for retries
3. Implement cost tracking callbacks
4. Create multi-provider fallback chain
5. Implement circuit breaker pattern
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

# Model pricing (per 1K tokens)
MODEL_PRICING = {
    "gpt-4": {"input": 0.03, "output": 0.06},
    "gpt-4-turbo": {"input": 0.01, "output": 0.03},
    "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
    "claude-3-opus": {"input": 0.015, "output": 0.075},
    "claude-3-sonnet": {"input": 0.003, "output": 0.015},
}

# Rate limits (typical OpenAI tier)
RATE_LIMITS = {
    "tokens_per_minute": 90000,
    "requests_per_minute": 500,
}

# Budget configuration
BUDGET_CONFIG = {
    "daily_budget": 50.0,  # dollars
    "warning_threshold": 0.8,  # 80%
    "critical_threshold": 0.95,  # 95%
}


# =============================================================================
# Token Bucket Rate Limiter
# =============================================================================


@dataclass
class TokenBucketRateLimiter:
    """
    Token bucket rate limiter for LLM API calls.

    Tracks both token usage and request count to stay within limits.

    TODO: Implement the following methods:
    - consume(): Attempt to consume tokens, return success/failure
    - wait_for_tokens(): Block until tokens are available
    - get_status(): Return current rate limit status
    """

    # Configuration
    tokens_per_minute: int = RATE_LIMITS["tokens_per_minute"]
    requests_per_minute: int = RATE_LIMITS["requests_per_minute"]

    # State (initialized post-init)
    _token_bucket: float = field(default=0.0, init=False)
    _request_bucket: float = field(default=0.0, init=False)
    _last_refill: float = field(default_factory=time.time, init=False)

    def __post_init__(self):
        """Initialize buckets to full capacity."""
        self._token_bucket = float(self.tokens_per_minute)
        self._request_bucket = float(self.requests_per_minute)
        self._last_refill = time.time()

    def _refill(self) -> None:
        """Refill buckets based on elapsed time."""
        now = time.time()
        _ = (now - self._last_refill) / 60.0  # elapsed_minutes for TODO

        # TODO: Implement bucket refill logic
        # - Calculate tokens to add based on elapsed time (use elapsed_minutes)
        # - Cap at bucket capacity
        # - Update last_refill timestamp

        self._last_refill = now

    def consume(self, tokens: int, requests: int = 1) -> bool:
        """
        Attempt to consume tokens and requests from buckets.

        Args:
            tokens: Number of tokens to consume
            requests: Number of requests to consume (default 1)

        Returns:
            True if consumption succeeded, False otherwise
        """
        # TODO: Implement consumption logic
        # 1. Call _refill() to update buckets
        # 2. Check if both buckets have sufficient capacity
        # 3. If yes, deduct and return True
        # 4. If no, return False

        return False  # Placeholder

    def wait_for_tokens(self, tokens: int, requests: int = 1) -> float:
        """
        Wait until tokens are available, then consume them.

        Args:
            tokens: Number of tokens needed
            requests: Number of requests needed

        Returns:
            Time waited in seconds
        """
        # TODO: Implement waiting logic
        # 1. Calculate wait time needed for both buckets
        # 2. Sleep for the maximum of the two
        # 3. Consume tokens
        # 4. Return total wait time

        return 0.0  # Placeholder

    def get_status(self) -> dict[str, Any]:
        """Return current rate limit status for monitoring."""
        self._refill()
        return {
            "tokens_available": self._token_bucket,
            "tokens_capacity": self.tokens_per_minute,
            "tokens_utilization": 1 - (self._token_bucket / self.tokens_per_minute),
            "requests_available": self._request_bucket,
            "requests_capacity": self.requests_per_minute,
            "requests_utilization": 1 - (self._request_bucket / self.requests_per_minute),
        }


# =============================================================================
# Circuit Breaker
# =============================================================================


@dataclass
class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.

    States:
    - CLOSED: Normal operation, requests flow through
    - OPEN: Failing, requests are rejected immediately
    - HALF_OPEN: Testing recovery, limited requests allowed

    TODO: Implement the circuit breaker pattern
    """

    failure_threshold: int = 5
    reset_timeout_seconds: int = 300  # 5 minutes
    half_open_max_requests: int = 3

    # State
    _failure_count: int = field(default=0, init=False)
    _success_count: int = field(default=0, init=False)
    _last_failure_time: float | None = field(default=None, init=False)
    _state: str = field(default="CLOSED", init=False)
    _half_open_requests: int = field(default=0, init=False)

    def can_execute(self) -> bool:
        """
        Check if a request can be executed.

        Returns:
            True if request should proceed, False if circuit is open
        """
        # TODO: Implement circuit breaker logic
        # 1. If CLOSED, always allow
        # 2. If OPEN, check if reset_timeout has passed
        #    - If yes, transition to HALF_OPEN
        #    - If no, reject request
        # 3. If HALF_OPEN, allow limited requests

        return True  # Placeholder

    def record_success(self) -> None:
        """Record a successful request."""
        # TODO: Implement success recording
        # - If HALF_OPEN, increment success count
        # - If enough successes, transition to CLOSED
        # - Reset failure count
        pass

    def record_failure(self) -> None:
        """Record a failed request."""
        # TODO: Implement failure recording
        # - Increment failure count
        # - Update last_failure_time
        # - If threshold exceeded, transition to OPEN
        pass

    def get_state(self) -> dict[str, Any]:
        """Return current circuit breaker state."""
        return {
            "state": self._state,
            "failure_count": self._failure_count,
            "last_failure": self._last_failure_time,
        }


# =============================================================================
# Cost Tracker
# =============================================================================


@dataclass
class CostTracker:
    """
    Track LLM API costs and enforce budget limits.

    TODO: Implement cost tracking with budget alerts
    """

    daily_budget: float = BUDGET_CONFIG["daily_budget"]
    warning_threshold: float = BUDGET_CONFIG["warning_threshold"]
    critical_threshold: float = BUDGET_CONFIG["critical_threshold"]

    # State
    _daily_cost: float = field(default=0.0, init=False)
    _request_count: int = field(default=0, init=False)
    _total_tokens: int = field(default=0, init=False)
    _last_reset: datetime = field(default_factory=datetime.now, init=False)

    def track_usage(
        self,
        model: str,
        input_tokens: int,
        output_tokens: int,
    ) -> dict[str, Any]:
        """
        Track token usage and calculate cost.

        Args:
            model: Model name (e.g., "gpt-4")
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens

        Returns:
            Usage summary with cost information
        """
        # TODO: Implement cost tracking
        # 1. Look up pricing for model
        # 2. Calculate cost
        # 3. Update running totals
        # 4. Check budget thresholds
        # 5. Return usage summary

        return {
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "cost": 0.0,  # TODO: Calculate
            "daily_total": self._daily_cost,
            "budget_remaining": self.daily_budget - self._daily_cost,
        }

    def check_budget(self) -> dict[str, Any]:
        """Check current budget status and return alerts."""
        # TODO: Implement budget checking
        # - Check if approaching warning threshold
        # - Check if approaching critical threshold
        # - Return status with any alerts

        return {
            "daily_cost": self._daily_cost,
            "daily_budget": self.daily_budget,
            "utilization": self._daily_cost / self.daily_budget,
            "status": "OK",  # TODO: Calculate based on thresholds
            "alerts": [],
        }


# =============================================================================
# Mock LLM Functions
# =============================================================================


def mock_llm_call(
    prompt: str,
    model: str = "gpt-3.5-turbo",
    fail_rate: float = 0.1,
) -> dict[str, Any]:
    """
    Mock LLM API call for testing retry patterns.

    Args:
        prompt: Input prompt
        model: Model to use
        fail_rate: Probability of simulated failure

    Returns:
        Mock response with usage information
    """
    # Simulate API latency
    time.sleep(random.uniform(0.1, 0.3))

    # Simulate random failures
    if random.random() < fail_rate:
        error_types = [
            ("rate_limit", 429, "Rate limit exceeded"),
            ("server_error", 500, "Internal server error"),
            ("timeout", 408, "Request timeout"),
            ("overloaded", 503, "Service temporarily unavailable"),
        ]
        error_type, code, message = random.choice(error_types)
        raise LLMAPIError(error_type, code, message)

    # Calculate mock token counts
    input_tokens = len(prompt.split()) * 2
    output_tokens = random.randint(50, 200)

    return {
        "model": model,
        "content": f"Mock response to: {prompt[:50]}...",
        "usage": {
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
        "provider": "mock",
    }


def mock_fallback_llm_call(
    prompt: str,
    model: str = "claude-3-sonnet",
) -> dict[str, Any]:
    """Mock fallback LLM provider."""
    time.sleep(random.uniform(0.1, 0.2))

    input_tokens = len(prompt.split()) * 2
    output_tokens = random.randint(50, 150)

    return {
        "model": model,
        "content": f"Fallback response to: {prompt[:50]}...",
        "usage": {
            "prompt_tokens": input_tokens,
            "completion_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        },
        "provider": "fallback",
    }


class LLMAPIError(Exception):
    """Custom exception for LLM API errors."""

    def __init__(self, error_type: str, status_code: int, message: str):
        """Initialize LLM API error with type, status code, and message."""
        self.error_type = error_type
        self.status_code = status_code
        self.message = message
        super().__init__(f"{error_type} ({status_code}): {message}")

    def is_retryable(self) -> bool:
        """Check if error is retryable."""
        # TODO: Implement retryable error detection
        # - 429 (rate limit) -> retryable with backoff
        # - 5xx (server errors) -> retryable
        # - 4xx (client errors, except 429) -> not retryable
        return self.status_code in [429, 500, 502, 503, 504, 408]


# =============================================================================
# Retry Helper Functions
# =============================================================================


def calculate_backoff(
    attempt: int,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
) -> float:
    """
    Calculate exponential backoff delay with optional jitter.

    Args:
        attempt: Current attempt number (0-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap
        jitter: Whether to add random jitter

    Returns:
        Delay in seconds
    """
    # TODO: Implement exponential backoff with jitter
    # 1. Calculate exponential delay: base * 2^attempt
    # 2. Cap at max_delay
    # 3. Add jitter if enabled (random 0-10% of delay)

    return base_delay  # Placeholder


def call_with_retry(
    fn,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> Any:
    """
    Call function with exponential backoff retry.

    Args:
        fn: Function to call
        max_retries: Maximum retry attempts
        base_delay: Base delay between retries
        max_delay: Maximum delay cap

    Returns:
        Function result

    Raises:
        Exception: If all retries fail
    """
    # TODO: Implement retry logic
    # 1. Try calling function
    # 2. On retryable error, wait and retry
    # 3. On non-retryable error, raise immediately
    # 4. Track attempt count and log
    # 5. Raise after max_retries exceeded

    return fn()  # Placeholder - no retry


def call_with_fallback(
    prompt: str,
    providers: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Call LLM with fallback chain.

    Args:
        prompt: Input prompt
        providers: List of provider configs with 'name' and 'call_fn'

    Returns:
        Response from first successful provider
    """
    # TODO: Implement fallback chain
    # 1. Try each provider in order
    # 2. On success, return response with provider info
    # 3. On failure, log and try next provider
    # 4. If all fail, raise with last error

    if providers:
        return providers[0]["call_fn"](prompt)
    raise ValueError("No providers configured")


# =============================================================================
# DAG Definition
# =============================================================================


@dag(
    dag_id="llm_retry_patterns",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "ml-team",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        # TODO: Add retry_exponential_backoff=True
        # TODO: Add max_retry_delay
        # TODO: Add on_failure_callback
    },
    tags=["ai-ml", "production", "module-09"],
    doc_md=__doc__,
)
def llm_retry_patterns():
    """
    Production LLM Pipeline with Retry Patterns.

    Demonstrates:
    - Rate limiting with token bucket
    - Exponential backoff with jitter
    - Cost tracking and budget alerts
    - Multi-provider fallback
    - Circuit breaker pattern
    """

    @task
    def get_prompts() -> list[dict[str, Any]]:
        """Get sample prompts for processing."""
        return [
            {"id": "prompt-001", "text": "Summarize the key benefits of cloud computing"},
            {"id": "prompt-002", "text": "Explain machine learning in simple terms"},
            {"id": "prompt-003", "text": "What are best practices for API design?"},
            {"id": "prompt-004", "text": "Describe the importance of data security"},
            {"id": "prompt-005", "text": "How does containerization improve deployment?"},
        ]

    @task(
        retries=5,
        retry_delay=timedelta(seconds=5),
        # TODO: Configure exponential backoff
        # retry_exponential_backoff=True,
        # max_retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=5),
    )
    def process_with_rate_limiting(prompts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Process prompts with rate limiting.

        TODO: Implement rate-limited processing
        1. Initialize TokenBucketRateLimiter
        2. For each prompt, wait for tokens if needed
        3. Call LLM with rate limiting
        4. Track usage statistics
        """
        results = []

        # TODO: Initialize rate limiter
        # rate_limiter = TokenBucketRateLimiter()

        for prompt in prompts:
            prompt_id = prompt.get("id", "unknown")
            prompt_text = prompt.get("text", "")

            # TODO: Wait for rate limit tokens
            # estimated_tokens = len(prompt_text.split()) * 3
            # wait_time = rate_limiter.wait_for_tokens(estimated_tokens)
            # if wait_time > 0:
            #     logger.info(f"Rate limited, waited {wait_time:.2f}s")

            # TODO: Call LLM with retry
            try:
                response = mock_llm_call(prompt_text, fail_rate=0.2)
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": True,
                        "response": response.get("content"),
                        "usage": response.get("usage"),
                    }
                )
            except LLMAPIError as e:
                logger.error(f"LLM call failed for {prompt_id}: {e}")
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": False,
                        "error": str(e),
                    }
                )

        return results

    @task
    def process_with_fallback(prompts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Process prompts with multi-provider fallback.

        TODO: Implement fallback chain
        1. Define provider list with primary and fallback
        2. Try primary provider first
        3. Fall back to secondary on failure
        """
        results = []

        # TODO: Define providers
        providers = [
            {
                "name": "primary",
                "call_fn": lambda p: mock_llm_call(p, model="gpt-4", fail_rate=0.3),
            },
            {
                "name": "fallback",
                "call_fn": lambda p: mock_fallback_llm_call(p, model="claude-3-sonnet"),
            },
        ]

        for prompt in prompts:
            prompt_id = prompt.get("id", "unknown")
            prompt_text = prompt.get("text", "")

            # TODO: Call with fallback chain
            try:
                response = call_with_fallback(prompt_text, providers)
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": True,
                        "provider": response.get("provider"),
                        "response": response.get("content"),
                    }
                )
            except Exception as e:
                logger.error(f"All providers failed for {prompt_id}: {e}")
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": False,
                        "error": str(e),
                    }
                )

        return results

    @task
    def process_with_circuit_breaker(prompts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Process prompts with circuit breaker protection.

        TODO: Implement circuit breaker
        1. Initialize CircuitBreaker
        2. Check can_execute() before each call
        3. Record success/failure after each call
        4. Handle open circuit gracefully
        """
        results = []

        # TODO: Initialize circuit breaker
        # circuit_breaker = CircuitBreaker(failure_threshold=3)

        for prompt in prompts:
            prompt_id = prompt.get("id", "unknown")
            prompt_text = prompt.get("text", "")

            # TODO: Check circuit breaker
            # if not circuit_breaker.can_execute():
            #     logger.warning(f"Circuit open, skipping {prompt_id}")
            #     results.append({
            #         "prompt_id": prompt_id,
            #         "success": False,
            #         "error": "Circuit breaker open",
            #     })
            #     continue

            try:
                response = mock_llm_call(prompt_text, fail_rate=0.4)
                # TODO: circuit_breaker.record_success()
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": True,
                        "response": response.get("content"),
                    }
                )
            except LLMAPIError as e:
                # TODO: circuit_breaker.record_failure()
                logger.error(f"LLM call failed for {prompt_id}: {e}")
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": False,
                        "error": str(e),
                    }
                )

        return results

    @task
    def track_costs(results: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Track costs from LLM responses.

        TODO: Implement cost tracking
        1. Initialize CostTracker
        2. Track usage from each result
        3. Check budget status
        4. Return cost summary with alerts
        """
        # TODO: Initialize cost tracker
        # cost_tracker = CostTracker()

        total_tokens = 0
        total_cost = 0.0
        successful_calls = 0

        for result in results:
            if result.get("success") and result.get("usage"):
                usage = result["usage"]
                total_tokens += usage.get("total_tokens", 0)
                successful_calls += 1

                # TODO: Track actual cost
                # cost_info = cost_tracker.track_usage(
                #     model="gpt-3.5-turbo",
                #     input_tokens=usage.get("prompt_tokens", 0),
                #     output_tokens=usage.get("completion_tokens", 0),
                # )
                # total_cost += cost_info["cost"]

        # TODO: Check budget and get alerts
        # budget_status = cost_tracker.check_budget()

        return {
            "total_requests": len(results),
            "successful_requests": successful_calls,
            "total_tokens": total_tokens,
            "estimated_cost": total_cost,
            "budget_status": "OK",  # TODO: From cost_tracker
        }

    @task
    def aggregate_results(
        rate_limited: list[dict[str, Any]],
        fallback: list[dict[str, Any]],
        circuit_breaker: list[dict[str, Any]],
        costs: dict[str, Any],
    ) -> dict[str, Any]:
        """Aggregate all processing results."""
        all_results = rate_limited + fallback + circuit_breaker
        successful = sum(1 for r in all_results if r.get("success"))

        return {
            "total_processed": len(all_results),
            "successful": successful,
            "failed": len(all_results) - successful,
            "success_rate": successful / len(all_results) if all_results else 0,
            "cost_summary": costs,
            "patterns_demonstrated": [
                "rate_limiting",
                "fallback_chain",
                "circuit_breaker",
                "cost_tracking",
            ],
        }

    # DAG Flow
    prompts = get_prompts()

    # Process with different patterns
    rate_limited = process_with_rate_limiting(prompts)
    fallback = process_with_fallback(prompts)
    circuit_breaker_results = process_with_circuit_breaker(prompts)

    # Track costs
    costs = track_costs(rate_limited)

    # Aggregate results
    aggregate_results(rate_limited, fallback, circuit_breaker_results, costs)


# Instantiate the DAG
llm_retry_patterns()
