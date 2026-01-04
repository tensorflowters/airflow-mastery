"""
LLM Retry Patterns - Complete Solution.

This solution demonstrates production-grade retry and rate limiting patterns
for LLM API calls, including exponential backoff, cost tracking, fallback
routing, and circuit breaker patterns.

Key patterns demonstrated:
- Token bucket rate limiting for API quota management
- Exponential backoff with jitter for transient failures
- Cost tracking with budget alerts
- Multi-provider fallback chains
- Circuit breaker for failure isolation
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

    Implements the token bucket algorithm to enforce rate limits on both
    token usage (TPM) and request count (RPM). The bucket refills gradually
    over time, allowing burst traffic while maintaining average limits.
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
        """Refill buckets based on elapsed time since last refill."""
        now = time.time()
        elapsed_minutes = (now - self._last_refill) / 60.0

        # Refill token bucket
        tokens_to_add = elapsed_minutes * self.tokens_per_minute
        self._token_bucket = min(
            self.tokens_per_minute,
            self._token_bucket + tokens_to_add,
        )

        # Refill request bucket
        requests_to_add = elapsed_minutes * self.requests_per_minute
        self._request_bucket = min(
            self.requests_per_minute,
            self._request_bucket + requests_to_add,
        )

        self._last_refill = now

    def consume(self, tokens: int, requests: int = 1) -> bool:
        """
        Attempt to consume tokens and requests from buckets.

        Args:
            tokens: Number of tokens to consume
            requests: Number of requests to consume (default 1)

        Returns:
            True if consumption succeeded, False if insufficient capacity
        """
        self._refill()

        # Check if both buckets have sufficient capacity
        if self._token_bucket >= tokens and self._request_bucket >= requests:
            self._token_bucket -= tokens
            self._request_bucket -= requests
            return True

        return False

    def wait_for_tokens(self, tokens: int, requests: int = 1) -> float:
        """
        Wait until tokens are available, then consume them.

        Args:
            tokens: Number of tokens needed
            requests: Number of requests needed

        Returns:
            Time waited in seconds
        """
        start_time = time.time()

        while not self.consume(tokens, requests):
            # Calculate wait time for each bucket
            self._refill()

            token_wait = 0.0
            if self._token_bucket < tokens:
                tokens_needed = tokens - self._token_bucket
                token_wait = (tokens_needed / self.tokens_per_minute) * 60

            request_wait = 0.0
            if self._request_bucket < requests:
                requests_needed = requests - self._request_bucket
                request_wait = (requests_needed / self.requests_per_minute) * 60

            # Wait for the longer of the two
            wait_time = max(token_wait, request_wait, 0.1)  # Min 100ms
            time.sleep(min(wait_time, 5.0))  # Cap at 5s per iteration

        return time.time() - start_time

    def get_status(self) -> dict[str, Any]:
        """Return current rate limit status for monitoring."""
        self._refill()
        return {
            "tokens_available": round(self._token_bucket, 2),
            "tokens_capacity": self.tokens_per_minute,
            "tokens_utilization": round(1 - (self._token_bucket / self.tokens_per_minute), 3),
            "requests_available": round(self._request_bucket, 2),
            "requests_capacity": self.requests_per_minute,
            "requests_utilization": round(1 - (self._request_bucket / self.requests_per_minute), 3),
        }


# =============================================================================
# Circuit Breaker
# =============================================================================


@dataclass
class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.

    Implements the circuit breaker pattern with three states:
    - CLOSED: Normal operation, requests flow through
    - OPEN: Failing, requests are rejected immediately
    - HALF_OPEN: Testing recovery, limited requests allowed

    The circuit opens after consecutive failures exceed the threshold,
    then transitions to half-open after a cooldown period to test recovery.
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
        if self._state == "CLOSED":
            return True

        if self._state == "OPEN":
            # Check if reset timeout has passed
            if self._last_failure_time is not None:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self.reset_timeout_seconds:
                    logger.info("Circuit breaker transitioning to HALF_OPEN")
                    self._state = "HALF_OPEN"
                    self._half_open_requests = 0
                    return True
            return False

        # HALF_OPEN state - allow limited requests
        if self._half_open_requests < self.half_open_max_requests:
            self._half_open_requests += 1
            return True

        return False

    def record_success(self) -> None:
        """Record a successful request."""
        if self._state == "HALF_OPEN":
            self._success_count += 1
            # Reset to CLOSED after enough successes
            if self._success_count >= self.half_open_max_requests:
                logger.info("Circuit breaker transitioning to CLOSED")
                self._state = "CLOSED"
                self._failure_count = 0
                self._success_count = 0
        else:
            self._failure_count = 0

    def record_failure(self) -> None:
        """Record a failed request."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == "HALF_OPEN":
            # Any failure in half-open returns to open
            logger.warning("Circuit breaker returning to OPEN (half-open failure)")
            self._state = "OPEN"
            self._half_open_requests = 0
        elif self._failure_count >= self.failure_threshold:
            logger.warning(f"Circuit breaker OPEN after {self._failure_count} failures")
            self._state = "OPEN"

    def get_state(self) -> dict[str, Any]:
        """Return current circuit breaker state for monitoring."""
        return {
            "state": self._state,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure": self._last_failure_time,
            "half_open_requests": self._half_open_requests,
        }


# =============================================================================
# Cost Tracker
# =============================================================================


@dataclass
class CostTracker:
    """
    Track LLM API costs and enforce budget limits.

    Calculates costs based on model-specific token pricing and
    monitors against configurable budget thresholds.
    """

    daily_budget: float = BUDGET_CONFIG["daily_budget"]
    warning_threshold: float = BUDGET_CONFIG["warning_threshold"]
    critical_threshold: float = BUDGET_CONFIG["critical_threshold"]

    # State
    _daily_cost: float = field(default=0.0, init=False)
    _request_count: int = field(default=0, init=False)
    _total_input_tokens: int = field(default=0, init=False)
    _total_output_tokens: int = field(default=0, init=False)
    _last_reset: datetime = field(default_factory=datetime.now, init=False)

    def _check_daily_reset(self) -> None:
        """Reset daily counters if new day."""
        now = datetime.now()
        if now.date() > self._last_reset.date():
            logger.info("Resetting daily cost counters")
            self._daily_cost = 0.0
            self._request_count = 0
            self._total_input_tokens = 0
            self._total_output_tokens = 0
            self._last_reset = now

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
            input_tokens: Number of input/prompt tokens
            output_tokens: Number of output/completion tokens

        Returns:
            Usage summary with cost information
        """
        self._check_daily_reset()

        # Get pricing for model (default to gpt-3.5 if unknown)
        pricing = MODEL_PRICING.get(model, MODEL_PRICING["gpt-3.5-turbo"])

        # Calculate cost (pricing is per 1K tokens)
        input_cost = (input_tokens / 1000) * pricing["input"]
        output_cost = (output_tokens / 1000) * pricing["output"]
        total_cost = input_cost + output_cost

        # Update counters
        self._daily_cost += total_cost
        self._request_count += 1
        self._total_input_tokens += input_tokens
        self._total_output_tokens += output_tokens

        # Check thresholds
        utilization = self._daily_cost / self.daily_budget
        alerts = []

        if utilization >= self.critical_threshold:
            alerts.append("CRITICAL: Budget nearly exhausted!")
            logger.error(f"LLM budget critical: {utilization:.1%} utilized")
        elif utilization >= self.warning_threshold:
            alerts.append("WARNING: Approaching budget limit")
            logger.warning(f"LLM budget warning: {utilization:.1%} utilized")

        return {
            "model": model,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "request_cost": round(total_cost, 6),
            "daily_total": round(self._daily_cost, 4),
            "budget_remaining": round(self.daily_budget - self._daily_cost, 4),
            "budget_utilization": round(utilization, 4),
            "alerts": alerts,
        }

    def check_budget(self) -> dict[str, Any]:
        """Check current budget status and return alerts."""
        self._check_daily_reset()

        utilization = self._daily_cost / self.daily_budget
        alerts = []

        if utilization >= self.critical_threshold:
            status = "CRITICAL"
            alerts.append("Budget nearly exhausted - consider pausing operations")
        elif utilization >= self.warning_threshold:
            status = "WARNING"
            alerts.append("Approaching budget limit - monitor closely")
        else:
            status = "OK"

        return {
            "daily_cost": round(self._daily_cost, 4),
            "daily_budget": self.daily_budget,
            "utilization": round(utilization, 4),
            "status": status,
            "alerts": alerts,
            "request_count": self._request_count,
            "total_input_tokens": self._total_input_tokens,
            "total_output_tokens": self._total_output_tokens,
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

    Raises:
        LLMAPIError: On simulated failures
    """
    # Simulate API latency
    time.sleep(random.uniform(0.05, 0.15))

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
        "provider": "primary",
    }


def mock_fallback_llm_call(
    prompt: str,
    model: str = "claude-3-sonnet",
) -> dict[str, Any]:
    """Mock fallback LLM provider (more reliable, possibly slower)."""
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
        """
        Check if error is retryable.

        Retryable errors:
        - 429: Rate limit (retry with backoff)
        - 408: Timeout (retry immediately)
        - 5xx: Server errors (retry with backoff)

        Non-retryable errors:
        - 400: Bad request
        - 401: Authentication
        - 403: Forbidden
        - 404: Not found
        """
        return self.status_code in [429, 408, 500, 502, 503, 504]


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

    Uses the formula: delay = min(base * 2^attempt, max_delay)
    Jitter adds randomness to prevent thundering herd problem.

    Args:
        attempt: Current attempt number (0-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap
        jitter: Whether to add random jitter

    Returns:
        Delay in seconds
    """
    # Exponential backoff
    delay = min(base_delay * (2**attempt), max_delay)

    # Add jitter (0-10% of delay)
    if jitter:
        jitter_amount = random.uniform(0, delay * 0.1)
        delay += jitter_amount

    return delay


def call_with_retry(
    fn,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
) -> Any:
    """
    Call function with exponential backoff retry.

    Args:
        fn: Function to call (no arguments)
        max_retries: Maximum retry attempts
        base_delay: Base delay between retries
        max_delay: Maximum delay cap

    Returns:
        Function result

    Raises:
        Exception: If all retries fail or non-retryable error
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return fn()
        except LLMAPIError as e:
            last_exception = e

            if not e.is_retryable():
                logger.error(f"Non-retryable error: {e}")
                raise

            if attempt < max_retries:
                delay = calculate_backoff(attempt, base_delay, max_delay)
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f}s...")
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries + 1} attempts failed")
        except Exception as e:
            last_exception = e
            logger.error(f"Unexpected error: {e}")
            raise

    raise last_exception if last_exception else RuntimeError("Retry failed")


def call_with_fallback(
    prompt: str,
    providers: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Call LLM with fallback chain.

    Tries each provider in order until one succeeds.

    Args:
        prompt: Input prompt
        providers: List of provider configs with 'name' and 'call_fn'

    Returns:
        Response from first successful provider

    Raises:
        Exception: If all providers fail
    """
    if not providers:
        raise ValueError("No providers configured")

    last_error = None

    for provider in providers:
        provider_name = provider.get("name", "unknown")
        call_fn = provider.get("call_fn")

        if not call_fn:
            continue

        try:
            logger.info(f"Trying provider: {provider_name}")
            response = call_fn(prompt)

            # Add provider info to response
            response["provider_used"] = provider_name
            response["fallback_used"] = provider != providers[0]

            logger.info(f"Success with provider: {provider_name}")
            return response

        except Exception as e:
            logger.warning(f"Provider {provider_name} failed: {e}")
            last_error = e
            continue

    raise RuntimeError(f"All providers failed. Last error: {last_error}")


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
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
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
            {
                "id": "prompt-001",
                "text": "Summarize the key benefits of cloud computing",
            },
            {"id": "prompt-002", "text": "Explain machine learning in simple terms"},
            {"id": "prompt-003", "text": "What are best practices for API design?"},
            {"id": "prompt-004", "text": "Describe the importance of data security"},
            {
                "id": "prompt-005",
                "text": "How does containerization improve deployment?",
            },
        ]

    @task(
        retries=5,
        retry_delay=timedelta(seconds=5),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=5),
    )
    def process_with_rate_limiting(
        prompts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Process prompts with rate limiting.

        Uses token bucket algorithm to stay within API rate limits.
        """
        results = []
        rate_limiter = TokenBucketRateLimiter()
        cost_tracker = CostTracker()

        logger.info(f"Processing {len(prompts)} prompts with rate limiting")
        logger.info(f"Rate limiter status: {rate_limiter.get_status()}")

        for prompt in prompts:
            prompt_id = prompt.get("id", "unknown")
            prompt_text = prompt.get("text", "")

            # Estimate tokens and wait if needed
            estimated_tokens = len(prompt_text.split()) * 3
            wait_time = rate_limiter.wait_for_tokens(estimated_tokens)

            if wait_time > 0.1:
                logger.info(f"Rate limited for {prompt_id}, waited {wait_time:.2f}s")

            # Call LLM with retry
            try:
                response = call_with_retry(
                    lambda p=prompt_text: mock_llm_call(p, fail_rate=0.2),
                    max_retries=3,
                    base_delay=1.0,
                )

                # Track costs
                usage = response.get("usage", {})
                cost_info = cost_tracker.track_usage(
                    model=response.get("model", "gpt-3.5-turbo"),
                    input_tokens=usage.get("prompt_tokens", 0),
                    output_tokens=usage.get("completion_tokens", 0),
                )

                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": True,
                        "response": response.get("content"),
                        "usage": usage,
                        "cost": cost_info.get("request_cost"),
                    }
                )

            except LLMAPIError as e:
                logger.error(f"LLM call failed for {prompt_id}: {e}")
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": False,
                        "error": str(e),
                        "retryable": e.is_retryable(),
                    }
                )

        # Log final status
        logger.info(f"Rate limiter final status: {rate_limiter.get_status()}")
        logger.info(f"Cost tracker status: {cost_tracker.check_budget()}")

        return results

    @task
    def process_with_fallback(prompts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Process prompts with multi-provider fallback.

        Tries primary provider first, falls back to secondary on failure.
        """
        results = []

        # Define providers in priority order
        providers = [
            {
                "name": "primary-gpt4",
                "call_fn": lambda p: mock_llm_call(p, model="gpt-4", fail_rate=0.3),
            },
            {
                "name": "fallback-claude",
                "call_fn": lambda p: mock_fallback_llm_call(p, model="claude-3-sonnet"),
            },
        ]

        logger.info(f"Processing {len(prompts)} prompts with fallback chain")

        for prompt in prompts:
            prompt_id = prompt.get("id", "unknown")
            prompt_text = prompt.get("text", "")

            try:
                response = call_with_fallback(prompt_text, providers)
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": True,
                        "provider": response.get("provider_used"),
                        "fallback_used": response.get("fallback_used", False),
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

        # Log fallback statistics
        fallback_count = sum(1 for r in results if r.get("fallback_used"))
        logger.info(f"Fallback used for {fallback_count}/{len(results)} requests")

        return results

    @task
    def process_with_circuit_breaker(
        prompts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Process prompts with circuit breaker protection.

        Circuit breaker prevents cascading failures by failing fast
        when error rate exceeds threshold.
        """
        results = []
        circuit_breaker = CircuitBreaker(failure_threshold=3, reset_timeout_seconds=60)

        logger.info(f"Processing {len(prompts)} prompts with circuit breaker")

        for prompt in prompts:
            prompt_id = prompt.get("id", "unknown")
            prompt_text = prompt.get("text", "")

            # Check circuit breaker before calling
            if not circuit_breaker.can_execute():
                logger.warning(f"Circuit open, skipping {prompt_id}")
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": False,
                        "error": "Circuit breaker open",
                        "circuit_state": circuit_breaker.get_state(),
                    }
                )
                continue

            try:
                # High fail rate to demonstrate circuit breaker
                response = mock_llm_call(prompt_text, fail_rate=0.4)
                circuit_breaker.record_success()

                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": True,
                        "response": response.get("content"),
                        "circuit_state": circuit_breaker.get_state(),
                    }
                )

            except LLMAPIError as e:
                circuit_breaker.record_failure()
                logger.error(f"LLM call failed for {prompt_id}: {e}")
                results.append(
                    {
                        "prompt_id": prompt_id,
                        "success": False,
                        "error": str(e),
                        "circuit_state": circuit_breaker.get_state(),
                    }
                )

        # Log circuit breaker final state
        logger.info(f"Circuit breaker final state: {circuit_breaker.get_state()}")

        return results

    @task
    def track_costs(results: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Track costs from LLM responses.

        Aggregates token usage and calculates total cost.
        """
        cost_tracker = CostTracker()

        total_tokens = 0
        successful_calls = 0

        for result in results:
            if result.get("success") and result.get("usage"):
                usage = result["usage"]
                total_tokens += usage.get("total_tokens", 0)
                successful_calls += 1

                # Track actual cost
                cost_tracker.track_usage(
                    model="gpt-3.5-turbo",
                    input_tokens=usage.get("prompt_tokens", 0),
                    output_tokens=usage.get("completion_tokens", 0),
                )

        # Get budget status
        budget_status = cost_tracker.check_budget()

        return {
            "total_requests": len(results),
            "successful_requests": successful_calls,
            "total_tokens": total_tokens,
            "estimated_cost": budget_status.get("daily_cost", 0),
            "budget_status": budget_status.get("status", "OK"),
            "budget_utilization": budget_status.get("utilization", 0),
            "alerts": budget_status.get("alerts", []),
        }

    @task
    def aggregate_results(
        rate_limited: list[dict[str, Any]],
        fallback: list[dict[str, Any]],
        circuit_breaker: list[dict[str, Any]],
        costs: dict[str, Any],
    ) -> dict[str, Any]:
        """Aggregate all processing results with statistics."""
        all_results = rate_limited + fallback + circuit_breaker
        successful = sum(1 for r in all_results if r.get("success"))

        # Calculate per-pattern statistics
        rate_limited_success = sum(1 for r in rate_limited if r.get("success"))
        fallback_success = sum(1 for r in fallback if r.get("success"))
        fallback_used = sum(1 for r in fallback if r.get("fallback_used"))
        circuit_success = sum(1 for r in circuit_breaker if r.get("success"))
        circuit_opened = sum(1 for r in circuit_breaker if r.get("error") == "Circuit breaker open")

        summary = {
            "total_processed": len(all_results),
            "successful": successful,
            "failed": len(all_results) - successful,
            "success_rate": round(successful / len(all_results), 3) if all_results else 0,
            "pattern_statistics": {
                "rate_limiting": {
                    "total": len(rate_limited),
                    "successful": rate_limited_success,
                },
                "fallback_chain": {
                    "total": len(fallback),
                    "successful": fallback_success,
                    "fallback_used": fallback_used,
                },
                "circuit_breaker": {
                    "total": len(circuit_breaker),
                    "successful": circuit_success,
                    "circuit_opened": circuit_opened,
                },
            },
            "cost_summary": costs,
            "patterns_demonstrated": [
                "token_bucket_rate_limiting",
                "exponential_backoff_with_jitter",
                "cost_tracking_with_alerts",
                "multi_provider_fallback",
                "circuit_breaker",
            ],
        }

        logger.info(f"Pipeline complete: {successful}/{len(all_results)} successful")
        return summary

    # DAG Flow
    prompts = get_prompts()

    # Process with different patterns
    rate_limited = process_with_rate_limiting(prompts)
    fallback = process_with_fallback(prompts)
    circuit_breaker_results = process_with_circuit_breaker(prompts)

    # Track costs from rate-limited results
    costs = track_costs(rate_limited)

    # Aggregate all results
    aggregate_results(rate_limited, fallback, circuit_breaker_results, costs)


# Instantiate the DAG
llm_retry_patterns()
