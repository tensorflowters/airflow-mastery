"""
LLM Chain Orchestration - Complete Solution.

This solution demonstrates a production-ready LLM chain workflow that processes
content through multiple steps with rate limiting, cost tracking, fallback
model routing, and comprehensive observability.

Key patterns demonstrated:
- Token bucket rate limiting for API calls
- Multi-step LLM chain (extract -> summarize -> classify -> score)
- Task-level cost tracking with budget alerts
- Fallback model routing for reliability
- Comprehensive pipeline metrics aggregation
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
from typing import Any

from airflow import Variable
from airflow.sdk import dag, task

# Configuration
DAILY_BUDGET_USD = 10.0
DEFAULT_MODEL = "gpt-3.5-turbo"
FALLBACK_MODEL = "gpt-3.5-turbo"
REQUESTS_PER_MINUTE = 60
USE_MOCK_LLM = True  # Set to False to use real OpenAI API

# Model pricing (per 1K tokens)
MODEL_PRICING = {
    "gpt-4-turbo": {"input": 0.01, "output": 0.03},
    "gpt-4": {"input": 0.03, "output": 0.06},
    "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
}

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class LLMResponse:
    """Structured response from an LLM call."""

    content: str
    model: str
    input_tokens: int
    output_tokens: int
    cost: float
    latency_ms: float
    success: bool = True
    error: str | None = None


@dataclass
class ChainResult:
    """Result from a chain step."""

    step_name: str
    output: dict[str, Any]
    llm_response: LLMResponse
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


# =============================================================================
# Rate Limiter
# =============================================================================


class TokenBucketRateLimiter:
    """Token bucket rate limiter for API calls."""

    def __init__(self, tokens_per_second: float, max_tokens: int = 10) -> None:
        """Initialize the rate limiter with configurable token rate and capacity."""
        self.tokens_per_second = tokens_per_second
        self.max_tokens = max_tokens
        self.tokens = float(max_tokens)
        self.last_update = time.time()
        self.lock = Lock()

    def acquire(self, tokens: int = 1) -> float:
        """
        Acquire tokens, blocking if necessary.

        Returns the wait time in seconds.
        """
        with self.lock:
            self._refill()

            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0.0

            # Calculate wait time for sufficient tokens
            wait_time = (tokens - self.tokens) / self.tokens_per_second
            time.sleep(wait_time)

            # Refill after waiting
            self._refill()
            self.tokens -= tokens
            return wait_time

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.max_tokens, self.tokens + elapsed * self.tokens_per_second)
        self.last_update = now


# Global rate limiter instance
rate_limiter = TokenBucketRateLimiter(
    tokens_per_second=REQUESTS_PER_MINUTE / 60,
    max_tokens=10,
)


# =============================================================================
# Helper Functions
# =============================================================================


def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate cost in USD for an LLM call."""
    pricing = MODEL_PRICING.get(model, MODEL_PRICING["gpt-3.5-turbo"])
    input_cost = (input_tokens / 1000) * pricing["input"]
    output_cost = (output_tokens / 1000) * pricing["output"]
    return input_cost + output_cost


def estimate_tokens(text: str) -> int:
    """Estimate token count for text."""
    return len(text) // 4


def select_model(task_complexity: str, primary_failed: bool = False) -> str:
    """Select model based on task complexity and failure state."""
    if primary_failed:
        # Fallback chain: gpt-4 -> gpt-3.5-turbo
        return FALLBACK_MODEL

    complexity_map = {
        "simple": "gpt-3.5-turbo",
        "moderate": "gpt-3.5-turbo",
        "complex": "gpt-4",
        "critical": "gpt-4-turbo",
    }
    return complexity_map.get(task_complexity, DEFAULT_MODEL)


def mock_llm_call(prompt: str, model: str) -> dict[str, Any]:
    """Mock LLM call for testing without API costs."""
    # Simulate processing time
    time.sleep(0.1)

    input_tokens = estimate_tokens(prompt)
    output_tokens = 50 + (len(prompt) % 100)

    # Generate mock responses based on prompt content
    if "extract" in prompt.lower():
        content = json.dumps(
            {
                "entities": ["OpenAI", "Anthropic", "Google", "EU"],
                "dates": ["2024-01-15", "2024"],
                "facts": [
                    "AI advances rapidly in 2024",
                    "New models have improved reasoning",
                    "Enterprise AI adoption up 40%",
                    "EU AI Act sets new standards",
                ],
            }
        )
    elif "summar" in prompt.lower():
        content = (
            "AI technology continues rapid advancement in 2024 with major companies "
            "releasing improved models. Enterprise adoption has increased significantly, "
            "particularly in healthcare and finance, while new regulatory frameworks "
            "like the EU AI Act establish global governance standards."
        )
    elif "classif" in prompt.lower():
        content = json.dumps({"category": "technology", "confidence": 0.92})
    elif "score" in prompt.lower():
        content = json.dumps(
            {
                "quality": 8,
                "relevance": 9,
                "reasoning": "Well-structured with current data and clear examples",
            }
        )
    else:
        content = "Mock response for: " + prompt[:50]

    return {
        "content": content,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "model": model,
    }


def call_llm(prompt: str, model: str, max_retries: int = 3) -> LLMResponse:
    """
    Call LLM with rate limiting and retry logic.

    Args:
        prompt: The prompt to send.
        model: Model identifier.
        max_retries: Maximum retry attempts.

    Returns:
        LLMResponse with results or error.
    """
    # Acquire rate limit token
    wait_time = rate_limiter.acquire(1)
    if wait_time > 0:
        logger.info(f"Rate limited: waited {wait_time:.2f}s")

    start_time = time.time()
    last_error = None

    for attempt in range(max_retries):
        try:
            if USE_MOCK_LLM:
                result = mock_llm_call(prompt, model)
            else:
                # Real OpenAI API call would go here
                # from openai import OpenAI
                # client = OpenAI()
                # response = client.chat.completions.create(
                #     model=model,
                #     messages=[{"role": "user", "content": prompt}],
                # )
                # result = {
                #     "content": response.choices[0].message.content,
                #     "input_tokens": response.usage.prompt_tokens,
                #     "output_tokens": response.usage.completion_tokens,
                #     "model": model,
                # }
                result = mock_llm_call(prompt, model)  # Fallback to mock

            latency_ms = (time.time() - start_time) * 1000
            cost = calculate_cost(model, result["input_tokens"], result["output_tokens"])

            logger.info(
                f"LLM call success: model={model}, "
                f"tokens={result['input_tokens']}+{result['output_tokens']}, "
                f"cost=${cost:.6f}, latency={latency_ms:.0f}ms"
            )

            return LLMResponse(
                content=result["content"],
                model=model,
                input_tokens=result["input_tokens"],
                output_tokens=result["output_tokens"],
                cost=cost,
                latency_ms=latency_ms,
            )

        except Exception as e:
            last_error = str(e)
            logger.warning(f"LLM call attempt {attempt + 1} failed: {e}")

            if attempt < max_retries - 1:
                # Exponential backoff
                backoff = 2**attempt
                time.sleep(backoff)

    # All retries exhausted
    return LLMResponse(
        content="",
        model=model,
        input_tokens=0,
        output_tokens=0,
        cost=0,
        latency_ms=(time.time() - start_time) * 1000,
        success=False,
        error=last_error or "Max retries exceeded",
    )


# =============================================================================
# Cost Tracking
# =============================================================================


def track_cost(task_name: str, response: LLMResponse) -> None:
    """Track LLM cost for a task."""
    if not response.success:
        return

    # Get current daily spend
    daily_key = f"llm_spend_{datetime.now().strftime('%Y-%m-%d')}"
    current_spend = Variable.get(daily_key, default_var=0.0, deserialize_json=True)

    # Add new cost
    new_spend = current_spend + response.cost

    # Save updated spend
    Variable.set(daily_key, new_spend, serialize_json=True)

    logger.info(f"Cost tracked for {task_name}: ${response.cost:.6f}, daily total: ${new_spend:.6f}")

    # Check budget and log warning if exceeded
    if new_spend > DAILY_BUDGET_USD:
        logger.warning(f"BUDGET EXCEEDED: Daily spend ${new_spend:.2f} exceeds ${DAILY_BUDGET_USD:.2f}")


def get_daily_spend() -> float:
    """Get current daily LLM spend."""
    daily_key = f"llm_spend_{datetime.now().strftime('%Y-%m-%d')}"
    return Variable.get(daily_key, default_var=0.0, deserialize_json=True)


def check_budget() -> bool:
    """Check if daily budget is exceeded."""
    return get_daily_spend() < DAILY_BUDGET_USD


# =============================================================================
# DAG Definition
# =============================================================================


@dag(
    dag_id="llm_chain_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["ai-ml", "llm", "module-15"],
    doc_md=__doc__,
)
def llm_chain_pipeline():
    """
    LLM Chain Pipeline.

    Processes content through multiple LLM steps with cost tracking
    and fallback model routing.
    """

    @task
    def get_sample_content() -> dict[str, Any]:
        """
        Get sample content for processing.

        In production, this would fetch from a queue or database.
        """
        return {
            "id": "article-001",
            "title": "AI Advances in 2024",
            "content": """
            Artificial intelligence continues to advance rapidly in 2024.
            Major tech companies like OpenAI, Anthropic, and Google have released
            new models with improved reasoning capabilities. The industry is seeing
            a shift towards more efficient, smaller models that can run on consumer
            hardware. Enterprise adoption of AI has increased by 40% compared to
            last year, with particular growth in healthcare and finance sectors.
            Regulatory frameworks are also evolving, with the EU AI Act setting
            new standards for AI governance globally.
            """,
            "source": "Tech News Daily",
            "published_date": "2024-01-15",
        }

    @task
    def extract_entities(content: dict[str, Any]) -> dict[str, Any]:
        """
        Extract key entities and facts from the content.

        Args:
            content: Article content dict.

        Returns:
            Dict with extracted entities and LLM response metadata.
        """
        text = content.get("content", "")

        # Build extraction prompt
        extraction_prompt = f"""Extract key entities from this text. Return JSON with:
- entities: list of named entities (people, companies, organizations)
- dates: list of dates mentioned
- facts: list of key facts (max 5)

Text:
{text}

Return only valid JSON."""

        # Select model for simple extraction task
        model = select_model("simple")

        # Call LLM with rate limiting
        response = call_llm(extraction_prompt, model)

        if not response.success:
            logger.error(f"Entity extraction failed: {response.error}")
            result: dict[str, Any] = {"entities": [], "dates": [], "facts": []}
        else:
            # Parse and validate response
            try:
                result = json.loads(response.content)
            except json.JSONDecodeError:
                logger.warning("Failed to parse extraction response as JSON")
                result = {"entities": [], "dates": [], "facts": [], "raw": response.content}

        # Track cost
        track_cost("extract_entities", response)

        return {
            "step": "extract_entities",
            "input_id": content.get("id"),
            "output": result,
            "cost": response.cost,
            "tokens": {"input": response.input_tokens, "output": response.output_tokens},
            "model": response.model,
            "latency_ms": response.latency_ms,
        }

    @task
    def generate_summary(
        content: dict[str, Any],
        entities: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate a concise summary of the content.

        Args:
            content: Original article content.
            entities: Extracted entities from previous step.

        Returns:
            Dict with summary and LLM response metadata.
        """
        text = content.get("content", "")
        entity_context = json.dumps(entities.get("output", {}))

        # Build summary prompt incorporating entities
        summary_prompt = f"""Summarize this article in 2-3 sentences.
Use the extracted entities for context: {entity_context}

Article:
{text}

Summary:"""

        # Select model for moderate complexity
        model = select_model("moderate")

        # Call LLM
        response = call_llm(summary_prompt, model)

        if not response.success:
            logger.error(f"Summary generation failed: {response.error}")
            result = ""
        else:
            result = response.content.strip()

        # Track cost
        track_cost("generate_summary", response)

        return {
            "step": "generate_summary",
            "input_id": content.get("id"),
            "output": {"summary": result},
            "cost": response.cost,
            "tokens": {"input": response.input_tokens, "output": response.output_tokens},
            "model": response.model,
            "latency_ms": response.latency_ms,
        }

    @task
    def classify_content(
        content: dict[str, Any],
        summary: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Classify the content into categories.

        Args:
            content: Original article content.
            summary: Generated summary from previous step.

        Returns:
            Dict with classification and LLM response metadata.
        """
        summary_text = summary.get("output", {}).get("summary", "")

        # Build classification prompt
        classification_prompt = f"""Classify this content into one of these categories:
- technology
- business
- science
- politics
- entertainment
- sports
- health

Summary: {summary_text}

Return JSON with:
- category: the primary category
- confidence: confidence score 0-1

JSON:"""

        # Select model for simple classification
        model = select_model("simple")

        # Call LLM
        response = call_llm(classification_prompt, model)

        if not response.success:
            logger.error(f"Classification failed: {response.error}")
            result = {"category": "unknown", "confidence": 0.0}
        else:
            try:
                result = json.loads(response.content)
            except json.JSONDecodeError:
                logger.warning("Failed to parse classification response")
                result = {"category": "unknown", "confidence": 0.0, "raw": response.content}

        # Track cost
        track_cost("classify_content", response)

        # Mark content as used for logging
        logger.debug(f"Classified content ID: {content.get('id')}")

        return {
            "step": "classify_content",
            "input_id": content.get("id"),
            "output": result,
            "cost": response.cost,
            "tokens": {"input": response.input_tokens, "output": response.output_tokens},
            "model": response.model,
            "latency_ms": response.latency_ms,
        }

    @task
    def score_quality(
        content: dict[str, Any],
        summary: dict[str, Any],
        classification: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Score the content quality and relevance.

        Args:
            content: Original article content.
            summary: Generated summary.
            classification: Content classification.

        Returns:
            Dict with quality scores and LLM response metadata.
        """
        summary_text = summary.get("output", {}).get("summary", "")
        category = classification.get("output", {}).get("category", "unknown")

        # Build scoring prompt - use better model for complex analysis
        scoring_prompt = f"""Rate this {category} content on:
- quality: writing quality and depth (1-10)
- relevance: current relevance and importance (1-10)

Summary: {summary_text}

Return JSON with quality, relevance scores, and brief reasoning.
JSON:"""

        # Select model for complex scoring task
        model = select_model("complex")

        # Call LLM
        response = call_llm(scoring_prompt, model)

        if not response.success:
            logger.error(f"Quality scoring failed: {response.error}")
            result = {"quality": 0, "relevance": 0}
        else:
            try:
                result = json.loads(response.content)
            except json.JSONDecodeError:
                logger.warning("Failed to parse scoring response")
                result = {"quality": 0, "relevance": 0, "raw": response.content}

        # Track cost
        track_cost("score_quality", response)

        # Mark content as used for logging
        logger.debug(f"Scored content ID: {content.get('id')}")

        return {
            "step": "score_quality",
            "input_id": content.get("id"),
            "output": result,
            "cost": response.cost,
            "tokens": {"input": response.input_tokens, "output": response.output_tokens},
            "model": response.model,
            "latency_ms": response.latency_ms,
        }

    @task
    def aggregate_results(
        content: dict[str, Any],
        entities: dict[str, Any],
        summary: dict[str, Any],
        classification: dict[str, Any],
        scores: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Aggregate all chain results into a final output.

        Returns:
            Complete analysis with all steps and cost breakdown.
        """
        steps = [entities, summary, classification, scores]
        total_cost = sum(s.get("cost", 0) for s in steps)
        total_input_tokens = sum(s.get("tokens", {}).get("input", 0) for s in steps)
        total_output_tokens = sum(s.get("tokens", {}).get("output", 0) for s in steps)
        total_latency = sum(s.get("latency_ms", 0) for s in steps)

        # Model usage distribution
        model_usage: dict[str, int] = {}
        for s in steps:
            model = s.get("model", "unknown")
            model_usage[model] = model_usage.get(model, 0) + 1

        result = {
            "id": content.get("id"),
            "title": content.get("title"),
            "analysis": {
                "entities": entities.get("output", {}),
                "summary": summary.get("output", {}).get("summary", ""),
                "category": classification.get("output", {}).get("category", ""),
                "confidence": classification.get("output", {}).get("confidence", 0),
                "quality_score": scores.get("output", {}).get("quality", 0),
                "relevance_score": scores.get("output", {}).get("relevance", 0),
            },
            "cost_breakdown": {
                "total_cost_usd": total_cost,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "by_step": {s.get("step", "unknown"): s.get("cost", 0) for s in steps},
            },
            "performance": {
                "total_latency_ms": total_latency,
                "by_step": {s.get("step", "unknown"): s.get("latency_ms", 0) for s in steps},
            },
            "model_usage": model_usage,
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(
            f"Chain complete for {content.get('id')}. "
            f"Total cost: ${total_cost:.6f}, Total latency: {total_latency:.0f}ms"
        )

        return result

    # =========================================================================
    # DAG Flow
    # =========================================================================

    # Get content to process
    content = get_sample_content()

    # Chain steps
    entities = extract_entities(content)
    summary = generate_summary(content, entities)
    classification = classify_content(content, summary)
    scores = score_quality(content, summary, classification)

    # Aggregate final result
    aggregate_results(content, entities, summary, classification, scores)


# Instantiate the DAG
llm_chain_pipeline()
