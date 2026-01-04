"""
LLM Chain Orchestration Starter Code.

Build a multi-step LLM workflow with rate limiting, cost tracking,
fallback model routing, and comprehensive observability.

Requirements:
1. Implement token bucket rate limiting for API calls
2. Create a chain of LLM tasks (extract -> summarize -> classify -> score)
3. Track costs at the task level with budget alerts
4. Implement fallback model routing for reliability
5. Aggregate pipeline metrics and costs

Instructions:
- Complete all TODO sections
- Run with: airflow dags test llm_chain_pipeline <date>
- Test with sample articles to validate the chain
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
        """Initialize the rate limiter."""
        # TODO: Initialize rate limiter state
        # - tokens_per_second: rate of token refill
        # - max_tokens: bucket capacity
        # - tokens: current token count
        # - last_update: timestamp of last refill
        # - lock: threading lock for thread safety
        self.tokens_per_second = tokens_per_second
        self.max_tokens = max_tokens
        self.tokens = float(max_tokens)
        self.last_update = time.time()

    def acquire(self, tokens: int = 1) -> float:
        """
        Acquire tokens, blocking if necessary.

        Returns the wait time in seconds.
        """
        # TODO: Implement token acquisition
        # 1. Refill tokens based on elapsed time
        # 2. If enough tokens available, deduct and return 0
        # 3. Otherwise, calculate wait time, sleep, then deduct
        return 0.0

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        # TODO: Implement token refill logic
        pass


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
    # TODO: Implement model selection logic
    # - Simple tasks: gpt-3.5-turbo
    # - Complex tasks: gpt-4
    # - On failure: fallback to cheaper model
    _ = task_complexity, primary_failed  # Use in your implementation
    return DEFAULT_MODEL


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
                "entities": ["Company A", "Person B", "Location C"],
                "dates": ["2024-01-15"],
                "facts": ["Key fact 1", "Key fact 2"],
            }
        )
    elif "summar" in prompt.lower():
        content = "This is a mock summary of the provided content."
    elif "classif" in prompt.lower():
        content = json.dumps({"category": "technology", "confidence": 0.85})
    elif "score" in prompt.lower():
        content = json.dumps({"quality": 7, "relevance": 8})
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
    # TODO: Implement LLM call with rate limiting and retries
    # 1. Acquire rate limit token
    # 2. Make API call (or mock)
    # 3. Handle errors with exponential backoff
    # 4. Calculate cost and return LLMResponse

    _ = max_retries  # Use in your retry implementation
    start_time = time.time()

    # Placeholder implementation
    if USE_MOCK_LLM:
        result = mock_llm_call(prompt, model)
        latency_ms = (time.time() - start_time) * 1000
        cost = calculate_cost(model, result["input_tokens"], result["output_tokens"])

        return LLMResponse(
            content=result["content"],
            model=model,
            input_tokens=result["input_tokens"],
            output_tokens=result["output_tokens"],
            cost=cost,
            latency_ms=latency_ms,
        )

    # TODO: Implement real API call with retries
    return LLMResponse(
        content="",
        model=model,
        input_tokens=0,
        output_tokens=0,
        cost=0,
        latency_ms=0,
        success=False,
        error="Real API not implemented",
    )


# =============================================================================
# Cost Tracking
# =============================================================================


def track_cost(task_name: str, response: LLMResponse) -> None:
    """Track LLM cost for a task."""
    # TODO: Implement cost tracking
    # 1. Get current daily spend from Variable
    # 2. Add new cost
    # 3. Save updated spend
    # 4. Check against budget and log warning if exceeded
    _ = task_name, response  # Use in your implementation


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
        # TODO: Implement entity extraction
        # 1. Build extraction prompt
        # 2. Select appropriate model (simple task)
        # 3. Call LLM with rate limiting
        # 4. Parse and validate response
        # 5. Track cost
        # 6. Return structured result

        text = content.get("content", "")

        # Build the prompt - use it with call_llm() in your implementation
        extraction_prompt = f"""Extract key entities from this text. Return JSON with:
- entities: list of named entities (people, companies, organizations)
- dates: list of dates mentioned
- facts: list of key facts (max 5)

Text:
{text}

Return only valid JSON."""

        # TODO: Call call_llm(extraction_prompt, model) and parse response
        _ = extraction_prompt  # Remove this line when implementing

        result: dict[str, Any] = {
            "entities": [],
            "dates": [],
            "facts": [],
        }

        return {
            "step": "extract_entities",
            "input_id": content.get("id"),
            "output": result,
            "cost": 0.0,
            "tokens": {"input": 0, "output": 0},
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
        # TODO: Implement summary generation
        # 1. Build summary prompt incorporating entities
        # 2. Select model (moderate complexity)
        # 3. Call LLM
        # 4. Track cost
        # 5. Return result

        text = content.get("content", "")
        entity_context = json.dumps(entities.get("output", {}))

        # Build the prompt - use it with call_llm() in your implementation
        summary_prompt = f"""Summarize this article in 2-3 sentences.
Use the extracted entities for context: {entity_context}

Article:
{text}

Summary:"""

        # TODO: Call call_llm(summary_prompt, model) and extract summary
        _ = summary_prompt  # Remove this line when implementing

        result = ""

        return {
            "step": "generate_summary",
            "input_id": content.get("id"),
            "output": {"summary": result},
            "cost": 0.0,
            "tokens": {"input": 0, "output": 0},
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
        # TODO: Implement content classification
        # 1. Build classification prompt
        # 2. Select model (simple task)
        # 3. Call LLM
        # 4. Parse category and confidence
        # 5. Track cost

        summary_text = summary.get("output", {}).get("summary", "")

        # Build the prompt - use it with call_llm() in your implementation
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

        # TODO: Call call_llm(classification_prompt, model) and parse response
        _ = classification_prompt, content  # Remove this line when implementing

        result = {"category": "technology", "confidence": 0.0}

        return {
            "step": "classify_content",
            "input_id": content.get("id"),
            "output": result,
            "cost": 0.0,
            "tokens": {"input": 0, "output": 0},
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
        # TODO: Implement quality scoring
        # 1. Build scoring prompt
        # 2. Select model (complex task - use better model)
        # 3. Call LLM
        # 4. Parse scores
        # 5. Track cost

        summary_text = summary.get("output", {}).get("summary", "")
        category = classification.get("output", {}).get("category", "unknown")

        # Build the prompt - use it with call_llm() in your implementation
        scoring_prompt = f"""Rate this {category} content on:
- quality: writing quality and depth (1-10)
- relevance: current relevance and importance (1-10)

Summary: {summary_text}

Return JSON with quality and relevance scores.
JSON:"""

        # TODO: Call call_llm(scoring_prompt, model) and parse response
        _ = scoring_prompt, content  # Remove this line when implementing

        result = {"quality": 0, "relevance": 0}

        return {
            "step": "score_quality",
            "input_id": content.get("id"),
            "output": result,
            "cost": 0.0,
            "tokens": {"input": 0, "output": 0},
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
            "timestamp": datetime.now().isoformat(),
        }

        logger.info(f"Chain complete. Total cost: ${total_cost:.6f}")
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
