"""
Custom LLM Operator for Document Processing
============================================

Production-ready operator for LLM-based document processing with:
- Retry strategies with exponential backoff
- Token usage tracking and cost estimation
- Quality validation of outputs
- Configurable model selection

Modules Applied: 02, 09, 14, 15
"""

from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import Any, Callable

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator

logger = logging.getLogger(__name__)


class LLMProcessingOperator(BaseOperator):
    """
    Operator for processing documents with LLM APIs.

    This operator handles:
    - API calls with configurable retry logic
    - Token usage tracking for cost management
    - Quality validation of LLM outputs
    - Structured output parsing

    Args:
        task_id: Unique task identifier.
        document: Document data to process.
        operation: Type of LLM operation ('extract', 'summarize', 'classify').
        model: LLM model to use (default: 'gpt-3.5-turbo').
        max_tokens: Maximum tokens for response.
        temperature: Model temperature (0.0-1.0).
        quality_threshold: Minimum acceptable quality score.
        api_timeout: API call timeout in seconds.
        validate_output: Whether to validate LLM output quality.

    Example:
        ```python
        extract_entities = LLMProcessingOperator(
            task_id="extract_entities",
            document='{{ ti.xcom_pull(task_ids="load_doc") }}',
            operation="extract",
            model="gpt-4",
            quality_threshold=0.8,
            pool="llm_api",
        )
        ```
    """

    template_fields = ("document", "model", "operation")
    ui_color = "#e8f4f8"
    ui_fgcolor = "#000000"

    # Cost per 1K tokens (approximate)
    COST_PER_1K_TOKENS = {
        "gpt-4": {"input": 0.03, "output": 0.06},
        "gpt-4-turbo": {"input": 0.01, "output": 0.03},
        "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
    }

    OPERATIONS = {
        "extract": {
            "system_prompt": "Extract named entities from the document.",
            "output_format": "json",
            "expected_keys": ["people", "organizations", "dates", "locations"],
        },
        "summarize": {
            "system_prompt": "Provide a concise summary of the document.",
            "output_format": "text",
            "max_length": 500,
        },
        "classify": {
            "system_prompt": "Classify the document into categories.",
            "output_format": "json",
            "expected_keys": ["primary_category", "confidence", "secondary_categories"],
        },
    }

    def __init__(
        self,
        *,
        document: dict | str,
        operation: str = "extract",
        model: str = "gpt-3.5-turbo",
        max_tokens: int = 1000,
        temperature: float = 0.3,
        quality_threshold: float = 0.7,
        api_timeout: int = 60,
        validate_output: bool = True,
        on_success_callback: Callable | None = None,
        on_failure_callback: Callable | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize LLM Processing Operator."""
        super().__init__(**kwargs)
        self.document = document
        self.operation = operation
        self.model = model
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.quality_threshold = quality_threshold
        self.api_timeout = api_timeout
        self.validate_output = validate_output
        self._on_success_callback = on_success_callback
        self._on_failure_callback = on_failure_callback

        if operation not in self.OPERATIONS:
            raise ValueError(f"Invalid operation: {operation}. Must be one of {list(self.OPERATIONS.keys())}")

    def execute(self, context: dict) -> dict:
        """
        Execute the LLM processing operation.

        Args:
            context: Airflow task context.

        Returns:
            Dictionary containing processed results and metadata.

        Raises:
            AirflowException: If processing fails or quality is below threshold.
        """
        start_time = time.time()

        try:
            # Get document content
            doc_content = self._get_document_content()

            # Execute LLM call
            result = self._call_llm(doc_content)

            # Track token usage
            token_usage = self._estimate_tokens(doc_content, result)

            # Validate output quality
            if self.validate_output:
                quality_score = self._validate_quality(result)
                if quality_score < self.quality_threshold:
                    raise AirflowException(
                        f"Output quality {quality_score:.2f} below threshold {self.quality_threshold}"
                    )
            else:
                quality_score = 1.0

            # Calculate cost
            cost = self._calculate_cost(token_usage)

            # Prepare output
            output = {
                "operation": self.operation,
                "model": self.model,
                "result": result,
                "token_usage": token_usage,
                "cost": cost,
                "quality_score": quality_score,
                "processing_time_seconds": round(time.time() - start_time, 2),
            }

            logger.info(
                f"LLM {self.operation} completed: "
                f"tokens={token_usage['total']}, "
                f"cost=${cost:.4f}, "
                f"quality={quality_score:.2f}"
            )

            if self._on_success_callback:
                self._on_success_callback(context, output)

            return output

        except Exception as e:
            logger.error(f"LLM processing failed: {e}")
            if self._on_failure_callback:
                self._on_failure_callback(context, e)
            raise AirflowException(f"LLM {self.operation} failed: {e}") from e

    def _get_document_content(self) -> str:
        """Extract content from document."""
        if isinstance(self.document, dict):
            return self.document.get("content_preview", str(self.document))
        return str(self.document)

    def _call_llm(self, content: str) -> dict | str:
        """
        Call the LLM API.

        In production, this would use the actual LLM API.
        This implementation provides simulated responses for demonstration.
        """
        # Simulated LLM responses (in production: use operation_config from self.OPERATIONS)
        if self.operation == "extract":
            return {
                "people": ["John Smith", "Jane Doe"],
                "organizations": ["Acme Corp", "Tech Inc"],
                "dates": ["2024-01-15", "2024-02-20"],
                "locations": ["New York", "San Francisco"],
            }
        elif self.operation == "summarize":
            return f"Summary: This document discusses key business topics. Content length: {len(content)} characters."
        elif self.operation == "classify":
            return {
                "primary_category": "business",
                "confidence": 0.92,
                "secondary_categories": ["technology", "finance"],
            }
        else:
            return {"raw_output": "Processed content"}

    def _estimate_tokens(self, input_text: str, output: dict | str) -> dict:
        """Estimate token usage for cost tracking."""
        # Approximate: 1 token ≈ 4 characters
        input_tokens = len(input_text) // 4
        output_text = str(output)
        output_tokens = len(output_text) // 4

        return {
            "input": input_tokens,
            "output": output_tokens,
            "total": input_tokens + output_tokens,
        }

    def _calculate_cost(self, token_usage: dict) -> float:
        """Calculate cost based on token usage."""
        costs = self.COST_PER_1K_TOKENS.get(self.model, self.COST_PER_1K_TOKENS["gpt-3.5-turbo"])

        input_cost = (token_usage["input"] / 1000) * costs["input"]
        output_cost = (token_usage["output"] / 1000) * costs["output"]

        return round(input_cost + output_cost, 6)

    def _validate_quality(self, result: dict | str) -> float:
        """
        Validate the quality of LLM output.

        Returns a score between 0 and 1.
        """
        operation_config = self.OPERATIONS[self.operation]

        if operation_config["output_format"] == "json" and isinstance(result, dict):
            expected_keys = operation_config.get("expected_keys", [])
            if expected_keys:
                present_keys = sum(1 for k in expected_keys if k in result)
                return present_keys / len(expected_keys)
            return 1.0

        elif operation_config["output_format"] == "text" and isinstance(result, str):
            # Check minimum length and content quality
            min_length = 50
            if len(result) < min_length:
                return len(result) / min_length
            return 1.0

        return 0.5  # Unknown format


class LLMChainOperator(BaseOperator):
    """
    Operator for executing a chain of LLM operations.

    Chains multiple LLM operations in sequence, passing results
    between steps. Useful for complex document processing pipelines.

    Args:
        task_id: Unique task identifier.
        document: Document data to process.
        chain: List of operations to execute in sequence.
        model: LLM model to use.
        fail_fast: Stop chain on first failure.

    Example:
        ```python
        process_doc = LLMChainOperator(
            task_id="process_document",
            document='{{ ti.xcom_pull(task_ids="load") }}',
            chain=["extract", "summarize", "classify"],
            model="gpt-4",
        )
        ```
    """

    template_fields = ("document", "model")
    ui_color = "#d4e8f4"

    def __init__(
        self,
        *,
        document: dict | str,
        chain: list[str],
        model: str = "gpt-3.5-turbo",
        fail_fast: bool = True,
        **kwargs: Any,
    ) -> None:
        """Initialize LLM Chain Operator."""
        super().__init__(**kwargs)
        self.document = document
        self.chain = chain
        self.model = model
        self.fail_fast = fail_fast

    def execute(self, context: dict) -> dict:
        """Execute the chain of LLM operations."""
        results = {}
        total_cost = 0.0
        total_tokens = 0

        for operation in self.chain:
            try:
                op = LLMProcessingOperator(
                    task_id=f"{self.task_id}_{operation}",
                    document=self.document,
                    operation=operation,
                    model=self.model,
                )

                result = op.execute(context)
                results[operation] = result
                total_cost += result.get("cost", 0)
                total_tokens += result.get("token_usage", {}).get("total", 0)

                logger.info(f"Chain step '{operation}' completed")

            except Exception as e:
                logger.error(f"Chain step '{operation}' failed: {e}")
                results[operation] = {"error": str(e)}

                if self.fail_fast:
                    raise AirflowException(f"Chain failed at '{operation}': {e}") from e

        return {
            "chain_results": results,
            "total_cost": round(total_cost, 6),
            "total_tokens": total_tokens,
            "completed_steps": len([r for r in results.values() if "error" not in r]),
        }


# Retry configuration factory
def get_llm_retry_config(
    max_retries: int = 5,
    initial_delay: int = 30,
    max_delay: int = 600,
) -> dict:
    """
    Get recommended retry configuration for LLM tasks.

    Args:
        max_retries: Maximum number of retry attempts.
        initial_delay: Initial delay between retries in seconds.
        max_delay: Maximum delay between retries in seconds.

    Returns:
        Dictionary with retry configuration for Airflow tasks.
    """
    return {
        "retries": max_retries,
        "retry_delay": timedelta(seconds=initial_delay),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(seconds=max_delay),
    }
