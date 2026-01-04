"""
LLM-Powered Text Extraction Pipeline Starter Code.

Build an ETL pipeline that uses LLM APIs to extract structured information
from unstructured text, demonstrating XCom and context patterns.

Requirements:
1. Securely access API credentials from Airflow Variables
2. Pass complex data structures between tasks via XCom
3. Extract structured entities using LLM (mock provided)
4. Handle parsing errors gracefully

Instructions:
- Complete all TODO sections
- Run with: airflow dags test llm_text_extraction <date>
- Test with sample feedback documents
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)


# =============================================================================
# Helper Functions (provided)
# =============================================================================


def mock_llm_extraction(text: str) -> str:
    """
    Mock LLM extraction for testing without API costs.

    In production, replace with actual OpenAI/Anthropic API call.
    """
    # Simulate API latency
    time.sleep(0.1)

    # Simple rule-based mock for consistent testing
    sentiment = "positive" if any(w in text.lower() for w in ["great", "love", "excellent"]) else "neutral"
    if any(w in text.lower() for w in ["terrible", "awful", "hate"]):
        sentiment = "negative"

    topics = []
    if "product" in text.lower() or "widget" in text.lower():
        topics.append("product")
    if "service" in text.lower() or "support" in text.lower():
        topics.append("service")
    if "price" in text.lower() or "cost" in text.lower():
        topics.append("pricing")
    if not topics:
        topics.append("general")

    # Return JSON string as LLM would
    return json.dumps(
        {
            "sentiment": {"label": sentiment, "confidence": 0.85},
            "topics": topics,
            "entities": [{"type": "TOPIC", "text": t, "confidence": 0.9} for t in topics],
            "action_items": [{"priority": "medium", "description": "Review feedback"}]
            if sentiment != "positive"
            else [],
        }
    )


# =============================================================================
# DAG Definition
# =============================================================================


@dag(
    dag_id="llm_text_extraction",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["ai-ml", "taskflow", "module-02"],
    doc_md=__doc__,
)
def llm_text_extraction():
    """
    LLM-Powered Text Extraction Pipeline.

    Extracts structured information from customer feedback
    using TaskFlow patterns and LLM APIs.
    """

    @task
    def get_api_credentials() -> dict[str, str]:
        """
        Securely retrieve API credentials.

        Returns:
            Dict with credential metadata (never the actual keys in logs).
        """
        # TODO: Implement secure credential access
        # 1. Use Variable.get() to retrieve API key
        # 2. Check if key exists (return mock indicator if not)
        # 3. Return metadata about credentials (NOT the actual key)
        # 4. Never log the actual API key

        # Hint: Use Variable.get("OPENAI_API_KEY", default_var=None)

        return {
            "has_api_key": False,
            "using_mock": True,
            "provider": "mock",
        }

    @task
    def load_feedback_documents() -> list[dict[str, Any]]:
        """
        Load sample feedback documents for processing.

        In production, this would fetch from a database or queue.
        """
        return [
            {
                "id": "feedback-001",
                "text": "I love the new Widget Pro! Great product and excellent customer service.",
                "source": "email",
                "timestamp": "2024-01-15T10:30:00Z",
            },
            {
                "id": "feedback-002",
                "text": "The support team was helpful, but the product price is too high.",
                "source": "survey",
                "timestamp": "2024-01-15T11:45:00Z",
            },
            {
                "id": "feedback-003",
                "text": "Terrible experience with shipping. Product arrived damaged.",
                "source": "review",
                "timestamp": "2024-01-15T14:20:00Z",
            },
        ]

    @task
    def extract_entities(
        document: dict[str, Any],
        credentials: dict[str, str],
    ) -> dict[str, Any]:
        """
        Extract structured entities from a document using LLM.

        Args:
            document: Feedback document with text.
            credentials: API credential metadata.

        Returns:
            Extraction result with entities and metadata.
        """
        # TODO: Implement LLM extraction
        # 1. Get the text from the document
        # 2. Call mock_llm_extraction (or real API if credentials available)
        # 3. Parse the JSON response
        # 4. Handle parsing errors gracefully
        # 5. Return structured result with document ID

        text = document.get("text", "")

        # TODO: Your implementation here
        # - Call mock_llm_extraction(text)
        # - Parse the response with json.loads()
        # - Handle json.JSONDecodeError

        _ = text, credentials  # Use in your implementation

        return {
            "document_id": document.get("id"),
            "extraction": {},
            "success": False,
            "error": "Not implemented",
        }

    @task
    def aggregate_extractions(
        extractions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Aggregate extraction results from all documents.

        Args:
            extractions: List of extraction results.

        Returns:
            Aggregated summary with statistics.
        """
        # TODO: Implement aggregation
        # 1. Count successful vs failed extractions
        # 2. Aggregate sentiment distribution
        # 3. Collect all unique topics
        # 4. Count action items requiring follow-up

        summary = {
            "total_documents": len(extractions),
            "successful": 0,
            "failed": 0,
            "sentiment_distribution": {},
            "all_topics": [],
            "action_items_count": 0,
        }

        # TODO: Your implementation here
        # Loop through extractions and aggregate statistics

        return summary

    @task
    def store_results(
        summary: dict[str, Any],
        **context: Any,
    ) -> dict[str, Any]:
        """
        Store extraction results with context metadata.

        Args:
            summary: Aggregated extraction summary.
            **context: Airflow context with execution metadata.

        Returns:
            Storage confirmation with run metadata.
        """
        # TODO: Implement context-aware storage
        # 1. Access task_instance from context
        # 2. Get run_id and logical_date
        # 3. Log processing metadata (no sensitive data!)
        # 4. Return storage confirmation

        # TODO: Your implementation here
        # Access context["task_instance"], context["run_id"], etc.

        return {
            "stored": True,
            "summary": summary,
            "metadata": {
                "run_id": "unknown",
                "processed_at": datetime.now().isoformat(),
            },
        }

    # =========================================================================
    # DAG Flow
    # =========================================================================

    # Get credentials (for API key check)
    credentials = get_api_credentials()

    # Load documents
    documents = load_feedback_documents()

    # Extract entities from each document (using expand)
    # TODO: Use .expand() to process documents in parallel
    # Each document should be passed along with credentials
    extractions = extract_entities.partial(credentials=credentials).expand(document=documents)

    # Aggregate results
    summary = aggregate_extractions(extractions)

    # Store with context
    store_results(summary)


# Instantiate the DAG
llm_text_extraction()
