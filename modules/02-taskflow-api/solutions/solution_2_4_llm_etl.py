"""
LLM-Powered Text Extraction Pipeline - Complete Solution.

This solution demonstrates TaskFlow patterns for LLM-powered workflows,
including secure credential access, complex XCom data, and context usage.

Key patterns demonstrated:
- Secure API credential access via Variables
- Complex nested data structures via XCom
- Dynamic task mapping with .partial().expand()
- Airflow context for run metadata
- Graceful error handling for LLM responses
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import Variable
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)


# =============================================================================
# Helper Functions
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
    if any(w in text.lower() for w in ["terrible", "awful", "hate", "damaged"]):
        sentiment = "negative"

    topics = []
    if "product" in text.lower() or "widget" in text.lower():
        topics.append("product")
    if "service" in text.lower() or "support" in text.lower():
        topics.append("service")
    if "price" in text.lower() or "cost" in text.lower():
        topics.append("pricing")
    if "shipping" in text.lower() or "delivery" in text.lower():
        topics.append("shipping")
    if not topics:
        topics.append("general")

    # Return JSON string as LLM would
    return json.dumps(
        {
            "sentiment": {"label": sentiment, "confidence": 0.85},
            "topics": topics,
            "entities": [{"type": "TOPIC", "text": t, "confidence": 0.9} for t in topics],
            "action_items": [
                {"priority": "high" if sentiment == "negative" else "medium", "description": "Review feedback"}
            ]
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
        # Securely retrieve API key - never log the actual value
        api_key = Variable.get("OPENAI_API_KEY", default_var=None)

        has_key = api_key is not None and len(api_key) > 0
        using_mock = not has_key

        # Log only metadata, never the actual key
        logger.info(f"API credentials check: has_key={has_key}, using_mock={using_mock}")

        return {
            "has_api_key": has_key,
            "using_mock": using_mock,
            "provider": "openai" if has_key else "mock",
        }

    @task
    def load_feedback_documents() -> list[dict[str, Any]]:
        """
        Load sample feedback documents for processing.

        In production, this would fetch from a database or queue.
        """
        documents = [
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

        logger.info(f"Loaded {len(documents)} feedback documents")
        return documents

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
        doc_id = document.get("id", "unknown")
        text = document.get("text", "")

        if not text:
            return {
                "document_id": doc_id,
                "extraction": {},
                "success": False,
                "error": "Empty text",
            }

        try:
            # Use mock or real API based on credentials
            if credentials.get("using_mock", True):
                response = mock_llm_extraction(text)
            else:
                # Real API call would go here
                # from openai import OpenAI
                # client = OpenAI()
                # response = client.chat.completions.create(...)
                response = mock_llm_extraction(text)  # Fallback

            # Parse the JSON response
            try:
                extraction = json.loads(response)
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse LLM response for {doc_id}: {e}")
                extraction = {"raw_response": response, "parse_error": True}

            logger.info(
                f"Extracted from {doc_id}: "
                f"sentiment={extraction.get('sentiment', {}).get('label', 'unknown')}, "
                f"topics={extraction.get('topics', [])}"
            )

            return {
                "document_id": doc_id,
                "extraction": extraction,
                "source": document.get("source"),
                "success": True,
                "error": None,
            }

        except Exception as e:
            logger.exception(f"Extraction failed for {doc_id}")
            return {
                "document_id": doc_id,
                "extraction": {},
                "success": False,
                "error": str(e),
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
        successful = [e for e in extractions if e.get("success", False)]
        failed = [e for e in extractions if not e.get("success", False)]

        # Aggregate sentiment distribution
        sentiment_dist: dict[str, int] = {}
        all_topics: set[str] = set()
        action_items_count = 0

        for ext in successful:
            extraction = ext.get("extraction", {})

            # Sentiment
            sentiment = extraction.get("sentiment", {}).get("label", "unknown")
            sentiment_dist[sentiment] = sentiment_dist.get(sentiment, 0) + 1

            # Topics
            topics = extraction.get("topics", [])
            all_topics.update(topics)

            # Action items
            action_items = extraction.get("action_items", [])
            action_items_count += len(action_items)

        summary = {
            "total_documents": len(extractions),
            "successful": len(successful),
            "failed": len(failed),
            "sentiment_distribution": sentiment_dist,
            "all_topics": sorted(list(all_topics)),
            "action_items_count": action_items_count,
            "failed_ids": [e["document_id"] for e in failed],
        }

        logger.info(
            f"Aggregation complete: {len(successful)}/{len(extractions)} successful, "
            f"sentiments={sentiment_dist}, topics={len(all_topics)}"
        )

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
        # Access Airflow context
        run_id = context.get("run_id", "unknown")
        logical_date = context.get("logical_date")
        ti = context.get("task_instance")

        # Get DAG run configuration (if any)
        dag_run = context.get("dag_run")
        run_conf = dag_run.conf if dag_run and dag_run.conf else {}

        # Log processing metadata (no sensitive data)
        logger.info(
            f"Storing results for run_id={run_id}, "
            f"logical_date={logical_date}, "
            f"task_id={ti.task_id if ti else 'unknown'}"
        )

        # In production, this would write to a database
        storage_result = {
            "stored": True,
            "summary": summary,
            "metadata": {
                "run_id": run_id,
                "logical_date": str(logical_date) if logical_date else None,
                "processed_at": datetime.now().isoformat(),
                "run_config": run_conf,
            },
        }

        logger.info(
            f"Results stored: {summary['successful']} extractions, {summary['action_items_count']} action items"
        )

        return storage_result

    # =========================================================================
    # DAG Flow
    # =========================================================================

    # Get credentials (for API key check)
    credentials = get_api_credentials()

    # Load documents
    documents = load_feedback_documents()

    # Extract entities from each document using dynamic task mapping
    # .partial() fixes the credentials argument for all mapped tasks
    # .expand() creates one task per document
    extractions = extract_entities.partial(credentials=credentials).expand(document=documents)

    # Aggregate results from all extractions
    summary = aggregate_extractions(extractions)

    # Store with context metadata
    store_results(summary)


# Instantiate the DAG
llm_text_extraction()
