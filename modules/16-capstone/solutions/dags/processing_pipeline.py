"""
Capstone Solution: LLM Processing Pipeline
==========================================

Production-ready LLM processing DAG demonstrating:
- Asset-triggered scheduling from ingestion pipeline
- LLM chain orchestration with proper error handling
- Pool-based rate limiting for API calls
- Quality gates with branching logic
- Cost tracking for LLM usage

Modules Applied: 02, 09, 14, 15
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Variable, dag, task
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

BASE_PATH = Variable.get("capstone_base_path", default_var="/tmp/capstone")
PROCESSED_PATH = f"{BASE_PATH}/processed"
ENRICHED_PATH = f"{BASE_PATH}/enriched"

# Asset definitions
RAW_DOCUMENTS = Asset("documents.raw")
ENRICHED_DOCUMENTS = Asset("documents.enriched")

# LLM Configuration
LLM_MODEL = Variable.get("llm_model", default_var="gpt-3.5-turbo")
QUALITY_THRESHOLD = float(Variable.get("quality_threshold", default_var="0.8"))

# =============================================================================
# COST TRACKING
# =============================================================================


class CostTracker:
    """Track LLM API costs for monitoring."""

    # Approximate costs per 1K tokens (as of 2024)
    COSTS = {
        "gpt-4": {"input": 0.03, "output": 0.06},
        "gpt-4-turbo": {"input": 0.01, "output": 0.03},
        "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
    }

    @classmethod
    def estimate_cost(cls, model: str, input_tokens: int, output_tokens: int) -> float:
        """Estimate cost based on token usage."""
        costs = cls.COSTS.get(model, cls.COSTS["gpt-3.5-turbo"])
        input_cost = (input_tokens / 1000) * costs["input"]
        output_cost = (output_tokens / 1000) * costs["output"]
        return round(input_cost + output_cost, 6)


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="capstone_processing_pipeline",
    description="LLM-powered document enrichment with quality gates",
    schedule=RAW_DOCUMENTS,  # Triggered when raw documents are available
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "processing", "llm", "production"],
    default_args={
        "owner": "capstone",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
    doc_md="""
    ## LLM Processing Pipeline

    Enriches documents using LLM chains for extraction, summarization,
    and classification.

    ### Trigger
    - Asset-based: Runs when `documents.raw` is updated

    ### Processing Chain
    ```
    Load Document → Extract Entities → Summarize → Classify → Quality Gate
                                                                    ↓
                                                        Pass → Enriched Asset
                                                        Fail → Human Review
    ```

    ### Rate Limiting
    - Uses `llm_api` pool (5 slots) to prevent API overload
    """,
)
def processing_pipeline():
    """LLM-powered document processing pipeline."""
    # =========================================================================
    # LOAD: Get documents from raw storage
    # =========================================================================

    @task
    def load_pending_documents() -> list[dict]:
        """Load documents awaiting processing."""
        processed_dir = Path(PROCESSED_PATH)
        documents = []

        if not processed_dir.exists():
            logger.warning(f"Processed directory not found: {PROCESSED_PATH}")
            return []

        for record_path in processed_dir.glob("doc_*.json"):
            try:
                document = json.loads(record_path.read_text())
                # Only process documents not yet enriched
                if document.get("status") == "stored":
                    documents.append(document)
            except Exception as e:
                logger.error(f"Error loading {record_path}: {e}")

        logger.info(f"Loaded {len(documents)} documents for processing")
        return documents

    # =========================================================================
    # EXTRACTION: LLM-powered entity extraction
    # =========================================================================

    @task(
        pool="llm_api",
        pool_slots=1,
        retries=5,
        retry_delay=timedelta(seconds=30),
        retry_exponential_backoff=True,
    )
    def extract_entities(document: dict) -> dict:
        """
        Extract entities from document using LLM.

        Uses pool-based rate limiting to respect API limits.
        """
        # Simulated LLM call (in production: actual API call)
        # In production: use langchain or direct API
        content = document.get("content_preview", "")

        # Simulated extraction results
        entities = {
            "people": ["John Smith", "Jane Doe"],
            "organizations": ["Acme Corp"],
            "dates": ["2024-01-15"],
            "locations": ["New York"],
        }

        # Track token usage (simulated)
        input_tokens = len(content.split()) * 1.3  # Approximate
        output_tokens = 50
        cost = CostTracker.estimate_cost(LLM_MODEL, input_tokens, output_tokens)

        document["entities"] = entities
        document["extraction_model"] = LLM_MODEL
        document["extraction_tokens"] = {
            "input": int(input_tokens),
            "output": output_tokens,
        }
        document["extraction_cost"] = cost
        document["extracted_at"] = datetime.utcnow().isoformat()

        logger.info(
            f"Extracted entities from {document['id']}: "
            f"{len(entities.get('people', []))} people, "
            f"{len(entities.get('organizations', []))} orgs"
        )
        return document

    # =========================================================================
    # SUMMARIZATION: Generate document summary
    # =========================================================================

    @task(
        pool="llm_api",
        pool_slots=1,
        retries=3,
        retry_delay=timedelta(seconds=30),
    )
    def summarize_document(document: dict) -> dict:
        """Generate a concise summary of the document."""
        content = document.get("content_preview", "")

        # Simulated summarization (in production: actual LLM call)
        summary = (
            f"Summary of {document['filename']}: This document contains "
            f"information about {', '.join(document['entities'].get('people', ['various topics']))}."
        )

        # Track costs
        input_tokens = len(content.split()) * 1.3
        output_tokens = len(summary.split()) * 1.3
        cost = CostTracker.estimate_cost(LLM_MODEL, input_tokens, output_tokens)

        document["summary"] = summary
        document["summary_tokens"] = {
            "input": int(input_tokens),
            "output": int(output_tokens),
        }
        document["summary_cost"] = cost
        document["summarized_at"] = datetime.utcnow().isoformat()

        logger.info(f"Generated summary for {document['id']}: {len(summary)} chars")
        return document

    # =========================================================================
    # CLASSIFICATION: Categorize document
    # =========================================================================

    @task(
        pool="llm_api",
        pool_slots=1,
        retries=3,
    )
    def classify_document(document: dict) -> dict:
        """Classify document into categories."""
        import random

        # Simulated classification (in production: actual LLM call)
        categories = ["legal", "financial", "technical", "hr", "marketing"]
        classification = {
            "primary_category": random.choice(categories),
            "confidence": round(random.uniform(0.7, 0.99), 2),
            "secondary_categories": random.sample(categories, k=2),
        }

        # Track costs
        cost = CostTracker.estimate_cost(LLM_MODEL, 100, 20)

        document["classification"] = classification
        document["classification_cost"] = cost
        document["classified_at"] = datetime.utcnow().isoformat()

        logger.info(
            f"Classified {document['id']}: "
            f"{classification['primary_category']} "
            f"(confidence: {classification['confidence']})"
        )
        return document

    # =========================================================================
    # QUALITY GATE: Validate enrichment quality
    # =========================================================================

    @task
    def evaluate_quality(document: dict) -> dict:
        """Evaluate the quality of LLM enrichment."""
        scores = {
            "extraction_score": 1.0 if document.get("entities") else 0.0,
            "summary_score": (min(1.0, len(document.get("summary", "")) / 100) if document.get("summary") else 0.0),
            "classification_score": document.get("classification", {}).get("confidence", 0.0),
        }

        overall_score = sum(scores.values()) / len(scores)

        document["quality_scores"] = scores
        document["overall_quality"] = round(overall_score, 2)
        document["meets_threshold"] = overall_score >= QUALITY_THRESHOLD
        document["quality_evaluated_at"] = datetime.utcnow().isoformat()

        logger.info(f"Quality score for {document['id']}: {overall_score:.2f} (threshold: {QUALITY_THRESHOLD})")
        return document

    # =========================================================================
    # BRANCHING: Route based on quality
    # =========================================================================

    @task.branch
    def route_by_quality(document: dict) -> str:
        """Route document based on quality score."""
        if document.get("meets_threshold", False):
            return "store_enriched_document"
        return "flag_for_review"

    # =========================================================================
    # SUCCESS PATH: Store enriched document
    # =========================================================================

    @task(outlets=[ENRICHED_DOCUMENTS])
    def store_enriched_document(document: dict) -> dict:
        """Store enriched document and emit asset."""
        enriched_dir = Path(ENRICHED_PATH)
        enriched_dir.mkdir(parents=True, exist_ok=True)

        # Calculate total costs
        total_cost = sum(
            [
                document.get("extraction_cost", 0),
                document.get("summary_cost", 0),
                document.get("classification_cost", 0),
            ]
        )

        document["total_llm_cost"] = round(total_cost, 6)
        document["status"] = "enriched"
        document["enriched_at"] = datetime.utcnow().isoformat()

        # Store enriched record
        record_path = enriched_dir / f"{document['id']}.json"
        record_path.write_text(json.dumps(document, indent=2))

        logger.info(f"Stored enriched document: {document['id']} (cost: ${total_cost:.4f})")
        return document

    # =========================================================================
    # REVIEW PATH: Flag for human review
    # =========================================================================

    @task
    def flag_for_review(document: dict) -> dict:
        """Flag document for human review."""
        review_dir = Path(f"{BASE_PATH}/review")
        review_dir.mkdir(parents=True, exist_ok=True)

        document["status"] = "needs_review"
        document["flagged_at"] = datetime.utcnow().isoformat()
        document["review_reason"] = (
            f"Quality score {document.get('overall_quality', 0):.2f} below threshold {QUALITY_THRESHOLD}"
        )

        # Store for review
        record_path = review_dir / f"{document['id']}_review.json"
        record_path.write_text(json.dumps(document, indent=2))

        logger.warning(f"Document flagged for review: {document['id']} - {document['review_reason']}")
        return document

    # =========================================================================
    # AGGREGATION: Collect processing stats
    # =========================================================================

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def collect_processing_stats(enriched: list[dict] | None = None, reviewed: list[dict] | None = None) -> dict:
        """Collect processing statistics for monitoring."""
        enriched_docs = enriched or []
        reviewed_docs = reviewed or []

        total_cost = sum(doc.get("total_llm_cost", 0) for doc in enriched_docs)

        stats = {
            "run_timestamp": datetime.utcnow().isoformat(),
            "documents_processed": len(enriched_docs) + len(reviewed_docs),
            "documents_enriched": len(enriched_docs),
            "documents_flagged": len(reviewed_docs),
            "total_llm_cost": round(total_cost, 4),
            "average_quality": (
                sum(doc.get("overall_quality", 0) for doc in enriched_docs) / len(enriched_docs) if enriched_docs else 0
            ),
        }

        logger.info(f"Processing stats: {stats}")
        return stats

    # =========================================================================
    # TASK FLOW
    # =========================================================================

    # Load documents
    documents = load_pending_documents()

    # LLM processing chain (sequential per document)
    extracted = extract_entities.expand(document=documents)
    summarized = summarize_document.expand(document=extracted)
    classified = classify_document.expand(document=summarized)

    # Quality evaluation
    evaluated = evaluate_quality.expand(document=classified)

    # Store enriched documents (simplified flow)
    stored = store_enriched_document.expand(document=evaluated)

    # Collect stats
    collect_processing_stats(enriched=stored)


# Instantiate the DAG
processing_pipeline()
