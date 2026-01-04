"""
Data Preparation Pipeline Starter Code.

Build a production-ready data preparation pipeline with quality gates,
human-in-the-loop review, metadata enrichment, and training dataset versioning.

Requirements:
1. Implement quality gates with configurable thresholds
2. Create human-in-the-loop routing for low-confidence items
3. Build metadata enrichment (sentiment, topics, urgency)
4. Implement versioned training dataset creation
5. Add data drift detection

Instructions:
- Complete all TODO sections
- Run with: airflow dags test data_prep_pipeline <date>
- Test with sample support tickets to validate the pipeline
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task

# Configuration
QUALITY_THRESHOLD_AUTO = 0.9  # Auto-approve threshold
QUALITY_THRESHOLD_REVIEW = 0.6  # Human review threshold
MAX_REVIEW_QUEUE_SIZE = 100
USE_MOCK_ENRICHMENT = True  # Set to False to use real LLM API

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class QualityScore:
    """Quality assessment for a data item."""

    completeness: float  # 0-1: required fields present
    validity: float  # 0-1: format compliance
    consistency: float  # 0-1: cross-field consistency

    @property
    def overall(self) -> float:
        """Weighted overall score."""
        # TODO: Implement weighted scoring
        # - completeness: 40% weight
        # - validity: 30% weight
        # - consistency: 30% weight
        return 0.0

    @property
    def routing_decision(self) -> str:
        """Determine routing based on score."""
        # TODO: Implement routing logic
        # - overall >= 0.9: "auto_approve"
        # - overall >= 0.6: "human_review"
        # - otherwise: "reject"
        return "reject"


@dataclass
class DatasetVersion:
    """Immutable dataset version with metadata."""

    version_id: str
    created_at: str
    record_count: int
    content_hash: str
    statistics: dict[str, Any] = field(default_factory=dict)
    lineage: dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Helper Functions
# =============================================================================


def validate_email(email: str) -> bool:
    """Validate email format."""
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


def validate_date(date_str: str) -> bool:
    """Validate ISO date format."""
    try:
        datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def calculate_completeness(item: dict[str, Any], required_fields: list[str]) -> float:
    """Calculate completeness score based on required fields."""
    # TODO: Implement completeness calculation
    # Return ratio of present required fields to total required fields
    _ = item, required_fields  # Use in your implementation
    return 0.0


def calculate_validity(item: dict[str, Any]) -> float:
    """Calculate validity score based on format compliance."""
    # TODO: Implement validity calculation
    # Check email format, date format, required patterns
    _ = item  # Use in your implementation
    return 0.0


def calculate_consistency(item: dict[str, Any]) -> float:
    """Calculate consistency score based on cross-field logic."""
    # TODO: Implement consistency calculation
    # Check logical relationships between fields
    _ = item  # Use in your implementation
    return 0.0


def mock_sentiment_analysis(text: str) -> dict[str, Any]:
    """Mock sentiment analysis for testing."""
    # Simple rule-based mock
    positive_words = ["thank", "great", "excellent", "happy", "resolved"]
    negative_words = ["angry", "frustrated", "terrible", "broken", "urgent"]

    text_lower = text.lower()
    pos_count = sum(1 for w in positive_words if w in text_lower)
    neg_count = sum(1 for w in negative_words if w in text_lower)

    if pos_count > neg_count:
        return {"sentiment": "positive", "confidence": 0.7 + (pos_count * 0.05)}
    elif neg_count > pos_count:
        return {"sentiment": "negative", "confidence": 0.7 + (neg_count * 0.05)}
    return {"sentiment": "neutral", "confidence": 0.6}


def mock_topic_extraction(text: str) -> list[str]:
    """Mock topic extraction for testing."""
    topics = []
    topic_keywords = {
        "billing": ["invoice", "payment", "charge", "refund", "bill"],
        "technical": ["error", "bug", "crash", "not working", "broken"],
        "account": ["password", "login", "access", "account", "profile"],
        "shipping": ["delivery", "shipping", "package", "tracking", "arrived"],
    }

    text_lower = text.lower()
    for topic, keywords in topic_keywords.items():
        if any(kw in text_lower for kw in keywords):
            topics.append(topic)

    return topics if topics else ["general"]


def mock_urgency_detection(text: str) -> dict[str, Any]:
    """Mock urgency detection for testing."""
    urgent_patterns = ["asap", "urgent", "immediately", "critical", "emergency"]
    text_lower = text.lower()

    is_urgent = any(p in text_lower for p in urgent_patterns)

    return {
        "urgency_level": "high" if is_urgent else "normal",
        "confidence": 0.85 if is_urgent else 0.7,
    }


# =============================================================================
# DAG Definition
# =============================================================================


@dag(
    dag_id="data_prep_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["ai-ml", "data-prep", "module-15"],
    doc_md=__doc__,
)
def data_prep_pipeline():
    """
    Data Preparation Pipeline.

    Processes raw support tickets through quality gates, enrichment,
    and creates versioned training datasets.
    """

    @task
    def get_sample_tickets() -> list[dict[str, Any]]:
        """
        Get sample support tickets for processing.

        In production, this would fetch from a database or queue.
        """
        return [
            {
                "id": "ticket-001",
                "customer_email": "john.doe@example.com",
                "subject": "Payment not processed",
                "content": "I tried to make a payment but got an error. Please help urgently!",
                "created_at": "2024-01-15T10:30:00Z",
                "category": "billing",
            },
            {
                "id": "ticket-002",
                "customer_email": "jane.smith@example.com",
                "subject": "Great customer service",
                "content": "Thank you for resolving my issue so quickly. Excellent support!",
                "created_at": "2024-01-15T11:45:00Z",
                "category": "feedback",
            },
            {
                "id": "ticket-003",
                "customer_email": "invalid-email",  # Invalid email for testing
                "subject": "App keeps crashing",
                "content": "The mobile app crashes every time I try to login. Very frustrated!",
                "created_at": "2024-01-15T14:20:00Z",
                "category": "technical",
            },
            {
                "id": "ticket-004",
                "customer_email": "bob.wilson@example.com",
                "subject": "",  # Missing subject for testing
                "content": "Where is my package? Tracking shows delivered but I don't have it.",
                "created_at": "2024-01-15T16:00:00Z",
                "category": "shipping",
            },
            {
                "id": "ticket-005",
                "customer_email": "alice.johnson@example.com",
                "subject": "Account access issue",
                "content": "I forgot my password and the reset email isn't arriving.",
                "created_at": "2024-01-15T17:30:00Z",
                "category": "account",
            },
        ]

    @task
    def validate_quality(tickets: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        """
        Validate ticket quality and route based on scores.

        Args:
            tickets: Raw ticket data.

        Returns:
            Dict with routed tickets: auto_approve, human_review, reject.
        """
        # TODO: Implement quality validation
        # 1. Define required fields
        # 2. For each ticket:
        #    - Calculate completeness score
        #    - Calculate validity score
        #    - Calculate consistency score
        #    - Create QualityScore instance
        #    - Route based on overall score
        # 3. Return categorized tickets

        required_fields = ["id", "customer_email", "subject", "content", "created_at"]

        results: dict[str, list[dict[str, Any]]] = {
            "auto_approve": [],
            "human_review": [],
            "reject": [],
        }

        for ticket in tickets:
            # TODO: Calculate quality scores and route
            # Use calculate_completeness, calculate_validity, calculate_consistency
            # Create QualityScore and use routing_decision

            # Placeholder - remove when implementing
            _ = required_fields
            results["reject"].append(
                {
                    "ticket": ticket,
                    "quality_score": 0.0,
                    "decision": "reject",
                }
            )

        logger.info(
            f"Quality validation complete: "
            f"approved={len(results['auto_approve'])}, "
            f"review={len(results['human_review'])}, "
            f"rejected={len(results['reject'])}"
        )

        return results

    @task
    def manage_review_queue(
        review_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Manage the human review queue.

        Args:
            review_items: Items needing human review.

        Returns:
            Updated review queue with new items.
        """
        # TODO: Implement review queue management
        # 1. Load existing queue from Variable
        # 2. Add new items with metadata (timestamp, score, reasons)
        # 3. Enforce max queue size (remove oldest if exceeded)
        # 4. Save updated queue
        # 5. Return queue for monitoring

        queue_key = "data_prep_review_queue"

        # TODO: Your implementation here
        _ = queue_key  # Use in your implementation

        logger.info(f"Review queue updated: {len(review_items)} items added")
        return review_items

    @task
    def enrich_metadata(
        approved_items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Enrich tickets with additional metadata.

        Args:
            approved_items: Quality-approved tickets.

        Returns:
            Enriched tickets with sentiment, topics, urgency.
        """
        # TODO: Implement metadata enrichment
        # 1. For each ticket:
        #    - Extract sentiment (use mock or real LLM)
        #    - Extract topics
        #    - Detect urgency
        #    - Add processing metadata (timestamp, version)
        # 2. Return enriched tickets

        enriched: list[dict[str, Any]] = []

        for item in approved_items:
            ticket = item.get("ticket", item)
            content = ticket.get("content", "")

            # TODO: Call enrichment functions and add to ticket
            # - mock_sentiment_analysis(content)
            # - mock_topic_extraction(content)
            # - mock_urgency_detection(content)

            _ = content  # Use in your implementation

            enriched.append(
                {
                    "ticket": ticket,
                    "enrichment": {
                        "sentiment": {},
                        "topics": [],
                        "urgency": {},
                        "enriched_at": datetime.now().isoformat(),
                    },
                }
            )

        logger.info(f"Enriched {len(enriched)} tickets with metadata")
        return enriched

    @task
    def create_dataset_version(
        enriched_data: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Create a versioned training dataset.

        Args:
            enriched_data: Enriched and validated tickets.

        Returns:
            Dataset version metadata with statistics.
        """
        # TODO: Implement dataset versioning
        # 1. Create content hash from data
        # 2. Generate version ID (prefix_date_hash)
        # 3. Calculate statistics:
        #    - Record count
        #    - Field distributions
        #    - Category distribution
        #    - Sentiment distribution
        # 4. Store dataset with lineage information
        # 5. Save version to Variable for tracking

        if not enriched_data:
            return {
                "version_id": None,
                "message": "No data to version",
            }

        # TODO: Your implementation here
        # Create content hash
        content_json = json.dumps(enriched_data, sort_keys=True, default=str)
        content_hash = hashlib.sha256(content_json.encode()).hexdigest()[:12]

        version_id = f"dataset_{datetime.now().strftime('%Y%m%d')}_{content_hash}"

        version_info = {
            "version_id": version_id,
            "created_at": datetime.now().isoformat(),
            "record_count": len(enriched_data),
            "content_hash": content_hash,
            "statistics": {},  # TODO: Calculate statistics
            "lineage": {
                "source": "support_tickets",
                "transformations": ["quality_validation", "enrichment"],
            },
        }

        logger.info(f"Created dataset version: {version_id}")
        return version_info

    @task
    def detect_drift(
        current_version: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Detect data drift compared to baseline.

        Args:
            current_version: Current dataset version info.

        Returns:
            Drift detection report.
        """
        # TODO: Implement drift detection
        # 1. Load baseline statistics from Variable
        # 2. Compare current statistics to baseline:
        #    - Distribution changes
        #    - Category shifts
        #    - Sentiment trends
        # 3. Flag significant drifts (threshold-based)
        # 4. Update baseline if no drift or on schedule
        # 5. Return drift report

        baseline_key = "data_prep_baseline_stats"

        # TODO: Load baseline and compare
        _ = baseline_key  # Use in your implementation

        drift_report = {
            "timestamp": datetime.now().isoformat(),
            "current_version": current_version.get("version_id"),
            "drifted_features": [],
            "overall_drift": False,
            "recommendation": "continue_processing",
        }

        logger.info(f"Drift detection complete: drift={drift_report['overall_drift']}")
        return drift_report

    @task
    def summarize_run(
        quality_results: dict[str, list[dict[str, Any]]],
        enriched_data: list[dict[str, Any]],
        version_info: dict[str, Any],
        drift_report: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Generate pipeline run summary.

        Returns:
            Comprehensive summary with metrics and recommendations.
        """
        summary = {
            "timestamp": datetime.now().isoformat(),
            "quality_metrics": {
                "total_processed": sum(len(v) for v in quality_results.values()),
                "auto_approved": len(quality_results.get("auto_approve", [])),
                "sent_for_review": len(quality_results.get("human_review", [])),
                "rejected": len(quality_results.get("reject", [])),
            },
            "enrichment_metrics": {
                "enriched_count": len(enriched_data),
            },
            "versioning": {
                "version_id": version_info.get("version_id"),
                "record_count": version_info.get("record_count", 0),
            },
            "drift_status": {
                "drift_detected": drift_report.get("overall_drift", False),
                "drifted_features": drift_report.get("drifted_features", []),
            },
        }

        logger.info(f"Pipeline run summary:\n{json.dumps(summary, indent=2)}")
        return summary

    # =========================================================================
    # DAG Flow
    # =========================================================================

    # Step 1: Get raw tickets
    tickets = get_sample_tickets()

    # Step 2: Validate quality and route
    quality_results = validate_quality(tickets)

    # Step 3: Manage review queue (for items needing human review)
    # Note: In a real pipeline, this would connect to a review system
    review_queue = manage_review_queue(quality_results["human_review"])

    # Step 4: Enrich approved items
    enriched = enrich_metadata(quality_results["auto_approve"])

    # Step 5: Create versioned dataset
    version_info = create_dataset_version(enriched)

    # Step 6: Detect drift
    drift_report = detect_drift(version_info)

    # Step 7: Summarize run
    summary = summarize_run(quality_results, enriched, version_info, drift_report)

    # Set dependencies
    review_queue >> summary


# Instantiate the DAG
data_prep_pipeline()
