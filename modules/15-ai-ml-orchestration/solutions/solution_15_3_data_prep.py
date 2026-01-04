"""
Data Preparation Pipeline - Complete Solution.

This solution demonstrates a production-ready data preparation pipeline with
quality gates, human-in-the-loop review, metadata enrichment, and training
dataset versioning.

Key patterns demonstrated:
- Quality gates with configurable thresholds
- Human-in-the-loop routing for low-confidence items
- Metadata enrichment (sentiment, topics, urgency)
- Versioned training dataset creation with lineage
- Data drift detection and monitoring
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from airflow import Variable
from airflow.sdk import dag, task

# Configuration
QUALITY_THRESHOLD_AUTO = 0.9  # Auto-approve threshold
QUALITY_THRESHOLD_REVIEW = 0.6  # Human review threshold
MAX_REVIEW_QUEUE_SIZE = 100
USE_MOCK_ENRICHMENT = True  # Set to False to use real LLM API
DRIFT_THRESHOLD = 0.15  # Threshold for drift detection

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
    details: dict[str, Any] = field(default_factory=dict)

    @property
    def overall(self) -> float:
        """Weighted overall score."""
        return self.completeness * 0.4 + self.validity * 0.3 + self.consistency * 0.3

    @property
    def routing_decision(self) -> str:
        """Determine routing based on score."""
        if self.overall >= QUALITY_THRESHOLD_AUTO:
            return "auto_approve"
        elif self.overall >= QUALITY_THRESHOLD_REVIEW:
            return "human_review"
        return "reject"

    def get_issues(self) -> list[str]:
        """Get list of quality issues."""
        issues = []
        if self.completeness < 1.0:
            issues.append(f"Missing fields: {self.details.get('missing_fields', [])}")
        if self.validity < 1.0:
            issues.append(f"Invalid fields: {self.details.get('invalid_fields', [])}")
        if self.consistency < 1.0:
            issues.append(f"Consistency issues: {self.details.get('consistency_issues', [])}")
        return issues


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
    except (ValueError, AttributeError):
        return False


def calculate_completeness(item: dict[str, Any], required_fields: list[str]) -> tuple[float, list[str]]:
    """Calculate completeness score based on required fields."""
    missing = []
    for field_name in required_fields:
        value = item.get(field_name)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(field_name)

    if not required_fields:
        return 1.0, []

    score = (len(required_fields) - len(missing)) / len(required_fields)
    return score, missing


def calculate_validity(item: dict[str, Any]) -> tuple[float, list[str]]:
    """Calculate validity score based on format compliance."""
    checks = 0
    valid = 0
    invalid_fields = []

    # Check email format
    if "customer_email" in item:
        checks += 1
        if validate_email(item["customer_email"]):
            valid += 1
        else:
            invalid_fields.append("customer_email")

    # Check date format
    if "created_at" in item:
        checks += 1
        if validate_date(item["created_at"]):
            valid += 1
        else:
            invalid_fields.append("created_at")

    # Check content length
    if "content" in item:
        checks += 1
        if len(item["content"]) >= 10:  # Minimum content length
            valid += 1
        else:
            invalid_fields.append("content_too_short")

    if checks == 0:
        return 1.0, []

    return valid / checks, invalid_fields


def calculate_consistency(item: dict[str, Any]) -> tuple[float, list[str]]:
    """Calculate consistency score based on cross-field logic."""
    checks = 0
    consistent = 0
    issues = []

    # Check if category matches content keywords
    category = item.get("category", "").lower()
    content = item.get("content", "").lower()

    category_keywords = {
        "billing": ["payment", "invoice", "charge", "refund", "bill"],
        "technical": ["error", "bug", "crash", "broken", "not working"],
        "account": ["password", "login", "access", "account"],
        "shipping": ["delivery", "shipping", "package", "tracking"],
        "feedback": ["thank", "great", "excellent", "happy"],
    }

    if category in category_keywords:
        checks += 1
        keywords = category_keywords[category]
        if any(kw in content for kw in keywords):
            consistent += 1
        else:
            issues.append(f"category_content_mismatch: {category}")

    # Check subject relates to content
    subject = item.get("subject", "").lower()
    if subject and content:
        checks += 1
        # Simple check: at least one word from subject appears in content
        subject_words = set(subject.split())
        content_words = set(content.split())
        common_words = subject_words & content_words - {"the", "a", "an", "is", "are", "i", "my"}
        if common_words:
            consistent += 1
        else:
            issues.append("subject_content_disconnect")

    if checks == 0:
        return 1.0, []

    return consistent / checks, issues


def mock_sentiment_analysis(text: str) -> dict[str, Any]:
    """Mock sentiment analysis for testing."""
    positive_words = ["thank", "great", "excellent", "happy", "resolved", "love", "amazing"]
    negative_words = ["angry", "frustrated", "terrible", "broken", "urgent", "worst", "hate"]

    text_lower = text.lower()
    pos_count = sum(1 for w in positive_words if w in text_lower)
    neg_count = sum(1 for w in negative_words if w in text_lower)

    if pos_count > neg_count:
        return {
            "sentiment": "positive",
            "confidence": min(0.95, 0.7 + (pos_count * 0.05)),
            "pos_signals": pos_count,
            "neg_signals": neg_count,
        }
    elif neg_count > pos_count:
        return {
            "sentiment": "negative",
            "confidence": min(0.95, 0.7 + (neg_count * 0.05)),
            "pos_signals": pos_count,
            "neg_signals": neg_count,
        }
    return {
        "sentiment": "neutral",
        "confidence": 0.6,
        "pos_signals": pos_count,
        "neg_signals": neg_count,
    }


def mock_topic_extraction(text: str) -> list[str]:
    """Mock topic extraction for testing."""
    topics = []
    topic_keywords = {
        "billing": ["invoice", "payment", "charge", "refund", "bill", "price"],
        "technical": ["error", "bug", "crash", "not working", "broken", "slow"],
        "account": ["password", "login", "access", "account", "profile", "reset"],
        "shipping": ["delivery", "shipping", "package", "tracking", "arrived", "order"],
        "feature_request": ["would like", "please add", "suggestion", "feature"],
    }

    text_lower = text.lower()
    for topic, keywords in topic_keywords.items():
        if any(kw in text_lower for kw in keywords):
            topics.append(topic)

    return topics if topics else ["general"]


def mock_urgency_detection(text: str) -> dict[str, Any]:
    """Mock urgency detection for testing."""
    high_urgency = ["asap", "urgent", "immediately", "critical", "emergency", "now"]
    medium_urgency = ["soon", "quickly", "important", "waiting"]

    text_lower = text.lower()

    if any(p in text_lower for p in high_urgency):
        return {"urgency_level": "high", "confidence": 0.9}
    elif any(p in text_lower for p in medium_urgency):
        return {"urgency_level": "medium", "confidence": 0.75}
    return {"urgency_level": "normal", "confidence": 0.7}


def calculate_statistics(data: list[dict[str, Any]]) -> dict[str, Any]:
    """Calculate dataset statistics for versioning and drift detection."""
    if not data:
        return {}

    stats = {
        "record_count": len(data),
        "distributions": {},
    }

    # Category distribution
    categories: dict[str, int] = {}
    sentiments: dict[str, int] = {}
    urgency_levels: dict[str, int] = {}
    topic_counts: dict[str, int] = {}

    for item in data:
        ticket = item.get("ticket", item)
        enrichment = item.get("enrichment", {})

        # Category
        cat = ticket.get("category", "unknown")
        categories[cat] = categories.get(cat, 0) + 1

        # Sentiment
        sent = enrichment.get("sentiment", {}).get("sentiment", "unknown")
        sentiments[sent] = sentiments.get(sent, 0) + 1

        # Urgency
        urg = enrichment.get("urgency", {}).get("urgency_level", "unknown")
        urgency_levels[urg] = urgency_levels.get(urg, 0) + 1

        # Topics
        for topic in enrichment.get("topics", []):
            topic_counts[topic] = topic_counts.get(topic, 0) + 1

    stats["distributions"] = {
        "category": {
            "values": categories,
            "mean": len(data) / len(categories) if categories else 0,
            "std": 0,  # Simplified
        },
        "sentiment": {
            "values": sentiments,
            "mean": len(data) / len(sentiments) if sentiments else 0,
            "std": 0,
        },
        "urgency": {
            "values": urgency_levels,
            "mean": len(data) / len(urgency_levels) if urgency_levels else 0,
            "std": 0,
        },
        "topics": {
            "values": topic_counts,
            "unique_count": len(topic_counts),
        },
    }

    return stats


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
        required_fields = ["id", "customer_email", "subject", "content", "created_at"]

        results: dict[str, list[dict[str, Any]]] = {
            "auto_approve": [],
            "human_review": [],
            "reject": [],
        }

        for ticket in tickets:
            # Calculate quality scores
            completeness, missing = calculate_completeness(ticket, required_fields)
            validity, invalid = calculate_validity(ticket)
            consistency, consistency_issues = calculate_consistency(ticket)

            # Create quality score
            quality = QualityScore(
                completeness=completeness,
                validity=validity,
                consistency=consistency,
                details={
                    "missing_fields": missing,
                    "invalid_fields": invalid,
                    "consistency_issues": consistency_issues,
                },
            )

            # Route based on decision
            decision = quality.routing_decision
            results[decision].append(
                {
                    "ticket": ticket,
                    "quality_score": quality.overall,
                    "decision": decision,
                    "issues": quality.get_issues(),
                    "details": {
                        "completeness": completeness,
                        "validity": validity,
                        "consistency": consistency,
                    },
                }
            )

            logger.debug(f"Ticket {ticket['id']}: score={quality.overall:.2f}, decision={decision}")

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
        queue_key = "data_prep_review_queue"

        # Load existing queue
        existing_queue = Variable.get(queue_key, default_var=[], deserialize_json=True)

        # Add new items with metadata
        for item in review_items:
            queue_entry = {
                "item_id": item["ticket"]["id"],
                "quality_score": item["quality_score"],
                "issues": item["issues"],
                "added_at": datetime.now().isoformat(),
                "status": "pending",
                "assigned_to": None,
            }
            existing_queue.append(queue_entry)

        # Enforce max queue size (remove oldest if exceeded)
        if len(existing_queue) > MAX_REVIEW_QUEUE_SIZE:
            overflow = len(existing_queue) - MAX_REVIEW_QUEUE_SIZE
            existing_queue = existing_queue[overflow:]
            logger.warning(f"Review queue overflow: removed {overflow} oldest items")

        # Save updated queue
        Variable.set(queue_key, existing_queue, serialize_json=True)

        logger.info(f"Review queue updated: {len(review_items)} items added, total queue size: {len(existing_queue)}")

        return existing_queue

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
        enriched: list[dict[str, Any]] = []

        for item in approved_items:
            ticket = item.get("ticket", item)
            content = ticket.get("content", "")

            # Perform enrichment
            sentiment = mock_sentiment_analysis(content)
            topics = mock_topic_extraction(content)
            urgency = mock_urgency_detection(content)

            enriched_item = {
                "ticket": ticket,
                "quality_score": item.get("quality_score", 1.0),
                "enrichment": {
                    "sentiment": sentiment,
                    "topics": topics,
                    "urgency": urgency,
                    "enriched_at": datetime.now().isoformat(),
                    "enrichment_version": "1.0",
                },
            }

            enriched.append(enriched_item)

            logger.debug(
                f"Enriched {ticket['id']}: "
                f"sentiment={sentiment['sentiment']}, "
                f"topics={topics}, "
                f"urgency={urgency['urgency_level']}"
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
        if not enriched_data:
            return {
                "version_id": None,
                "message": "No data to version",
            }

        # Create content hash from data
        content_json = json.dumps(enriched_data, sort_keys=True, default=str)
        content_hash = hashlib.sha256(content_json.encode()).hexdigest()[:12]

        # Generate version ID
        version_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{content_hash}"

        # Calculate statistics
        statistics = calculate_statistics(enriched_data)

        # Create version info
        version_info = {
            "version_id": version_id,
            "created_at": datetime.now().isoformat(),
            "record_count": len(enriched_data),
            "content_hash": content_hash,
            "statistics": statistics,
            "lineage": {
                "source": "support_tickets",
                "transformations": ["quality_validation", "enrichment"],
                "parent_version": None,
            },
        }

        # Store version metadata
        versions_key = "data_prep_dataset_versions"
        versions = Variable.get(versions_key, default_var=[], deserialize_json=True)
        versions.append(
            {
                "version_id": version_id,
                "created_at": version_info["created_at"],
                "record_count": len(enriched_data),
            }
        )
        Variable.set(versions_key, versions, serialize_json=True)

        logger.info(f"Created dataset version: {version_id} with {len(enriched_data)} records")
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
        baseline_key = "data_prep_baseline_stats"

        # Load baseline statistics
        baseline_stats = Variable.get(baseline_key, default_var=None, deserialize_json=True)

        drift_report = {
            "timestamp": datetime.now().isoformat(),
            "current_version": current_version.get("version_id"),
            "drifted_features": [],
            "overall_drift": False,
            "recommendation": "continue_processing",
        }

        if baseline_stats is None:
            # No baseline - use current as baseline
            Variable.set(
                baseline_key,
                current_version.get("statistics", {}),
                serialize_json=True,
            )
            drift_report["message"] = "No baseline found - current stats set as baseline"
            logger.info("Set current statistics as baseline for drift detection")
            return drift_report

        # Compare distributions
        current_stats = current_version.get("statistics", {})
        current_distributions = current_stats.get("distributions", {})
        baseline_distributions = baseline_stats.get("distributions", {})

        for feature in current_distributions:
            if feature not in baseline_distributions:
                continue

            current_dist = current_distributions[feature]
            baseline_dist = baseline_distributions[feature]

            # Compare category/value distributions
            current_values = current_dist.get("values", {})
            baseline_values = baseline_dist.get("values", {})

            # Calculate distribution difference (simplified chi-square-like metric)
            all_keys = set(current_values.keys()) | set(baseline_values.keys())
            if not all_keys:
                continue

            current_total = sum(current_values.values()) or 1
            baseline_total = sum(baseline_values.values()) or 1

            drift_score = 0
            for key in all_keys:
                current_pct = current_values.get(key, 0) / current_total
                baseline_pct = baseline_values.get(key, 0) / baseline_total
                drift_score += abs(current_pct - baseline_pct)

            normalized_drift = drift_score / len(all_keys)

            if normalized_drift > DRIFT_THRESHOLD:
                drift_report["drifted_features"].append(
                    {
                        "feature": feature,
                        "drift_score": round(normalized_drift, 4),
                        "current_distribution": current_values,
                        "baseline_distribution": baseline_values,
                    }
                )

        # Determine overall drift
        drift_report["overall_drift"] = len(drift_report["drifted_features"]) > 0

        if drift_report["overall_drift"]:
            drift_report["recommendation"] = "investigate_drift"
            logger.warning(
                f"Data drift detected in features: {[f['feature'] for f in drift_report['drifted_features']]}"
            )
        else:
            # Update baseline with current stats periodically
            Variable.set(baseline_key, current_stats, serialize_json=True)
            logger.info("No significant drift detected - baseline updated")

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
        # Calculate quality pass rate
        total_processed = sum(len(v) for v in quality_results.values())
        pass_rate = len(quality_results.get("auto_approve", [])) / total_processed if total_processed > 0 else 0

        # Aggregate enrichment stats
        sentiment_dist: dict[str, int] = {}
        urgency_dist: dict[str, int] = {}

        for item in enriched_data:
            enrichment = item.get("enrichment", {})
            sent = enrichment.get("sentiment", {}).get("sentiment", "unknown")
            sentiment_dist[sent] = sentiment_dist.get(sent, 0) + 1

            urg = enrichment.get("urgency", {}).get("urgency_level", "unknown")
            urgency_dist[urg] = urgency_dist.get(urg, 0) + 1

        summary = {
            "timestamp": datetime.now().isoformat(),
            "quality_metrics": {
                "total_processed": total_processed,
                "auto_approved": len(quality_results.get("auto_approve", [])),
                "sent_for_review": len(quality_results.get("human_review", [])),
                "rejected": len(quality_results.get("reject", [])),
                "pass_rate": round(pass_rate, 3),
            },
            "enrichment_metrics": {
                "enriched_count": len(enriched_data),
                "sentiment_distribution": sentiment_dist,
                "urgency_distribution": urgency_dist,
            },
            "versioning": {
                "version_id": version_info.get("version_id"),
                "record_count": version_info.get("record_count", 0),
                "content_hash": version_info.get("content_hash"),
            },
            "drift_status": {
                "drift_detected": drift_report.get("overall_drift", False),
                "drifted_features": [f["feature"] for f in drift_report.get("drifted_features", [])],
                "recommendation": drift_report.get("recommendation"),
            },
            "health_status": "healthy" if not drift_report.get("overall_drift") else "warning",
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
