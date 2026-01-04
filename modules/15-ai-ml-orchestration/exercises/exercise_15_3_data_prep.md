# Exercise 15.3: Data Preparation Pipeline

Build a production-ready data preparation pipeline with quality gates, human-in-the-loop review, metadata enrichment, and training dataset versioning.

## Learning Goals

- Design quality gates with configurable thresholds
- Implement human-in-the-loop patterns for low-confidence items
- Build metadata extraction and enrichment workflows
- Create versioned training datasets for ML models
- Apply data drift detection patterns

## Scenario

You're preparing data for an ML model that classifies customer support tickets. The pipeline should:

1. **Ingest**: Load raw ticket data from a source
2. **Validate**: Apply quality checks with configurable thresholds
3. **Route**: Send low-confidence items for human review
4. **Enrich**: Extract and add metadata (sentiment, topics, urgency)
5. **Version**: Create versioned training datasets with lineage

Each step should support idempotent processing and comprehensive observability.

## Requirements

### Task 1: Quality Gate Framework

Create a reusable quality validation system that:

- Validates data completeness (required fields present)
- Checks data format compliance (date formats, email patterns)
- Scores data quality with configurable thresholds
- Routes items based on quality score (pass/review/reject)

### Task 2: Human-in-the-Loop Routing

Implement routing logic for human review:

- Define confidence thresholds for automatic processing
- Create a review queue for borderline items
- Store review decisions for model improvement
- Support reviewer assignment and workload balancing

### Task 3: Metadata Enrichment

Create enrichment tasks that:

- Extract sentiment using LLM or rule-based analysis
- Identify topics and categories
- Estimate urgency based on content patterns
- Add timestamp and source metadata

### Task 4: Training Dataset Versioning

Implement dataset versioning that:

- Creates immutable dataset snapshots
- Tracks data lineage (source → processed → training)
- Supports dataset diffing between versions
- Stores dataset statistics and distribution metrics

### Task 5: Data Drift Detection

Add monitoring for data drift:

- Compare current batch to historical distribution
- Alert on significant statistical changes
- Track feature distribution over time
- Log drift metrics for model monitoring

## Success Criteria

- [ ] Quality gates prevent invalid data from processing
- [ ] Human review queue functions correctly
- [ ] Metadata enrichment adds value to raw data
- [ ] Dataset versions are immutable and traceable
- [ ] Drift detection identifies distribution changes
- [ ] All operations are idempotent and resumable

## Hints

<details>
<summary>Hint 1: Quality Scoring Framework</summary>

```python
from dataclasses import dataclass


@dataclass
class QualityScore:
    """Quality assessment for a data item."""

    completeness: float  # 0-1: required fields present
    validity: float  # 0-1: format compliance
    consistency: float  # 0-1: cross-field consistency

    @property
    def overall(self) -> float:
        """Weighted overall score."""
        return self.completeness * 0.4 + self.validity * 0.3 + self.consistency * 0.3

    @property
    def routing_decision(self) -> str:
        """Determine routing based on score."""
        if self.overall >= 0.9:
            return "auto_approve"
        elif self.overall >= 0.6:
            return "human_review"
        else:
            return "reject"
```

</details>

<details>
<summary>Hint 2: Human Review Queue Pattern</summary>

```python
def route_for_review(
    item: dict,
    quality_score: QualityScore,
    review_queue_key: str = "data_prep_review_queue",
) -> dict:
    """Route item for human review if needed."""
    from airflow import Variable

    decision = quality_score.routing_decision

    if decision == "human_review":
        # Add to review queue
        queue = Variable.get(review_queue_key, default_var=[], deserialize_json=True)
        queue.append(
            {
                "item_id": item["id"],
                "quality_score": quality_score.overall,
                "timestamp": datetime.now().isoformat(),
                "reasons": get_review_reasons(quality_score),
            }
        )
        Variable.set(review_queue_key, queue, serialize_json=True)

    return {
        "item": item,
        "decision": decision,
        "score": quality_score.overall,
    }
```

</details>

<details>
<summary>Hint 3: Dataset Versioning</summary>

```python
import hashlib
import json


def create_dataset_version(
    data: list[dict],
    version_prefix: str = "dataset",
) -> dict:
    """Create an immutable dataset version."""
    # Create content hash for version ID
    content = json.dumps(data, sort_keys=True)
    content_hash = hashlib.sha256(content.encode()).hexdigest()[:12]

    version_id = f"{version_prefix}_{datetime.now().strftime('%Y%m%d')}_{content_hash}"

    # Calculate statistics
    stats = {
        "record_count": len(data),
        "created_at": datetime.now().isoformat(),
        "content_hash": content_hash,
        "field_stats": calculate_field_stats(data),
    }

    return {
        "version_id": version_id,
        "data": data,
        "statistics": stats,
        "lineage": {"parent_version": None, "transformations": []},
    }
```

</details>

<details>
<summary>Hint 4: Drift Detection</summary>

```python
def detect_drift(
    current_stats: dict,
    baseline_stats: dict,
    threshold: float = 0.1,
) -> dict:
    """Detect statistical drift between datasets."""
    drift_report = {"drifted_features": [], "overall_drift": False}

    for feature in current_stats.get("distributions", {}):
        current_dist = current_stats["distributions"][feature]
        baseline_dist = baseline_stats["distributions"].get(feature, {})

        # Calculate distribution difference (simplified)
        diff = abs(current_dist.get("mean", 0) - baseline_dist.get("mean", 0))
        normalized_diff = diff / (baseline_dist.get("std", 1) + 0.001)

        if normalized_diff > threshold:
            drift_report["drifted_features"].append(
                {
                    "feature": feature,
                    "drift_score": normalized_diff,
                    "current_mean": current_dist.get("mean"),
                    "baseline_mean": baseline_dist.get("mean"),
                }
            )

    drift_report["overall_drift"] = len(drift_report["drifted_features"]) > 0
    return drift_report
```

</details>

## Files

- **Starter**: `exercise_15_3_data_prep_starter.py`
- **Solution**: `../solutions/solution_15_3_data_prep.py`

## Estimated Time

75-90 minutes

## Next Steps

After completing this exercise:

1. Integrate with a real data labeling platform (Label Studio, Scale AI)
2. Add A/B testing for different enrichment strategies
3. Implement continuous training triggers based on data quality
4. Build dashboards for data quality monitoring
