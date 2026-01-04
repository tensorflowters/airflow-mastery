# Case Study: Spotify's Recommendation Pipelines

## Company Context

**Scale**: 600+ million users, 100+ million tracks, billions of daily streams
**Challenge**: Generate personalized recommendations across multiple surfaces (Discover Weekly, Release Radar, Home feed)
**Requirements**: Train models on petabytes of listening data, serve fresh recommendations daily

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         SPOTIFY RECOMMENDATIONS                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │   Event      │───▶│   Feature    │───▶│   Model      │                  │
│  │   Ingestion  │    │   Pipeline   │    │   Training   │                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│        │                   │                    │                           │
│        ▼                   ▼                    ▼                           │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │   Stream     │    │   Feature    │    │   Model      │                  │
│  │   Processing │    │   Store      │    │   Registry   │                  │
│  │   (Kafka)    │    │   (BigQuery) │    │   (MLflow)   │                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│                                                │                           │
│                                                ▼                           │
│                                          ┌──────────────┐                  │
│                                          │   A/B Test   │                  │
│                                          │   Framework  │                  │
│                                          └──────────────┘                  │
│                                                │                           │
│                                                ▼                           │
│                                          ┌──────────────┐                  │
│                                          │   Serving    │                  │
│                                          │   Layer      │                  │
│                                          └──────────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Key Patterns Used

### 1. Dynamic Task Mapping for User Segments

Spotify processes different user segments with tailored models. Dynamic mapping enables parallel processing:

```python
from datetime import datetime

from airflow.sdk import dag, task


@dag(schedule="@daily", start_date=datetime(2024, 1, 1))
def recommendation_pipeline():
    @task
    def get_user_segments() -> list[dict]:
        """Identify user segments for parallel processing."""
        return [
            {"segment": "new_users", "model": "explore_heavy"},
            {"segment": "power_listeners", "model": "personalization_deep"},
            {"segment": "casual_listeners", "model": "popularity_blend"},
            {"segment": "genre_focused", "model": "genre_specialist"},
        ]

    @task
    def generate_recommendations(segment_config: dict) -> dict:
        """Generate recommendations for a user segment."""
        segment = segment_config["segment"]
        model = segment_config["model"]

        # Each segment processes millions of users in parallel
        # Uses segment-specific model and features
        recommendations = run_recommendation_model(
            segment=segment,
            model_id=model,
        )

        return {
            "segment": segment,
            "users_processed": recommendations["count"],
            "model_version": model,
        }

    @task
    def aggregate_metrics(results: list[dict]) -> dict:
        """Combine metrics across all segments."""
        total_users = sum(r["users_processed"] for r in results)
        return {"total_users": total_users, "segments": len(results)}

    # Dynamic mapping: one task per segment, running in parallel
    segments = get_user_segments()
    results = generate_recommendations.expand(segment_config=segments)
    aggregate_metrics(results)
```

### 2. Asset-Driven Feature Pipelines

Features are computed as Assets, triggering downstream model training:

```python
from airflow.sdk import Asset, dag, task

# Feature assets that models depend on
listening_features = Asset("features/listening_history")
audio_features = Asset("features/audio_embeddings")
social_features = Asset("features/social_graph")


@dag(schedule=None)  # Triggered by upstream data
def feature_pipeline():
    @task(outlets=[listening_features])
    def compute_listening_features():
        """Daily listening pattern features."""
        # Compute user listening patterns from event logs
        pass

    @task(outlets=[audio_features])
    def compute_audio_features():
        """Audio embedding features from track analysis."""
        # Generate embeddings from audio analysis
        pass


# Model training triggered when features update
@dag(
    schedule=[listening_features, audio_features, social_features],
    start_date=datetime(2024, 1, 1),
)
def model_training():
    @task
    def train_recommendation_model():
        """Train on latest features."""
        # Triggered automatically when any feature asset updates
        pass
```

### 3. A/B Test Orchestration

New models are tested through controlled experiments:

```python
@dag(schedule="@daily")
def ab_test_pipeline():
    @task
    def setup_experiment(experiment_id: str) -> dict:
        """Configure A/B test parameters."""
        return {
            "experiment_id": experiment_id,
            "control": "model_v2.1",
            "treatment": "model_v2.2",
            "allocation": 0.10,  # 10% of users
        }

    @task
    def allocate_users(config: dict) -> dict:
        """Assign users to control/treatment groups."""
        # Consistent hashing ensures stable assignment
        pass

    @task
    def generate_variant_recommendations(config: dict, variant: str):
        """Generate recommendations for each variant."""
        pass

    @task
    def collect_metrics(experiment_id: str) -> dict:
        """Collect streaming metrics for analysis."""
        # Metrics: streams, skips, saves, time spent
        pass

    @task
    def evaluate_significance(metrics: dict) -> dict:
        """Statistical analysis of experiment results."""
        # Calculate p-values, confidence intervals
        pass
```

## Lessons Learned

### What Worked

1. **Dynamic mapping for user segments** - Enabled horizontal scaling without code changes
2. **Asset-driven dependencies** - Features and models stay in sync automatically
3. **Gradual rollout with A/B testing** - Caught regressions before full deployment
4. **KubernetesExecutor for burst capacity** - Handled holiday traffic spikes

### Challenges Encountered

1. **Feature freshness vs. cost** - Had to balance real-time features against compute costs
2. **Model versioning complexity** - Multiple concurrent experiments required careful tracking
3. **Cross-DAG dependencies** - Needed clear Asset contracts between teams

### Key Metrics

| Metric                 | Before Airflow | After Airflow |
| ---------------------- | -------------- | ------------- |
| Pipeline reliability   | 94%            | 99.5%         |
| Feature freshness      | 24 hours       | 4 hours       |
| Experiment velocity    | 2/month        | 10/month      |
| Developer productivity | Baseline       | +40%          |

## Code Patterns

### Pattern: Parallel User Segment Processing

```python
# From Module 06: Dynamic Task Mapping
@task
def get_segments() -> list[str]:
    return ["segment_a", "segment_b", "segment_c"]


@task
def process_segment(segment: str) -> dict:
    # Heavy computation for each segment
    return {"segment": segment, "status": "complete"}


# Creates parallel tasks automatically
segments = get_segments()
results = process_segment.expand(segment=segments)
```

### Pattern: Feature Store Integration

```python
# From Module 05: Assets
@task(outlets=[Asset("features/user_vectors")])
def update_user_vectors():
    """Update feature store, triggering downstream consumers."""
    vectors = compute_vectors()
    write_to_feature_store(vectors)
    return {"vectors_updated": len(vectors)}
```

## Related Exercises

| Exercise                                                                                                          | Concepts Applied                        |
| ----------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| [Exercise 6.4: Parallel Embeddings](../../modules/06-dynamic-tasks/exercises/exercise_6_4_parallel_embeddings.md) | Dynamic mapping for parallel processing |
| [Exercise 5.4: Embedding Assets](../../modules/05-assets-data-aware/exercises/exercise_5_4_embedding_assets.md)   | Asset-driven feature pipelines          |
| [Exercise 15.1: RAG Pipeline](../../modules/15-ai-ml-orchestration/exercises/exercise_15_1_rag_pipeline.md)       | ML pipeline orchestration               |

## Further Reading

- [Spotify Engineering: How Spotify's Algorithm Works](https://engineering.atspotify.com/)
- [Luigi to Airflow Migration](https://www.slideshare.net/SpotifyEng)
- [Personalization at Spotify Using Cassandra](https://engineering.atspotify.com/)

---

[← Back to Case Studies](README.md) | [Stripe Fraud Detection →](stripe-fraud-detection.md)
