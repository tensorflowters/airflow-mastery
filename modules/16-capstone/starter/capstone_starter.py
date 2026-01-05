"""
Capstone Project Starter Template
=================================

This starter template provides scaffolding for the Module 16 capstone project.
Fill in the TODO sections to build your production-grade document processing pipeline.

Use this as a starting point and refer to the exercises for detailed guidance.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset, Variable, dag, task

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# TODO: Configure your paths and assets
# =============================================================================

BASE_PATH = Variable.get("capstone_base_path", default_var="/tmp/capstone")

# Define your assets for data-aware scheduling
# TODO: Define assets for raw documents, enriched documents, and vector store
RAW_DOCUMENTS = Asset("documents.raw")
# ENRICHED_DOCUMENTS = Asset(...)
# VECTOR_STORE = Asset(...)


# =============================================================================
# INGESTION PIPELINE
# Modules Applied: 03, 05, 06, 11
# =============================================================================


@dag(
    dag_id="capstone_ingestion_starter",
    description="Document ingestion pipeline - STARTER",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["capstone", "starter", "ingestion"],
    default_args={
        "owner": "capstone",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
)
def ingestion_pipeline():
    """Document ingestion pipeline starter."""
    # TODO: Implement file sensor for incoming documents
    # Hint: Use FileSensor with mode='reschedule' for efficiency

    @task
    def discover_files() -> list[dict]:
        """TODO: Implement file discovery logic."""
        # Hint: Use Path.glob() to find incoming files
        # Return list of file metadata dicts
        return []

    @task
    def process_document(file_info: dict) -> dict:
        """TODO: Process a single document."""
        # Hint: Extract metadata, calculate hash, prepare for storage
        return {}

    @task
    def validate_document(document: dict) -> dict:
        """TODO: Validate document meets quality requirements."""
        # Hint: Check required fields, file size limits
        return {}

    @task(outlets=[RAW_DOCUMENTS])
    def store_document(document: dict) -> dict:
        """TODO: Store validated document and emit asset."""
        # Hint: Move file to processed directory, write metadata JSON
        return {}

    # TODO: Build the task flow
    # files = discover_files()
    # processed = process_document.expand(file_info=files)
    # validated = validate_document.expand(document=processed)
    # store_document.expand(document=validated)


# =============================================================================
# PROCESSING PIPELINE
# Modules Applied: 02, 09, 14, 15
# =============================================================================


# @dag(
#     dag_id="capstone_processing_starter",
#     schedule=RAW_DOCUMENTS,  # Asset-triggered
#     ...
# )
# def processing_pipeline():
#     """LLM processing pipeline starter."""
#
#     # TODO: Implement LLM processing tasks
#     # - extract_entities: Pool-limited LLM call for entity extraction
#     # - summarize_document: Generate document summary
#     # - classify_document: Categorize document
#     # - evaluate_quality: Quality gate with branching
#
#     pass


# =============================================================================
# VECTOR STORE PIPELINE
# Modules Applied: 06, 11, 15
# =============================================================================


# @dag(
#     dag_id="capstone_vector_store_starter",
#     schedule=ENRICHED_DOCUMENTS,  # Asset-triggered
#     ...
# )
# def vector_store_pipeline():
#     """Vector store pipeline starter."""
#
#     # TODO: Implement embedding and indexing tasks
#     # - generate_embeddings: Parallel embedding generation
#     # - batch_upsert: Batch insert to vector store
#     # - wait_for_indexing: Deferrable sensor for index completion
#     # - validate_index: Verify index health
#
#     pass


# =============================================================================
# MONITORING PIPELINE
# Modules Applied: 09, 10, 12, 14
# =============================================================================


# @dag(
#     dag_id="capstone_monitoring_starter",
#     schedule="*/5 * * * *",
#     ...
# )
# def monitoring_pipeline():
#     """Platform monitoring pipeline starter."""
#
#     # TODO: Implement monitoring tasks
#     # - check_pipeline_health: Health checks for all pipelines
#     # - collect_metrics: Gather performance metrics
#     # - check_sla_compliance: Verify SLA targets
#     # - send_alerts: Alert on threshold breaches
#
#     pass


# =============================================================================
# HELPER UTILITIES
# =============================================================================


def create_test_environment():
    """Set up test directories and sample files."""
    dirs = ["incoming", "processed", "enriched", "review", "metrics"]
    for dir_name in dirs:
        Path(f"{BASE_PATH}/{dir_name}").mkdir(parents=True, exist_ok=True)
        print(f"Created: {BASE_PATH}/{dir_name}")


def create_sample_documents(count: int = 3):
    """Create sample PDF files for testing."""
    incoming_dir = Path(f"{BASE_PATH}/incoming")
    incoming_dir.mkdir(parents=True, exist_ok=True)

    for i in range(count):
        filepath = incoming_dir / f"sample_doc_{i + 1}.pdf"
        filepath.write_text(f"Sample document content {i + 1}")
        print(f"Created: {filepath}")


# Instantiate the starter DAG
ingestion_pipeline()


if __name__ == "__main__":
    print("Setting up capstone test environment...")
    create_test_environment()
    create_sample_documents()
    print("Done! Run airflow dags list to verify DAG is loaded.")
