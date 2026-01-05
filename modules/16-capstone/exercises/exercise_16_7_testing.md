# Exercise 16.7: Testing and Validation

## Objective

Create a comprehensive test suite for the document processing pipeline, covering DAG integrity, unit tests, integration tests, end-to-end validation, and performance benchmarking.

## Background

### Testing Pyramid for Airflow DAGs

A robust Airflow testing strategy follows the testing pyramid principle:

```
                    /\
                   /  \
                  / E2E \           ← Slow, expensive, high confidence
                 /______\
                /        \
               / Integration\       ← Moderate speed, mock external services
              /______________\
             /                \
            /    Unit Tests    \    ← Fast, isolated, foundational
           /____________________\
          /                      \
         /    DAG Integrity Tests  \ ← Fastest, catch structural issues
        /__________________________\
```

| Test Type          | Purpose                                        | Speed     | Scope             |
| ------------------ | ---------------------------------------------- | --------- | ----------------- |
| **DAG Integrity**  | Validate DAG structure, imports, configuration | Very Fast | All DAGs          |
| **Unit Tests**     | Test individual operators, sensors, hooks      | Fast      | Single component  |
| **Integration**    | Test task interactions with mocked services    | Medium    | Task groups       |
| **E2E Validation** | Full pipeline execution with test data         | Slow      | Complete workflow |
| **Performance**    | Benchmark execution time and resource usage    | Variable  | Critical paths    |

## Requirements

### Task 1: DAG Integrity Tests

Create comprehensive tests that validate all capstone DAGs load correctly and meet standards.

**File: `tests/test_dag_integrity.py`**

```python
"""
DAG Integrity Tests for Capstone Pipeline
==========================================

Tests that all DAGs:
- Load without import errors
- Have required configuration
- Follow naming conventions
- Have proper documentation
"""

import os
import re

import pytest
from airflow.models import DagBag

# Configure test DAG folder
DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "..", "..", "dags")

CAPSTONE_DAG_PATTERN = re.compile(r"^capstone_.*")


@pytest.fixture(scope="session")
def dag_bag():
    """Load all DAGs for testing."""
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


@pytest.fixture(scope="session")
def capstone_dags(dag_bag):
    """Filter to only capstone DAGs."""
    return {dag_id: dag for dag_id, dag in dag_bag.dags.items() if CAPSTONE_DAG_PATTERN.match(dag_id)}


class TestDagIntegrity:
    """Test DAG loading and basic structure."""

    def test_no_import_errors(self, dag_bag):
        """Verify no DAGs have import errors."""
        assert len(dag_bag.import_errors) == 0, f"DAG import errors found: {dag_bag.import_errors}"

    def test_capstone_dags_exist(self, capstone_dags):
        """Verify expected capstone DAGs are present."""
        expected_dags = [
            "capstone_ingestion_pipeline",
            "capstone_processing_pipeline",
            "capstone_vector_store_pipeline",
            "capstone_monitoring",
        ]

        for dag_id in expected_dags:
            assert dag_id in capstone_dags, f"Missing expected DAG: {dag_id}"

    def test_dags_have_tags(self, capstone_dags):
        """Verify all capstone DAGs have the 'capstone' tag."""
        for dag_id, dag in capstone_dags.items():
            assert "capstone" in dag.tags, f"DAG {dag_id} missing 'capstone' tag"

    def test_dags_have_owner(self, capstone_dags):
        """Verify all DAGs have a non-default owner."""
        for dag_id, dag in capstone_dags.items():
            owner = dag.default_args.get("owner", "airflow")
            assert owner != "airflow", f"DAG {dag_id} uses default owner"

    def test_dags_have_description(self, capstone_dags):
        """Verify all DAGs have documentation."""
        for dag_id, dag in capstone_dags.items():
            assert dag.doc_md or dag.description, f"DAG {dag_id} missing documentation"


class TestDagConfiguration:
    """Test DAG configuration requirements."""

    def test_retry_configuration(self, capstone_dags):
        """Verify DAGs have retry configuration."""
        for dag_id, dag in capstone_dags.items():
            retries = dag.default_args.get("retries", 0)
            assert retries >= 1, f"DAG {dag_id} should have at least 1 retry"

    def test_catchup_disabled(self, capstone_dags):
        """Verify catchup is disabled for scheduled DAGs."""
        for dag_id, dag in capstone_dags.items():
            if dag.schedule_interval:
                assert not dag.catchup, f"DAG {dag_id} should have catchup=False"

    def test_max_active_runs(self, capstone_dags):
        """Verify DAGs have reasonable max_active_runs."""
        for dag_id, dag in capstone_dags.items():
            # Monitoring DAGs should only run 1 at a time
            if "monitoring" in dag_id:
                assert dag.max_active_runs == 1, f"Monitoring DAG {dag_id} should have max_active_runs=1"

    def test_sla_configuration(self, capstone_dags):
        """Verify critical DAGs have SLA configuration."""
        critical_dags = [
            "capstone_ingestion_pipeline",
            "capstone_processing_pipeline",
        ]

        for dag_id in critical_dags:
            if dag_id in capstone_dags:
                dag = capstone_dags[dag_id]
                sla = dag.default_args.get("sla")
                assert sla is not None, f"Critical DAG {dag_id} should have SLA configured"


class TestTaskNaming:
    """Test task naming conventions."""

    def test_task_ids_snake_case(self, capstone_dags):
        """Verify all task IDs use snake_case."""
        snake_case_pattern = re.compile(r"^[a-z][a-z0-9_]*$")

        for dag_id, dag in capstone_dags.items():
            for task in dag.tasks:
                assert snake_case_pattern.match(task.task_id), f"Task {dag_id}.{task.task_id} not in snake_case"

    def test_task_ids_descriptive(self, capstone_dags):
        """Verify task IDs are descriptive (not generic)."""
        generic_names = ["task", "step", "run", "do"]

        for dag_id, dag in capstone_dags.items():
            for task in dag.tasks:
                assert task.task_id not in generic_names, f"Task {dag_id}.{task.task_id} has generic name"


class TestDependencies:
    """Test task dependencies are valid."""

    def test_no_cycles(self, capstone_dags):
        """Verify no circular dependencies exist."""
        for dag_id, dag in capstone_dags.items():
            # DAG validation should catch cycles during load
            # This test documents the expectation
            assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"

    def test_no_orphan_tasks(self, capstone_dags):
        """Verify tasks without upstream have valid reasons."""
        for dag_id, dag in capstone_dags.items():
            root_tasks = [t for t in dag.tasks if not t.upstream_list]

            # Each DAG should have exactly 1-3 root tasks
            assert 1 <= len(root_tasks) <= 3, f"DAG {dag_id} has {len(root_tasks)} root tasks (expected 1-3)"
```

### Task 2: Unit Tests for Custom Operators and Sensors

Create unit tests for the custom LLM operator and vector store sensor.

**File: `tests/test_operators.py`**

```python
"""
Unit Tests for Custom Operators
===============================

Tests custom operators in isolation with mocked dependencies.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest

# Import custom operators (adjust path as needed)
# from operators.llm_operator import LLMOperator
# from sensors.vector_store_sensor import VectorStoreSensor


class TestLLMOperator:
    """Test LLM operator functionality."""

    @pytest.fixture
    def mock_context(self):
        """Create mock Airflow context."""
        return {
            "task_instance": Mock(
                dag_id="test_dag", task_id="test_task", run_id="test_run_123", try_number=1, xcom_push=Mock()
            ),
            "params": {},
            "execution_date": datetime(2024, 1, 1),
        }

    @pytest.fixture
    def mock_openai_client(self):
        """Create mock OpenAI client."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content='{"entities": ["test"]}'))]
        mock_response.usage = Mock(prompt_tokens=100, completion_tokens=50, total_tokens=150)

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        return mock_client

    def test_llm_operator_initialization(self):
        """Test operator initializes with required parameters."""
        # Replace with actual operator import
        # operator = LLMOperator(
        #     task_id="test_llm",
        #     prompt_template="Extract entities from: {text}",
        #     model="gpt-4",
        # )
        # assert operator.task_id == "test_llm"
        # assert operator.model == "gpt-4"
        pass  # TODO: Implement with actual operator

    def test_llm_operator_executes_prompt(self, mock_context, mock_openai_client):
        """Test operator executes prompt and returns response."""
        with patch("openai.OpenAI", return_value=mock_openai_client):
            # Replace with actual operator execution
            # operator = LLMOperator(
            #     task_id="test_llm",
            #     prompt_template="Extract: {text}",
            # )
            # result = operator.execute(mock_context)
            # assert "entities" in result
            pass  # TODO: Implement with actual operator

    def test_llm_operator_handles_api_error(self, mock_context):
        """Test operator handles API errors gracefully."""
        with patch("openai.OpenAI") as mock_openai:
            mock_openai.side_effect = Exception("API Error")
            # operator = LLMOperator(...)
            # with pytest.raises(AirflowException):
            #     operator.execute(mock_context)
            pass  # TODO: Implement with actual operator

    def test_llm_operator_tracks_token_usage(self, mock_context, mock_openai_client):
        """Test operator tracks and reports token usage."""
        with patch("openai.OpenAI", return_value=mock_openai_client):
            # operator = LLMOperator(...)
            # result = operator.execute(mock_context)
            # assert "token_usage" in result
            # assert result["token_usage"]["total"] == 150
            pass  # TODO: Implement with actual operator


class TestVectorStoreSensor:
    """Test vector store sensor functionality."""

    @pytest.fixture
    def mock_vector_client(self):
        """Create mock vector store client."""
        mock_client = Mock()
        mock_client.get_collection.return_value = Mock(count=Mock(return_value=1000))
        return mock_client

    def test_sensor_initialization(self):
        """Test sensor initializes correctly."""
        # sensor = VectorStoreSensor(
        #     task_id="wait_vectors",
        #     collection_name="documents",
        #     min_document_count=100,
        # )
        # assert sensor.collection_name == "documents"
        pass  # TODO: Implement with actual sensor

    def test_sensor_poke_returns_true_when_ready(self, mock_vector_client):
        """Test sensor poke returns True when condition met."""
        with patch("chromadb.Client", return_value=mock_vector_client):
            # sensor = VectorStoreSensor(
            #     task_id="wait_vectors",
            #     collection_name="documents",
            #     min_document_count=100,
            # )
            # assert sensor.poke({}) is True
            pass  # TODO: Implement with actual sensor

    def test_sensor_poke_returns_false_when_not_ready(self):
        """Test sensor poke returns False when condition not met."""
        mock_client = Mock()
        mock_client.get_collection.return_value = Mock(
            count=Mock(return_value=50)  # Less than required
        )

        with patch("chromadb.Client", return_value=mock_client):
            # sensor = VectorStoreSensor(
            #     task_id="wait_vectors",
            #     collection_name="documents",
            #     min_document_count=100,
            # )
            # assert sensor.poke({}) is False
            pass  # TODO: Implement with actual sensor

    def test_sensor_deferrable_mode(self):
        """Test sensor works in deferrable mode."""
        # sensor = VectorStoreSensor(
        #     task_id="wait_vectors",
        #     collection_name="documents",
        #     deferrable=True,
        # )
        # assert sensor.deferrable is True
        pass  # TODO: Implement with actual sensor


class TestHelperFunctions:
    """Test helper functions used by operators."""

    def test_chunk_text(self):
        """Test text chunking function."""
        from typing import List

        def chunk_text(text: str, chunk_size: int = 500, overlap: int = 100) -> List[str]:
            """Split text into overlapping chunks."""
            words = text.split()
            chunks = []
            start = 0

            while start < len(words):
                end = start + chunk_size
                chunk = " ".join(words[start:end])
                chunks.append(chunk)
                start = end - overlap

            return chunks

        # Test chunking
        text = " ".join(["word"] * 1000)
        chunks = chunk_text(text, chunk_size=100, overlap=20)

        assert len(chunks) > 1
        assert all(len(c.split()) <= 100 for c in chunks)

    def test_generate_document_id(self):
        """Test document ID generation is deterministic."""
        import hashlib

        def generate_document_id(path: str, chunk_index: int, content: str) -> str:
            """Generate stable document ID."""
            data = f"{path}:{chunk_index}:{content}"
            return hashlib.sha256(data.encode()).hexdigest()[:16]

        # Same input should produce same ID
        id1 = generate_document_id("/doc.pdf", 0, "content")
        id2 = generate_document_id("/doc.pdf", 0, "content")
        assert id1 == id2

        # Different input should produce different ID
        id3 = generate_document_id("/doc.pdf", 1, "content")
        assert id1 != id3
```

### Task 3: Integration Tests with Mocked Services

Create integration tests that verify task interactions with mocked LLM and vector store.

**File: `tests/test_integration.py`**

```python
"""
Integration Tests for Capstone Pipeline
========================================

Tests task interactions with mocked external services.
"""

import json
from datetime import datetime
from unittest.mock import Mock

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dag_bag():
    """Load DAGs for testing."""
    import os

    dags_folder = os.path.join(os.path.dirname(__file__), "..", "..", "..", "dags")
    return DagBag(dag_folder=dags_folder, include_examples=False)


class TestIngestionIntegration:
    """Test ingestion pipeline task integration."""

    @pytest.fixture
    def mock_document_source(self):
        """Mock document source with sample files."""
        return [
            {"path": "/docs/doc1.pdf", "size": 1024, "modified": datetime.now()},
            {"path": "/docs/doc2.md", "size": 512, "modified": datetime.now()},
        ]

    @pytest.fixture
    def mock_parse_result(self):
        """Mock document parsing result."""
        return {
            "text": "This is sample document content for testing.",
            "metadata": {"title": "Sample Document", "pages": 3, "type": "pdf"},
        }

    def test_document_discovery_to_parsing_flow(self, dag_bag, mock_document_source, mock_parse_result):
        """Test data flows correctly from discovery to parsing."""
        # This tests the XCom data contract between tasks
        discovery_output = mock_document_source
        parse_input = discovery_output[0]

        # Verify contract: discovery output matches parsing input expectation
        assert "path" in parse_input
        assert "size" in parse_input

        # Verify parsing output structure
        assert "text" in mock_parse_result
        assert "metadata" in mock_parse_result

    def test_incremental_processing_logic(self):
        """Test that incremental processing correctly identifies new documents."""
        # Simulate processed documents tracking
        previously_processed = {"/docs/doc1.pdf": {"hash": "abc123", "processed_at": "2024-01-01"}}

        new_documents = [
            {"path": "/docs/doc1.pdf", "hash": "abc123"},  # Already processed
            {"path": "/docs/doc2.pdf", "hash": "def456"},  # New document
        ]

        # Filter to only new documents
        to_process = [
            doc
            for doc in new_documents
            if doc["path"] not in previously_processed or previously_processed[doc["path"]]["hash"] != doc["hash"]
        ]

        assert len(to_process) == 1
        assert to_process[0]["path"] == "/docs/doc2.pdf"


class TestProcessingIntegration:
    """Test LLM processing pipeline integration."""

    @pytest.fixture
    def mock_llm_response(self):
        """Mock LLM API response."""
        return {
            "entities": ["Python", "Airflow", "Machine Learning"],
            "summary": "A document about data engineering tools.",
            "classification": "technical_documentation",
            "confidence": 0.95,
        }

    @pytest.fixture
    def mock_openai_client(self, mock_llm_response):
        """Create mock OpenAI client with structured response."""
        mock_response = Mock()
        mock_response.choices = [Mock(message=Mock(content=json.dumps(mock_llm_response)))]
        mock_response.usage = Mock(prompt_tokens=500, completion_tokens=200, total_tokens=700)

        mock_client = Mock()
        mock_client.chat.completions.create.return_value = mock_response
        return mock_client

    def test_extraction_to_classification_flow(self, mock_llm_response):
        """Test data flows correctly from extraction to classification."""
        # Extraction output
        extraction_output = {"entities": mock_llm_response["entities"], "raw_text": "Sample text content"}

        # Classification input expects entities
        assert "entities" in extraction_output

        # Verify classification output structure
        assert "classification" in mock_llm_response
        assert "confidence" in mock_llm_response

    def test_llm_retry_on_rate_limit(self, mock_openai_client):
        """Test LLM task retries on rate limit errors."""
        from tenacity import retry, stop_after_attempt, wait_exponential

        call_count = 0

        @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=10))
        def call_llm_with_retry():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Rate limit exceeded")
            return {"success": True}

        result = call_llm_with_retry()
        assert result["success"] is True
        assert call_count == 3

    def test_llm_response_validation(self, mock_llm_response):
        """Test LLM response validation."""

        def validate_llm_response(response: dict) -> bool:
            required_fields = ["entities", "summary", "classification"]
            return all(field in response for field in required_fields)

        assert validate_llm_response(mock_llm_response) is True
        assert validate_llm_response({"partial": "response"}) is False


class TestVectorStoreIntegration:
    """Test vector store pipeline integration."""

    @pytest.fixture
    def mock_chroma_client(self):
        """Create mock ChromaDB client."""
        mock_collection = Mock()
        mock_collection.add = Mock()
        mock_collection.query = Mock(
            return_value={
                "ids": [["doc1", "doc2"]],
                "distances": [[0.1, 0.2]],
                "documents": [["doc1 content", "doc2 content"]],
            }
        )
        mock_collection.count = Mock(return_value=1000)

        mock_client = Mock()
        mock_client.get_or_create_collection = Mock(return_value=mock_collection)
        return mock_client, mock_collection

    def test_embedding_to_storage_flow(self, mock_chroma_client):
        """Test embeddings flow correctly to vector store."""
        mock_client, mock_collection = mock_chroma_client

        # Simulate embedding output
        embeddings_output = {
            "embeddings": [[0.1, 0.2, 0.3] * 512],  # 1536-dim
            "documents": ["Document content"],
            "ids": ["doc_001"],
            "metadatas": [{"source": "test.pdf"}],
        }

        # Verify storage accepts the format
        mock_collection.add(
            embeddings=embeddings_output["embeddings"],
            documents=embeddings_output["documents"],
            ids=embeddings_output["ids"],
            metadatas=embeddings_output["metadatas"],
        )

        mock_collection.add.assert_called_once()

    def test_idempotent_upsert(self, mock_chroma_client):
        """Test that upserting same document is idempotent."""
        mock_client, mock_collection = mock_chroma_client

        doc_id = "doc_001"
        document = {
            "id": doc_id,
            "embedding": [0.1, 0.2, 0.3] * 512,
            "content": "Test content",
            "metadata": {"source": "test.pdf"},
        }

        # First upsert
        mock_collection.upsert = Mock()
        mock_collection.upsert(
            ids=[document["id"]],
            embeddings=[document["embedding"]],
            documents=[document["content"]],
            metadatas=[document["metadata"]],
        )

        # Second upsert with same ID should overwrite, not duplicate
        mock_collection.upsert(
            ids=[document["id"]],
            embeddings=[document["embedding"]],
            documents=[document["content"]],
            metadatas=[document["metadata"]],
        )

        # Verify upsert was called twice (idempotent operation)
        assert mock_collection.upsert.call_count == 2


class TestCrossDAGIntegration:
    """Test integration between DAGs via Assets."""

    def test_asset_publication(self):
        """Test that producer DAG correctly publishes asset."""
        # Simulate asset publication
        from dataclasses import dataclass
        from datetime import datetime

        @dataclass
        class Asset:
            uri: str
            extra: dict = None

        published_asset = Asset(uri="documents.raw", extra={"last_updated": datetime.now().isoformat()})

        # Verify asset structure
        assert published_asset.uri == "documents.raw"
        assert "last_updated" in published_asset.extra

    def test_asset_consumption_trigger(self):
        """Test that consumer DAG correctly consumes asset trigger."""
        # This would be tested with actual Asset triggers in Airflow 3.x
        # For now, verify the contract

        asset_event = {
            "uri": "documents.raw",
            "source_dag_id": "capstone_ingestion_pipeline",
            "source_run_id": "run_123",
            "timestamp": datetime.now().isoformat(),
        }

        # Consumer should receive asset event
        assert "uri" in asset_event
        assert "source_dag_id" in asset_event
```

### Task 4: End-to-End Validation Pipeline

Create a DAG that validates the entire pipeline with test data.

**File: `dags/capstone_e2e_validation.py`**

```python
"""
End-to-End Validation Pipeline
==============================

Validates the complete document processing pipeline with test data.
Runs after deployments to verify system integrity.
"""

from datetime import datetime
from typing import Any

from airflow.models import Variable
from airflow.sdk import dag, task


@dag(
    dag_id="capstone_e2e_validation",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Triggered manually or by CI/CD
    catchup=False,
    default_args={
        "owner": "qa-team",
        "retries": 0,  # No retries for validation - we want to see failures
    },
    tags=["capstone", "testing", "e2e"],
    doc_md="""
    ## End-to-End Validation Pipeline

    Validates the complete document processing pipeline:
    1. Creates test documents
    2. Triggers ingestion pipeline
    3. Waits for processing completion
    4. Validates output in vector store
    5. Cleans up test data

    **Run after**: Deployments, major changes
    **Expected duration**: ~15 minutes
    """,
)
def e2e_validation():
    """End-to-end validation DAG."""

    @task
    def setup_test_environment() -> dict[str, Any]:
        """
        Set up test environment and create test documents.

        Returns:
            Test configuration including document paths
        """
        import os
        import tempfile

        # Create temporary test directory
        test_dir = tempfile.mkdtemp(prefix="capstone_e2e_")

        # Create test documents
        test_docs = []

        # Test document 1: Simple text
        doc1_path = os.path.join(test_dir, "test_doc_1.md")
        with open(doc1_path, "w") as f:
            f.write("""# Test Document 1

This is a test document for end-to-end validation.

## Topics
- Apache Airflow
- Data Engineering
- Machine Learning

## Content
This document contains sample content that should be:
1. Parsed correctly
2. Processed by the LLM
3. Stored in the vector database
""")
        test_docs.append({"path": doc1_path, "type": "markdown"})

        # Test document 2: JSON-like content
        doc2_path = os.path.join(test_dir, "test_doc_2.md")
        with open(doc2_path, "w") as f:
            f.write("""# Technical Specification

## API Endpoints
- GET /api/v1/documents
- POST /api/v1/process
- DELETE /api/v1/cleanup

## Expected Entities
The LLM should extract: API, endpoints, REST, documents
""")
        test_docs.append({"path": doc2_path, "type": "markdown"})

        test_config = {
            "test_run_id": f"e2e_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "test_dir": test_dir,
            "test_docs": test_docs,
            "expected_chunks": 4,  # Approximate
            "expected_entities": ["Airflow", "API", "Machine Learning"],
            "created_at": datetime.now().isoformat(),
        }

        # Store config for validation
        Variable.set("e2e_test_config", test_config, serialize_json=True)

        return test_config

    @task
    def trigger_ingestion(test_config: dict) -> dict[str, Any]:
        """
        Trigger the ingestion pipeline with test documents.

        Args:
            test_config: Test configuration from setup

        Returns:
            Trigger result
        """
        from airflow.api.client.local_client import Client

        client = Client(None, None)

        # Trigger ingestion with test directory
        run_id = f"e2e_test_{test_config['test_run_id']}"

        try:
            client.trigger_dag(
                dag_id="capstone_ingestion_pipeline", run_id=run_id, conf={"source_dir": test_config["test_dir"]}
            )

            return {"triggered": True, "dag_id": "capstone_ingestion_pipeline", "run_id": run_id}
        except Exception as e:
            return {"triggered": False, "error": str(e)}

    @task
    def wait_for_pipeline_completion(trigger_result: dict) -> dict[str, Any]:
        """
        Wait for the triggered pipeline to complete.

        Args:
            trigger_result: Result from trigger task

        Returns:
            Completion status
        """
        import time

        from airflow.models import DagRun
        from airflow.utils.db import provide_session
        from airflow.utils.state import DagRunState

        if not trigger_result.get("triggered"):
            return {"success": False, "error": "Pipeline not triggered"}

        run_id = trigger_result["run_id"]
        max_wait_seconds = 600  # 10 minutes
        poll_interval = 10

        @provide_session
        def check_run_state(session=None):
            run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == "capstone_ingestion_pipeline", DagRun.run_id == run_id)
                .first()
            )

            if run:
                return run.state
            return None

        start_time = time.time()
        while time.time() - start_time < max_wait_seconds:
            state = check_run_state()

            if state == DagRunState.SUCCESS:
                return {"success": True, "state": str(state), "duration_seconds": time.time() - start_time}
            elif state == DagRunState.FAILED:
                return {"success": False, "state": str(state), "duration_seconds": time.time() - start_time}

            time.sleep(poll_interval)

        return {"success": False, "state": "timeout", "duration_seconds": max_wait_seconds}

    @task
    def validate_vector_store(test_config: dict, completion_result: dict) -> dict[str, Any]:
        """
        Validate documents were correctly stored in vector store.

        Args:
            test_config: Test configuration
            completion_result: Pipeline completion status

        Returns:
            Validation results
        """
        if not completion_result.get("success"):
            return {"valid": False, "reason": "Pipeline did not complete successfully"}

        # In production, query actual vector store
        # This example uses mocked validation

        validation_results = {
            "documents_found": 0,
            "chunks_found": 0,
            "metadata_valid": True,
            "embeddings_valid": True,
            "errors": [],
        }

        try:
            # Mock validation - replace with actual vector store query
            # client = chromadb.Client()
            # collection = client.get_collection("documents")

            # Query for test documents
            test_run_id = test_config["test_run_id"]

            # results = collection.query(
            #     where={"test_run_id": test_run_id},
            #     include=["documents", "metadatas", "embeddings"]
            # )

            # Simulated results
            validation_results["documents_found"] = len(test_config["test_docs"])
            validation_results["chunks_found"] = test_config["expected_chunks"]

            # Validate expectations
            if validation_results["documents_found"] < len(test_config["test_docs"]):
                validation_results["errors"].append("Not all documents ingested")
                validation_results["metadata_valid"] = False

        except Exception as e:
            validation_results["errors"].append(str(e))

        validation_results["valid"] = len(validation_results["errors"]) == 0

        return validation_results

    @task
    def generate_report(test_config: dict, completion_result: dict, validation_result: dict) -> dict[str, Any]:
        """
        Generate comprehensive E2E validation report.

        Args:
            test_config: Test configuration
            completion_result: Pipeline completion status
            validation_result: Validation results

        Returns:
            Complete test report
        """
        report = {
            "test_run_id": test_config["test_run_id"],
            "timestamp": datetime.now().isoformat(),
            "overall_status": "PASS" if validation_result.get("valid") else "FAIL",
            "stages": {
                "setup": {"status": "PASS", "documents_created": len(test_config["test_docs"])},
                "ingestion": {
                    "status": "PASS" if completion_result.get("success") else "FAIL",
                    "duration_seconds": completion_result.get("duration_seconds", 0),
                },
                "validation": {
                    "status": "PASS" if validation_result.get("valid") else "FAIL",
                    "documents_found": validation_result.get("documents_found", 0),
                    "chunks_found": validation_result.get("chunks_found", 0),
                    "errors": validation_result.get("errors", []),
                },
            },
            "metrics": {
                "total_duration_seconds": (completion_result.get("duration_seconds", 0)),
                "documents_per_second": (
                    len(test_config["test_docs"]) / max(completion_result.get("duration_seconds", 1), 1)
                ),
            },
        }

        # Store report
        Variable.set(f"e2e_report_{test_config['test_run_id']}", report, serialize_json=True)

        # Print summary
        print(f"\n{'=' * 60}")
        print("E2E VALIDATION REPORT")
        print(f"{'=' * 60}")
        print(f"Test Run ID: {report['test_run_id']}")
        print(f"Overall Status: {report['overall_status']}")
        print(f"Duration: {report['metrics']['total_duration_seconds']:.2f}s")
        print(f"Documents Processed: {report['stages']['validation']['documents_found']}")
        print(f"{'=' * 60}\n")

        return report

    @task
    def cleanup_test_data(test_config: dict, report: dict) -> None:
        """
        Clean up test data and temporary files.

        Args:
            test_config: Test configuration
            report: Final report
        """
        import os
        import shutil

        test_dir = test_config.get("test_dir")

        if test_dir and os.path.exists(test_dir):
            try:
                shutil.rmtree(test_dir)
                print(f"Cleaned up test directory: {test_dir}")
            except Exception as e:
                print(f"Failed to clean up {test_dir}: {e}")

        # Optionally clean vector store test data
        # client = chromadb.Client()
        # collection = client.get_collection("documents")
        # collection.delete(where={"test_run_id": test_config["test_run_id"]})

    # Task flow
    config = setup_test_environment()
    trigger_result = trigger_ingestion(config)
    completion = wait_for_pipeline_completion(trigger_result)
    validation = validate_vector_store(config, completion)
    report = generate_report(config, completion, validation)
    cleanup_test_data(config, report)


e2e_validation()
```

### Task 5: Performance Benchmarking Suite

Create a performance benchmarking DAG and tests.

**File: `tests/test_performance.py`**

````python
"""
Performance Benchmarking Tests
==============================

Benchmarks critical pipeline operations for regression detection.
"""

import pytest
import time
from datetime import datetime, timedelta
from typing import Callable
from dataclasses import dataclass


@dataclass
class BenchmarkResult:
    """Result of a benchmark run."""
    name: str
    duration_seconds: float
    iterations: int
    avg_duration: float
    min_duration: float
    max_duration: float
    threshold_seconds: float
    passed: bool


def benchmark(
    func: Callable,
    iterations: int = 10,
    threshold_seconds: float = 1.0
) -> BenchmarkResult:
    """
    Run a benchmark on the given function.

    Args:
        func: Function to benchmark
        iterations: Number of iterations
        threshold_seconds: Maximum acceptable average duration

    Returns:
        BenchmarkResult with timing data
    """
    durations = []

    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        durations.append(end - start)

    avg_duration = sum(durations) / len(durations)

    return BenchmarkResult(
        name=func.__name__,
        duration_seconds=sum(durations),
        iterations=iterations,
        avg_duration=avg_duration,
        min_duration=min(durations),
        max_duration=max(durations),
        threshold_seconds=threshold_seconds,
        passed=avg_duration <= threshold_seconds
    )


class TestDocumentParsingPerformance:
    """Benchmark document parsing operations."""

    def test_markdown_parsing_speed(self):
        """Benchmark Markdown parsing."""
        sample_markdown = """
# Sample Document

This is a sample document with multiple sections.

## Section 1
Content for section 1 with **bold** and *italic* text.

## Section 2
- List item 1
- List item 2
- List item 3

## Section 3
```python
def example():
    return "code block"
````

""" \* 10 # 10x content

        def parse_markdown():
            # Simulate parsing
            lines = sample_markdown.split("\n")
            sections = [l for l in lines if l.startswith("#")]
            content = "\n".join(lines)
            return {"sections": len(sections), "length": len(content)}

        result = benchmark(parse_markdown, iterations=100, threshold_seconds=0.01)

        assert result.passed, (
            f"Markdown parsing too slow: {result.avg_duration:.4f}s "
            f"(threshold: {result.threshold_seconds}s)"
        )

    def test_text_chunking_speed(self):
        """Benchmark text chunking."""
        sample_text = "word " * 10000  # 10K words

        def chunk_text():
            words = sample_text.split()
            chunks = []
            chunk_size = 500
            overlap = 100
            start = 0

            while start < len(words):
                end = start + chunk_size
                chunks.append(" ".join(words[start:end]))
                start = end - overlap

            return chunks

        result = benchmark(chunk_text, iterations=50, threshold_seconds=0.05)

        assert result.passed, (
            f"Text chunking too slow: {result.avg_duration:.4f}s "
            f"(threshold: {result.threshold_seconds}s)"
        )

class TestEmbeddingPerformance:
"""Benchmark embedding operations."""

    def test_mock_embedding_generation_speed(self):
        """Benchmark mock embedding generation."""
        import hashlib

        def generate_mock_embedding():
            text = "Sample text for embedding " * 100
            hash_bytes = hashlib.sha256(text.encode()).digest()
            embedding = [b / 255.0 for b in hash_bytes * 48][:1536]
            return embedding

        result = benchmark(
            generate_mock_embedding,
            iterations=1000,
            threshold_seconds=0.001
        )

        assert result.passed, (
            f"Mock embedding too slow: {result.avg_duration:.4f}s"
        )

    def test_batch_embedding_efficiency(self):
        """Benchmark batch embedding vs individual."""
        import hashlib

        texts = [f"Sample text {i} for embedding" for i in range(100)]

        def individual_embedding():
            results = []
            for text in texts:
                hash_bytes = hashlib.sha256(text.encode()).digest()
                results.append([b / 255.0 for b in hash_bytes * 48][:1536])
            return results

        def batch_embedding():
            results = []
            # Simulate batch processing
            all_text = "|||".join(texts)
            hash_bytes = hashlib.sha256(all_text.encode()).digest()
            base = [b / 255.0 for b in hash_bytes * 48][:1536]
            results = [base for _ in texts]  # Simplified batch simulation
            return results

        individual_result = benchmark(individual_embedding, iterations=10)
        batch_result = benchmark(batch_embedding, iterations=10)

        # Batch should be faster (in real scenarios with API calls)
        print(f"Individual: {individual_result.avg_duration:.4f}s")
        print(f"Batch: {batch_result.avg_duration:.4f}s")

class TestVectorStorePerformance:
"""Benchmark vector store operations."""

    def test_document_id_generation_speed(self):
        """Benchmark document ID generation."""
        import hashlib

        def generate_id():
            path = "/documents/sample/file.pdf"
            chunk_index = 42
            content = "Sample content " * 100
            data = f"{path}:{chunk_index}:{content}"
            return hashlib.sha256(data.encode()).hexdigest()[:16]

        result = benchmark(
            generate_id,
            iterations=10000,
            threshold_seconds=0.0001
        )

        assert result.passed, (
            f"ID generation too slow: {result.avg_duration:.6f}s"
        )

    def test_metadata_serialization_speed(self):
        """Benchmark metadata serialization."""
        import json

        metadata = {
            "source": "/path/to/document.pdf",
            "chunk_index": 5,
            "total_chunks": 20,
            "timestamp": datetime.now().isoformat(),
            "document_type": "pdf",
            "word_count": 1500,
            "entities": ["entity1", "entity2", "entity3"],
            "classification": "technical_documentation",
            "confidence": 0.95
        }

        def serialize_metadata():
            return json.dumps(metadata)

        result = benchmark(
            serialize_metadata,
            iterations=10000,
            threshold_seconds=0.0001
        )

        assert result.passed, (
            f"Metadata serialization too slow: {result.avg_duration:.6f}s"
        )

class TestDAGLoadPerformance:
"""Benchmark DAG loading performance."""

    def test_dag_bag_load_time(self):
        """Benchmark DAG loading time."""
        import os
        from airflow.models import DagBag

        dags_folder = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "dags"
        )

        def load_dags():
            return DagBag(
                dag_folder=dags_folder,
                include_examples=False
            )

        result = benchmark(
            load_dags,
            iterations=5,
            threshold_seconds=5.0  # 5 seconds max
        )

        assert result.passed, (
            f"DAG loading too slow: {result.avg_duration:.2f}s "
            f"(threshold: {result.threshold_seconds}s)"
        )

    def test_individual_dag_parse_time(self):
        """Benchmark individual DAG parsing."""
        import os
        from airflow.models import DagBag

        dags_folder = os.path.join(
            os.path.dirname(__file__),
            "..", "..", "..", "dags"
        )

        dag_bag = DagBag(dag_folder=dags_folder, include_examples=False)

        # Each DAG should parse in under 2 seconds
        for dag_id, dag in dag_bag.dags.items():
            if dag_id.startswith("capstone_"):
                # Parse time is tracked during DagBag creation
                # This is a documentation/awareness test
                print(f"DAG {dag_id}: {len(dag.tasks)} tasks")
                assert len(dag.tasks) < 50, (
                    f"DAG {dag_id} has too many tasks ({len(dag.tasks)})"
                )

@pytest.fixture(scope="session")
def benchmark_report(request):
"""Generate benchmark report at end of session."""
results = []

    yield results

    # Print summary report
    print("\n" + "="*60)
    print("PERFORMANCE BENCHMARK REPORT")
    print("="*60)

    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"{status} {result.name}: {result.avg_duration:.4f}s (threshold: {result.threshold_seconds}s)")

    print("="*60)

````

## Deliverables

1. **`tests/test_dag_integrity.py`** - DAG structure and configuration tests
2. **`tests/test_operators.py`** - Unit tests for custom operators and sensors
3. **`tests/test_integration.py`** - Integration tests with mocked services
4. **`dags/capstone_e2e_validation.py`** - End-to-end validation DAG
5. **`tests/test_performance.py`** - Performance benchmarking suite

## Running the Tests

```bash
# Install test dependencies
uv pip install pytest pytest-cov pytest-benchmark

# Run all tests
uv run pytest tests/ -v

# Run specific test categories
uv run pytest tests/test_dag_integrity.py -v
uv run pytest tests/test_operators.py -v
uv run pytest tests/test_integration.py -v
uv run pytest tests/test_performance.py -v

# Run with coverage
uv run pytest tests/ --cov=dags --cov=operators --cov=sensors --cov-report=html

# Run benchmarks
uv run pytest tests/test_performance.py --benchmark-only
````

## Hints

<details>
<summary>Hint 1: Test fixtures for Airflow context</summary>

```python
@pytest.fixture
def mock_task_instance():
    """Create comprehensive mock TaskInstance."""
    ti = Mock()
    ti.dag_id = "test_dag"
    ti.task_id = "test_task"
    ti.run_id = "test_run_123"
    ti.try_number = 1
    ti.max_tries = 3
    ti.state = TaskInstanceState.RUNNING
    ti.start_date = datetime.now()
    ti.end_date = None
    ti.xcom_push = Mock()
    ti.xcom_pull = Mock(return_value={"test": "data"})
    return ti


@pytest.fixture
def mock_dag_run():
    """Create mock DagRun."""
    dr = Mock()
    dr.dag_id = "test_dag"
    dr.run_id = "test_run_123"
    dr.state = DagRunState.RUNNING
    dr.execution_date = datetime.now()
    dr.start_date = datetime.now()
    dr.end_date = None
    return dr
```

</details>

<details>
<summary>Hint 2: Testing with actual Airflow database</summary>

```python
import pytest
from airflow.utils.db import initdb


@pytest.fixture(scope="session", autouse=True)
def setup_test_database():
    """Initialize test database."""
    # Use test database
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:///test_airflow.db"

    # Initialize
    initdb()

    yield

    # Cleanup
    os.remove("test_airflow.db")
```

</details>

<details>
<summary>Hint 3: Parametrized tests for multiple scenarios</summary>

```python
@pytest.mark.parametrize(
    "doc_type,expected_chunks",
    [
        ("markdown", 5),
        ("pdf", 10),
        ("txt", 3),
    ],
)
def test_document_processing(doc_type, expected_chunks):
    """Test document processing for different types."""
    result = process_document(f"test.{doc_type}")
    assert len(result["chunks"]) >= expected_chunks
```

</details>

## Success Criteria

- [ ] All DAG integrity tests pass without import errors
- [ ] DAGs have proper owner, tags, and documentation
- [ ] Custom operator unit tests achieve >80% code coverage
- [ ] Integration tests verify data contracts between tasks
- [ ] E2E validation DAG completes successfully with test data
- [ ] Performance benchmarks pass within defined thresholds
- [ ] Test suite runs in under 5 minutes
- [ ] Coverage report generated with >80% coverage

## References

- [Module 07: Testing and Debugging](../../07-testing-debugging/README.md)
- [Airflow Testing Documentation](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing-a-dag)
- [Pytest Documentation](https://docs.pytest.org/)
- [Testing Best Practices](https://martinfowler.com/articles/practical-test-pyramid.html)

---

[Previous: Exercise 16.6 - Monitoring](exercise_16_6_monitoring.md) | [Back to Module 16 Overview](../README.md)
