"""
Solution 7.2: Unit Test a Pipeline
==================================

Complete unit test suite for an ETL pipeline.

Includes:
1. Happy path tests
2. Edge case tests
3. Mocked dependency tests
4. Parametrized tests
5. Integration-style tests
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime


# =========================================================================
# ETL FUNCTIONS TO TEST
# (In real scenario, import from your DAG module)
# =========================================================================


def extract_data(api_url: str = "https://api.example.com/data") -> dict:
    """Extract data from an API."""
    import requests

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    return {
        "data": response.json(),
        "extracted_at": datetime.now().isoformat(),
    }


def transform_data(raw_data: dict) -> dict:
    """Transform raw data into processed format."""
    records = raw_data.get("data", [])

    if not records:
        return {
            "records": [],
            "total_amount": 0,
            "record_count": 0,
            "avg_amount": 0,
            "transformed_at": datetime.now().isoformat(),
        }

    processed = []
    total = 0

    for record in records:
        amount = record.get("amount", 0) or 0
        processed.append({
            "id": record.get("id"),
            "amount": amount,
            "category": record.get("category", "unknown"),
        })
        total += amount

    return {
        "records": processed,
        "total_amount": total,
        "record_count": len(processed),
        "avg_amount": round(total / len(processed), 2) if processed else 0,
        "transformed_at": datetime.now().isoformat(),
    }


def load_data(processed_data: dict, destination: str = "warehouse") -> dict:
    """Load processed data to destination."""
    record_count = processed_data.get("record_count", 0)

    return {
        "destination": destination,
        "records_loaded": record_count,
        "status": "success" if record_count > 0 else "empty",
        "loaded_at": datetime.now().isoformat(),
    }


# =========================================================================
# FIXTURES
# =========================================================================


@pytest.fixture
def sample_api_response():
    """Sample API response for testing."""
    return [
        {"id": 1, "amount": 100, "category": "electronics"},
        {"id": 2, "amount": 200, "category": "clothing"},
        {"id": 3, "amount": 150, "category": "electronics"},
    ]


@pytest.fixture
def sample_raw_data(sample_api_response):
    """Sample raw data as it comes from extract."""
    return {
        "data": sample_api_response,
        "extracted_at": "2024-01-15T10:00:00",
    }


@pytest.fixture
def sample_processed_data():
    """Sample processed data for load testing."""
    return {
        "records": [
            {"id": 1, "amount": 100, "category": "electronics"},
            {"id": 2, "amount": 200, "category": "clothing"},
        ],
        "total_amount": 300,
        "record_count": 2,
        "avg_amount": 150.0,
    }


# =========================================================================
# TEST CLASS: Transform Function
# =========================================================================


class TestTransformData:
    """Unit tests for the transform_data function."""

    def test_calculates_correct_total(self, sample_raw_data):
        """Test that total amount is calculated correctly."""
        result = transform_data(sample_raw_data)

        assert result["total_amount"] == 450  # 100 + 200 + 150

    def test_counts_records_correctly(self, sample_raw_data):
        """Test that record count is correct."""
        result = transform_data(sample_raw_data)

        assert result["record_count"] == 3

    def test_calculates_average(self, sample_raw_data):
        """Test that average is calculated correctly."""
        result = transform_data(sample_raw_data)

        assert result["avg_amount"] == 150.0  # 450 / 3

    def test_preserves_all_records(self, sample_raw_data):
        """Test that all records are preserved in output."""
        result = transform_data(sample_raw_data)

        assert len(result["records"]) == 3
        ids = [r["id"] for r in result["records"]]
        assert ids == [1, 2, 3]

    def test_handles_empty_data(self):
        """Test graceful handling of empty data."""
        result = transform_data({"data": []})

        assert result["total_amount"] == 0
        assert result["record_count"] == 0
        assert result["avg_amount"] == 0
        assert result["records"] == []

    def test_handles_missing_data_key(self):
        """Test handling when 'data' key is missing."""
        result = transform_data({})

        assert result["record_count"] == 0

    def test_handles_missing_amount(self):
        """Test handling records with missing amount field."""
        raw_data = {
            "data": [
                {"id": 1, "category": "test"},  # No amount
                {"id": 2, "amount": 100},
            ]
        }

        result = transform_data(raw_data)

        assert result["total_amount"] == 100
        assert result["record_count"] == 2

    def test_handles_null_amount(self):
        """Test handling records with null amount."""
        raw_data = {
            "data": [
                {"id": 1, "amount": None},
                {"id": 2, "amount": 100},
            ]
        }

        result = transform_data(raw_data)

        assert result["total_amount"] == 100

    def test_defaults_missing_category(self):
        """Test that missing category defaults to 'unknown'."""
        raw_data = {
            "data": [{"id": 1, "amount": 100}]  # No category
        }

        result = transform_data(raw_data)

        assert result["records"][0]["category"] == "unknown"

    def test_includes_timestamp(self, sample_raw_data):
        """Test that transformation includes timestamp."""
        result = transform_data(sample_raw_data)

        assert "transformed_at" in result
        # Verify it's a valid ISO format
        datetime.fromisoformat(result["transformed_at"])


# =========================================================================
# TEST CLASS: Load Function
# =========================================================================


class TestLoadData:
    """Unit tests for the load_data function."""

    def test_returns_success_for_data(self, sample_processed_data):
        """Test success status when data is present."""
        result = load_data(sample_processed_data)

        assert result["status"] == "success"
        assert result["records_loaded"] == 2

    def test_returns_empty_for_no_data(self):
        """Test empty status when no records."""
        result = load_data({"records": [], "record_count": 0})

        assert result["status"] == "empty"
        assert result["records_loaded"] == 0

    def test_uses_default_destination(self, sample_processed_data):
        """Test that default destination is used."""
        result = load_data(sample_processed_data)

        assert result["destination"] == "warehouse"

    def test_uses_custom_destination(self, sample_processed_data):
        """Test that custom destination can be specified."""
        result = load_data(sample_processed_data, destination="staging")

        assert result["destination"] == "staging"

    def test_includes_timestamp(self, sample_processed_data):
        """Test that load includes timestamp."""
        result = load_data(sample_processed_data)

        assert "loaded_at" in result


# =========================================================================
# TEST CLASS: Extract Function (Mocked)
# =========================================================================


class TestExtractData:
    """Unit tests for the extract_data function with mocked API."""

    @patch('requests.get')
    def test_makes_api_call(self, mock_get, sample_api_response):
        """Test that API is called with correct parameters."""
        mock_get.return_value.json.return_value = sample_api_response
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status = MagicMock()

        extract_data("https://api.test.com/data")

        mock_get.assert_called_once_with(
            "https://api.test.com/data",
            timeout=30
        )

    @patch('requests.get')
    def test_returns_extracted_data(self, mock_get, sample_api_response):
        """Test that extracted data is returned correctly."""
        mock_get.return_value.json.return_value = sample_api_response
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status = MagicMock()

        result = extract_data()

        assert result["data"] == sample_api_response
        assert "extracted_at" in result

    @patch('requests.get')
    def test_raises_on_http_error(self, mock_get):
        """Test that HTTP errors are propagated."""
        from requests.exceptions import HTTPError

        mock_get.return_value.raise_for_status.side_effect = HTTPError("404")

        with pytest.raises(HTTPError):
            extract_data()

    @patch('requests.get')
    def test_raises_on_timeout(self, mock_get):
        """Test that timeout errors are propagated."""
        from requests.exceptions import Timeout

        mock_get.side_effect = Timeout("Connection timed out")

        with pytest.raises(Timeout):
            extract_data()


# =========================================================================
# PARAMETRIZED TESTS
# =========================================================================


class TestTransformParametrized:
    """Parametrized tests for transform function."""

    @pytest.mark.parametrize("amounts,expected_total", [
        ([100], 100),
        ([100, 200], 300),
        ([100, 200, 300], 600),
        ([0, 0, 0], 0),
        ([1, 2, 3, 4, 5], 15),
    ])
    def test_total_calculation(self, amounts, expected_total):
        """Test total calculation with various inputs."""
        records = [{"id": i, "amount": a} for i, a in enumerate(amounts)]
        raw_data = {"data": records}

        result = transform_data(raw_data)

        assert result["total_amount"] == expected_total

    @pytest.mark.parametrize("record_count", [0, 1, 5, 10, 100])
    def test_handles_various_sizes(self, record_count):
        """Test handling of various data sizes."""
        records = [{"id": i, "amount": 10} for i in range(record_count)]
        raw_data = {"data": records}

        result = transform_data(raw_data)

        assert result["record_count"] == record_count
        assert result["total_amount"] == record_count * 10


# =========================================================================
# INTEGRATION-STYLE TESTS
# =========================================================================


class TestPipelineIntegration:
    """Integration tests for the full pipeline."""

    @patch('requests.get')
    def test_full_pipeline_success(self, mock_get, sample_api_response):
        """Test successful execution of full pipeline."""
        mock_get.return_value.json.return_value = sample_api_response
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status = MagicMock()

        # Execute pipeline
        extracted = extract_data()
        transformed = transform_data(extracted)
        loaded = load_data(transformed)

        # Verify end-to-end
        assert loaded["status"] == "success"
        assert loaded["records_loaded"] == 3

    @patch('requests.get')
    def test_pipeline_with_empty_response(self, mock_get):
        """Test pipeline handles empty API response."""
        mock_get.return_value.json.return_value = []
        mock_get.return_value.status_code = 200
        mock_get.return_value.raise_for_status = MagicMock()

        extracted = extract_data()
        transformed = transform_data(extracted)
        loaded = load_data(transformed)

        assert loaded["status"] == "empty"
        assert loaded["records_loaded"] == 0
