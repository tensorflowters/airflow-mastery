"""
Exercise 7.2: Unit Test a Pipeline (Starter)
=============================================

Write unit tests for an ETL pipeline.

Tests to implement:
1. Happy path tests
2. Edge case tests
3. Mocked dependency tests
"""

# TODO: Import testing libraries
# import pytest
# from unittest.mock import patch, MagicMock
# from datetime import datetime


# =========================================================================
# SAMPLE ETL FUNCTIONS TO TEST
# (In real scenario, these would be imported from your DAG)
# =========================================================================

def extract_data(api_url: str = "https://api.example.com/data") -> dict:
    """
    Extract data from an API.

    In production, this would use requests to fetch data.
    """
    import requests

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    return {
        "data": response.json(),
        "extracted_at": datetime.now().isoformat(),
    }


def transform_data(raw_data: dict) -> dict:
    """
    Transform raw data into processed format.

    Transformations:
    - Calculate totals
    - Clean null values
    - Add derived fields
    """
    records = raw_data.get("data", [])

    if not records:
        return {
            "records": [],
            "total_amount": 0,
            "record_count": 0,
            "transformed_at": datetime.now().isoformat(),
        }

    # Process records
    processed = []
    total = 0

    for record in records:
        amount = record.get("amount", 0) or 0  # Handle None
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
        "transformed_at": datetime.now().isoformat(),
    }


def load_data(processed_data: dict, destination: str = "warehouse") -> dict:
    """
    Load processed data to destination.

    Simulates writing to a database or file.
    """
    record_count = processed_data.get("record_count", 0)

    return {
        "destination": destination,
        "records_loaded": record_count,
        "status": "success" if record_count > 0 else "empty",
        "loaded_at": datetime.now().isoformat(),
    }


# =========================================================================
# TEST FIXTURES
# =========================================================================

# TODO: Create fixtures for sample data
# @pytest.fixture
# def sample_api_response():
#     """Sample API response for testing."""
#     return [
#         {"id": 1, "amount": 100, "category": "electronics"},
#         {"id": 2, "amount": 200, "category": "clothing"},
#         {"id": 3, "amount": 150, "category": "electronics"},
#     ]


# @pytest.fixture
# def sample_raw_data(sample_api_response):
#     """Sample raw data as it comes from extract."""
#     return {
#         "data": sample_api_response,
#         "extracted_at": "2024-01-15T10:00:00",
#     }


# =========================================================================
# HAPPY PATH TESTS
# =========================================================================

# TODO: Test transform with normal data
# def test_transform_calculates_total(sample_raw_data):
#     """Test that transform correctly calculates total amount."""
#     result = transform_data(sample_raw_data)
#
#     assert result["total_amount"] == 450  # 100 + 200 + 150
#     assert result["record_count"] == 3


# TODO: Test transform preserves record data
# def test_transform_preserves_records(sample_raw_data):
#     """Test that transform preserves all records."""
#     result = transform_data(sample_raw_data)
#
#     assert len(result["records"]) == 3
#     assert result["records"][0]["id"] == 1
#     assert result["records"][0]["amount"] == 100


# TODO: Test load returns correct status
# def test_load_returns_success():
#     """Test that load returns success for non-empty data."""
#     processed = {
#         "records": [{"id": 1, "amount": 100}],
#         "total_amount": 100,
#         "record_count": 1,
#     }
#
#     result = load_data(processed)
#
#     assert result["status"] == "success"
#     assert result["records_loaded"] == 1


# =========================================================================
# EDGE CASE TESTS
# =========================================================================

# TODO: Test transform with empty data
# def test_transform_handles_empty_data():
#     """Test that transform handles empty data gracefully."""
#     result = transform_data({"data": []})
#
#     assert result["total_amount"] == 0
#     assert result["record_count"] == 0
#     assert result["records"] == []


# TODO: Test transform with missing fields
# def test_transform_handles_missing_amount():
#     """Test that transform handles records with missing amount."""
#     raw_data = {
#         "data": [
#             {"id": 1, "category": "test"},  # No amount
#             {"id": 2, "amount": None},      # Null amount
#             {"id": 3, "amount": 100},       # Normal
#         ]
#     }
#
#     result = transform_data(raw_data)
#
#     assert result["total_amount"] == 100  # Only id=3 has amount
#     assert result["record_count"] == 3    # All records preserved


# TODO: Test load with empty data
# def test_load_handles_empty_data():
#     """Test that load handles empty data set."""
#     result = load_data({"records": [], "record_count": 0})
#
#     assert result["status"] == "empty"
#     assert result["records_loaded"] == 0


# =========================================================================
# MOCKED DEPENDENCY TESTS
# =========================================================================

# TODO: Mock the API call in extract
# @patch('requests.get')
# def test_extract_calls_api(mock_get, sample_api_response):
#     """Test that extract makes correct API call."""
#     # Configure mock
#     mock_get.return_value.json.return_value = sample_api_response
#     mock_get.return_value.status_code = 200
#     mock_get.return_value.raise_for_status = MagicMock()
#
#     result = extract_data("https://api.test.com/data")
#
#     # Verify API was called correctly
#     mock_get.assert_called_once_with(
#         "https://api.test.com/data",
#         timeout=30
#     )
#     assert len(result["data"]) == 3


# TODO: Test API error handling
# @patch('requests.get')
# def test_extract_handles_api_error(mock_get):
#     """Test that extract raises on API error."""
#     from requests.exceptions import HTTPError
#
#     mock_get.return_value.raise_for_status.side_effect = HTTPError("404")
#
#     with pytest.raises(HTTPError):
#         extract_data()


# =========================================================================
# INTEGRATION-STYLE TESTS
# =========================================================================

# TODO: Test full pipeline flow (without external calls)
# @patch('requests.get')
# def test_full_pipeline(mock_get, sample_api_response):
#     """Test the full ETL pipeline flow."""
#     # Setup
#     mock_get.return_value.json.return_value = sample_api_response
#     mock_get.return_value.status_code = 200
#     mock_get.return_value.raise_for_status = MagicMock()
#
#     # Execute pipeline
#     extracted = extract_data()
#     transformed = transform_data(extracted)
#     loaded = load_data(transformed)
#
#     # Verify end-to-end
#     assert loaded["status"] == "success"
#     assert loaded["records_loaded"] == 3
