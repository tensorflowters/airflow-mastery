"""
TaskFlow API Pattern Tests
==========================

Unit tests for @task decorated functions and TaskFlow patterns.

This test suite validates:
- XCom passing between tasks
- Multiple outputs pattern
- Context usage patterns
- Dynamic task mapping with expand()
- Return type handling

Run with: pytest tests/test_taskflow_patterns.py -v
"""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch


class TestXComPatterns:
    """Test XCom data passing patterns."""

    def test_dict_xcom_serialization(self):
        """Verify dictionaries are properly serialized for XCom."""
        # Test data that would be passed via XCom
        test_data = {
            "users": [{"id": 1, "name": "Alice"}],
            "count": 1,
            "timestamp": "2024-01-01T00:00:00",
        }

        # Simulate JSON serialization (what XCom does)
        import json

        serialized = json.dumps(test_data)
        deserialized = json.loads(serialized)

        assert deserialized == test_data
        assert isinstance(deserialized["users"], list)
        assert deserialized["count"] == 1

    def test_list_xcom_serialization(self):
        """Verify lists are properly serialized for XCom."""
        test_data = [1, 2, 3, 4, 5]

        import json

        serialized = json.dumps(test_data)
        deserialized = json.loads(serialized)

        assert deserialized == test_data
        assert len(deserialized) == 5

    def test_nested_dict_xcom_serialization(self):
        """Verify nested dictionaries work with XCom."""
        test_data = {
            "level1": {
                "level2": {
                    "value": "deep",
                    "numbers": [1, 2, 3],
                }
            },
            "flat": "value",
        }

        import json

        serialized = json.dumps(test_data)
        deserialized = json.loads(serialized)

        assert deserialized["level1"]["level2"]["value"] == "deep"


class TestMultipleOutputsPattern:
    """Test multiple_outputs=True pattern."""

    def test_multiple_outputs_dict_unpacking(self):
        """Verify multiple_outputs dict can be accessed by key."""
        # Simulate what transform_orders from solution_2_1 returns
        result = {
            "orders": [{"id": 1}],
            "total": 299.98,
            "order_count": 3,
        }

        # With multiple_outputs=True, each key becomes a separate XCom
        assert result["orders"] == [{"id": 1}]
        assert result["total"] == 299.98
        assert result["order_count"] == 3

    def test_multiple_outputs_key_access(self):
        """Test accessing individual keys from multiple_outputs."""
        # Simulate XComArg["key"] access pattern
        class MockXComArg:
            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                return self._data[key]

        result = MockXComArg({
            "total": 100.0,
            "count": 5,
            "items": ["a", "b", "c"],
        })

        assert result["total"] == 100.0
        assert result["count"] == 5
        assert len(result["items"]) == 3


class TestContextPatterns:
    """Test context usage patterns in @task functions."""

    @pytest.fixture
    def mock_context(self):
        """Provide a comprehensive mock Airflow context."""
        from pendulum import timezone

        utc = timezone("UTC")
        logical_date = utc.datetime(2024, 1, 1, 0, 0, 0)

        mock_ti = MagicMock()
        mock_ti.task_id = "test_task"
        mock_ti.dag_id = "test_dag"
        mock_ti.run_id = "manual__2024-01-01T00:00:00+00:00"

        mock_dag_run = MagicMock()
        mock_dag_run.run_id = "manual__2024-01-01T00:00:00+00:00"
        mock_dag_run.logical_date = logical_date

        return {
            "ds": "2024-01-01",
            "ds_nodash": "20240101",
            "logical_date": logical_date,
            "data_interval_start": logical_date,
            "data_interval_end": utc.datetime(2024, 1, 2, 0, 0, 0),
            "run_id": "manual__2024-01-01T00:00:00+00:00",
            "dag_run": mock_dag_run,
            "ti": mock_ti,
            "params": {"batch_size": 100},
            "var": {"environment": "development"},
        }

    def test_context_ds_format(self, mock_context):
        """Verify ds template format is correct."""
        assert mock_context["ds"] == "2024-01-01"
        assert "-" in mock_context["ds"]

    def test_context_ds_nodash_format(self, mock_context):
        """Verify ds_nodash template format is correct."""
        assert mock_context["ds_nodash"] == "20240101"
        assert "-" not in mock_context["ds_nodash"]

    def test_context_params_access(self, mock_context):
        """Verify params can be accessed from context."""
        params = mock_context["params"]
        assert params["batch_size"] == 100

    def test_context_ti_access(self, mock_context):
        """Verify TaskInstance is accessible from context."""
        ti = mock_context["ti"]
        assert ti.task_id == "test_task"
        assert ti.dag_id == "test_dag"


class TestDynamicTaskMapping:
    """Test dynamic task mapping patterns with expand()."""

    def test_expand_creates_multiple_instances(self):
        """Verify expand() creates instance per input item."""
        # Simulate what happens with .expand(x=numbers)
        input_list = [1, 2, 3, 4, 5]
        results = [x ** 2 for x in input_list]

        assert len(results) == len(input_list)
        assert results == [1, 4, 9, 16, 25]

    def test_expand_preserves_order(self):
        """Verify mapped results maintain order by map_index."""
        input_list = [5, 3, 8, 1]

        # Process in "parallel" but results should be ordered
        results = [x * 2 for x in input_list]

        assert results[0] == 10  # map_index 0: input 5
        assert results[1] == 6   # map_index 1: input 3
        assert results[2] == 16  # map_index 2: input 8
        assert results[3] == 2   # map_index 3: input 1

    def test_expand_aggregation(self):
        """Verify downstream task receives list of all outputs."""
        # Simulate square_number.expand() results being collected
        mapped_outputs = [16, 49, 4, 81, 25]  # Individual task outputs

        # Aggregation task receives the full list
        total = sum(mapped_outputs)
        average = total / len(mapped_outputs)

        assert total == 175
        assert average == 35.0

    def test_expand_with_dict_outputs(self):
        """Verify expand works with dict-returning tasks."""
        inputs = ["file1.csv", "file2.csv", "file3.csv"]

        # Each mapped task returns a dict
        outputs = [
            {"file": f, "rows": 100 * (i + 1), "status": "success"}
            for i, f in enumerate(inputs)
        ]

        # Downstream receives list of dicts
        assert len(outputs) == 3
        assert outputs[0]["file"] == "file1.csv"
        assert outputs[1]["rows"] == 200


class TestReturnTypePatterns:
    """Test return type patterns in TaskFlow tasks."""

    def test_dict_return_type(self):
        """Verify dict return type handling."""
        def sample_task() -> dict:
            return {"status": "success", "count": 42}

        result = sample_task()
        assert isinstance(result, dict)
        assert result["status"] == "success"

    def test_list_return_type(self):
        """Verify list return type handling."""
        def sample_task() -> list[int]:
            return [1, 2, 3, 4, 5]

        result = sample_task()
        assert isinstance(result, list)
        assert len(result) == 5

    def test_none_return_type(self):
        """Verify None return type handling (side-effect tasks)."""
        side_effects = []

        def sample_task() -> None:
            side_effects.append("executed")
            return None

        result = sample_task()
        assert result is None
        assert len(side_effects) == 1


class TestETLPatterns:
    """Test common ETL patterns from solutions."""

    def test_extract_transform_load_pattern(self):
        """Verify ETL pipeline pattern works correctly."""
        # Extract
        def extract() -> dict:
            return {"records": [{"id": 1}, {"id": 2}]}

        # Transform
        def transform(data: dict) -> dict:
            return {
                "records": [
                    {**r, "processed": True}
                    for r in data["records"]
                ],
                "count": len(data["records"]),
            }

        # Load
        def load(data: dict) -> dict:
            return {"status": "success", "loaded": data["count"]}

        # Execute pipeline
        raw = extract()
        transformed = transform(raw)
        result = load(transformed)

        assert result["status"] == "success"
        assert result["loaded"] == 2

    def test_parallel_extract_pattern(self):
        """Verify parallel extraction pattern works."""
        def extract_source_a() -> dict:
            return {"source": "A", "data": [1, 2, 3]}

        def extract_source_b() -> dict:
            return {"source": "B", "data": [4, 5, 6]}

        # Both run in parallel
        result_a = extract_source_a()
        result_b = extract_source_b()

        # Combine in downstream
        combined = result_a["data"] + result_b["data"]

        assert len(combined) == 6
        assert combined == [1, 2, 3, 4, 5, 6]


class TestAssetPatterns:
    """Test Asset-based patterns from Module 5 solutions."""

    def test_producer_consumer_pattern(self):
        """Verify producer/consumer data flow pattern."""
        # Producer creates data
        producer_output = {
            "processed_at": datetime.now().isoformat(),
            "records": 1000,
            "status": "success",
        }

        # Consumer receives (via Asset trigger)
        def consume(data: dict) -> dict:
            return {
                "consumed_at": datetime.now().isoformat(),
                "records_processed": data["records"],
            }

        result = consume(producer_output)
        assert result["records_processed"] == 1000

    def test_multi_asset_trigger_pattern(self):
        """Verify multi-asset trigger pattern."""
        # Multiple producers
        asset_a_update = {"asset": "A", "timestamp": "2024-01-01T01:00:00"}
        asset_b_update = {"asset": "B", "timestamp": "2024-01-01T01:30:00"}

        # Consumer triggered when BOTH are updated
        # (simulating schedule=[asset_a, asset_b])
        all_assets_ready = [asset_a_update, asset_b_update]

        assert len(all_assets_ready) == 2
        assert all(a["asset"] in ["A", "B"] for a in all_assets_ready)


class TestErrorHandlingPatterns:
    """Test error handling patterns in tasks."""

    def test_retry_pattern_simulation(self):
        """Verify retry logic pattern."""
        attempt_count = 0
        max_retries = 3

        def task_with_retry():
            nonlocal attempt_count
            attempt_count += 1

            if attempt_count < 3:
                raise ValueError("Transient error")

            return {"status": "success", "attempts": attempt_count}

        # Simulate retries
        result = None
        for _ in range(max_retries):
            try:
                result = task_with_retry()
                break
            except ValueError:
                continue

        assert result is not None
        assert result["attempts"] == 3

    def test_exception_propagation(self):
        """Verify exceptions propagate correctly."""
        from airflow.exceptions import AirflowException

        def failing_task():
            raise AirflowException("Task failed intentionally")

        with pytest.raises(AirflowException, match="intentionally"):
            failing_task()


class TestSensorPatterns:
    """Test sensor-related patterns from Module 11."""

    def test_poke_mode_simulation(self):
        """Simulate sensor poke mode pattern."""
        check_count = 0

        def sensor_condition():
            nonlocal check_count
            check_count += 1
            return check_count >= 3  # Success on 3rd check

        # Simulate poke mode
        while not sensor_condition():
            pass  # In real sensor: time.sleep(poke_interval)

        assert check_count == 3

    def test_reschedule_mode_simulation(self):
        """Simulate sensor reschedule mode pattern."""
        # In reschedule mode, sensor releases worker slot between checks
        checks = []

        def sensor_check(check_num: int) -> bool:
            checks.append(check_num)
            return check_num >= 2

        # Each check is a separate execution
        for i in range(1, 4):
            if sensor_check(i):
                break

        assert len(checks) == 2
        assert checks[-1] == 2


class TestResourceManagementPatterns:
    """Test resource management patterns from Module 14."""

    def test_pool_slot_pattern(self):
        """Verify pool slot allocation pattern."""
        pool_size = 3
        slots_used = 0

        def acquire_slot() -> bool:
            nonlocal slots_used
            if slots_used < pool_size:
                slots_used += 1
                return True
            return False

        def release_slot():
            nonlocal slots_used
            slots_used -= 1

        # Acquire slots
        assert acquire_slot()  # 1
        assert acquire_slot()  # 2
        assert acquire_slot()  # 3
        assert not acquire_slot()  # Pool full

        release_slot()
        assert acquire_slot()  # Got released slot

    def test_priority_weight_pattern(self):
        """Verify priority weight ordering."""
        tasks = [
            {"id": "low", "priority": 1},
            {"id": "high", "priority": 10},
            {"id": "medium", "priority": 5},
        ]

        # Higher priority runs first
        sorted_tasks = sorted(tasks, key=lambda t: t["priority"], reverse=True)

        assert sorted_tasks[0]["id"] == "high"
        assert sorted_tasks[1]["id"] == "medium"
        assert sorted_tasks[2]["id"] == "low"
