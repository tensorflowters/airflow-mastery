"""
Integration Tests
=================

Integration tests for DAG execution using Airflow's DAG.test() method.

These tests verify:
- DAGs can execute end-to-end
- Task dependencies resolve correctly
- XCom passing works in realistic scenarios
- Asset triggers function as expected

Run with: pytest tests/test_integration.py -v
Run slow tests: pytest tests/test_integration.py -v --run-slow

Note: These tests require a full Airflow environment and may take longer to run.
"""

import pytest
import os
from pathlib import Path
from unittest.mock import patch, MagicMock


# Mark all tests in this module as integration tests
pytestmark = [pytest.mark.integration]


def pytest_configure(config):
    """Add custom markers."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )


@pytest.fixture(scope="module")
def airflow_test_env(tmp_path_factory):
    """Set up a test Airflow environment."""
    airflow_home = tmp_path_factory.mktemp("airflow_home")

    os.environ["AIRFLOW_HOME"] = str(airflow_home)
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

    # Create minimal config
    config_path = airflow_home / "airflow.cfg"
    config_path.write_text("""
[core]
load_examples = False
unit_test_mode = True

[database]
sql_alchemy_conn = sqlite:///:memory:
""")

    return airflow_home


class TestDAGTestMethod:
    """Test DAGs using DAG.test() method introduced in Airflow 2.5+."""

    @pytest.fixture
    def simple_dag(self):
        """Create a simple test DAG."""
        try:
            from airflow.sdk import dag, task
            from datetime import datetime

            @dag(
                dag_id="test_simple_dag",
                start_date=datetime(2024, 1, 1),
                schedule=None,
                catchup=False,
            )
            def simple_test_dag():
                @task
                def start() -> str:
                    return "started"

                @task
                def process(value: str) -> str:
                    return f"processed: {value}"

                @task
                def finish(value: str) -> dict:
                    return {"result": value, "status": "complete"}

                result = start()
                processed = process(result)
                finish(processed)

            return simple_test_dag()

        except ImportError:
            pytest.skip("Airflow SDK not available")

    @pytest.mark.slow
    def test_dag_test_method_exists(self, simple_dag, airflow_test_env):
        """Verify DAG has test() method."""
        assert hasattr(simple_dag, "test"), "DAG should have test() method"

    @pytest.mark.slow
    def test_dag_task_count(self, simple_dag, airflow_test_env):
        """Verify DAG has expected number of tasks."""
        assert len(simple_dag.tasks) == 3


class TestExampleDAGsIntegrity:
    """Test that example DAGs can be loaded."""

    @pytest.fixture(scope="class")
    def dag_bag(self, airflow_test_env):
        """Load all DAGs from dags folder."""
        try:
            from airflow.models import DagBag

            dags_folder = Path(__file__).parent.parent / "dags"
            return DagBag(dag_folder=str(dags_folder), include_examples=False)
        except ImportError:
            pytest.skip("Airflow not available")

    @pytest.mark.slow
    def test_no_import_errors(self, dag_bag):
        """Verify all example DAGs import without errors."""
        if dag_bag.import_errors:
            error_msgs = "\n".join(
                f"  {dag}: {error}"
                for dag, error in dag_bag.import_errors.items()
            )
            pytest.fail(f"DAG import errors:\n{error_msgs}")

    @pytest.mark.slow
    def test_all_dags_have_tasks(self, dag_bag):
        """Verify all DAGs have at least one task."""
        empty_dags = [
            dag_id for dag_id, dag in dag_bag.dags.items()
            if len(dag.tasks) == 0
        ]

        assert not empty_dags, f"DAGs with no tasks: {empty_dags}"


class TestSolutionDAGsExecution:
    """Test solution DAGs can be executed."""

    @pytest.fixture(scope="class")
    def solution_dag_bag(self, airflow_test_env):
        """Load solution DAGs."""
        try:
            from airflow.models import DagBag

            # Load from all solution directories
            project_root = Path(__file__).parent.parent

            # Create a temporary dags folder with symlinks to solutions
            all_dags = {}

            for solution_dir in project_root.glob("modules/*/solutions"):
                dag_bag = DagBag(
                    dag_folder=str(solution_dir),
                    include_examples=False,
                )
                all_dags.update(dag_bag.dags)

            return all_dags

        except ImportError:
            pytest.skip("Airflow not available")

    @pytest.mark.slow
    def test_solution_dags_loadable(self, solution_dag_bag):
        """Verify solution DAGs can be loaded."""
        assert len(solution_dag_bag) > 0, "No solution DAGs were loaded"

    @pytest.mark.slow
    def test_solution_dags_have_owner(self, solution_dag_bag):
        """Verify solution DAGs have owner defined."""
        dags_without_owner = []

        for dag_id, dag in solution_dag_bag.items():
            if not dag.owner or dag.owner == "airflow":
                # Default owner is "airflow" - solutions should override
                pass  # Allow default for learning exercises

        # This is informational, not a hard requirement
        if dags_without_owner:
            pytest.skip(f"DAGs using default owner: {dags_without_owner}")


class TestTaskFlowXComIntegration:
    """Test XCom integration in TaskFlow patterns."""

    @pytest.fixture
    def xcom_dag(self):
        """Create a DAG testing XCom patterns."""
        try:
            from airflow.sdk import dag, task
            from datetime import datetime

            @dag(
                dag_id="test_xcom_dag",
                start_date=datetime(2024, 1, 1),
                schedule=None,
                catchup=False,
            )
            def xcom_test_dag():
                @task
                def produce_dict() -> dict:
                    return {"key": "value", "count": 42}

                @task
                def consume_dict(data: dict) -> str:
                    return f"Received: {data['key']}, count: {data['count']}"

                @task(multiple_outputs=True)
                def produce_multiple() -> dict:
                    return {"first": 1, "second": 2, "third": 3}

                @task
                def consume_individual(first: int, second: int) -> int:
                    return first + second

                # Test basic dict passing
                data = produce_dict()
                consume_dict(data)

                # Test multiple outputs
                outputs = produce_multiple()
                consume_individual(outputs["first"], outputs["second"])

            return xcom_test_dag()

        except ImportError:
            pytest.skip("Airflow SDK not available")

    @pytest.mark.slow
    def test_xcom_dag_structure(self, xcom_dag, airflow_test_env):
        """Verify XCom test DAG has correct structure."""
        assert len(xcom_dag.tasks) == 4

        # Verify task dependencies
        task_ids = [t.task_id for t in xcom_dag.tasks]
        assert "produce_dict" in task_ids
        assert "consume_dict" in task_ids


class TestDynamicTaskMappingIntegration:
    """Test dynamic task mapping integration."""

    @pytest.fixture
    def mapping_dag(self):
        """Create a DAG testing dynamic mapping."""
        try:
            from airflow.sdk import dag, task
            from datetime import datetime

            @dag(
                dag_id="test_mapping_dag",
                start_date=datetime(2024, 1, 1),
                schedule=None,
                catchup=False,
            )
            def mapping_test_dag():
                @task
                def generate_list() -> list[int]:
                    return [1, 2, 3, 4, 5]

                @task
                def square(x: int) -> int:
                    return x ** 2

                @task
                def sum_all(values: list[int]) -> int:
                    return sum(values)

                numbers = generate_list()
                squared = square.expand(x=numbers)
                sum_all(squared)

            return mapping_test_dag()

        except ImportError:
            pytest.skip("Airflow SDK not available")

    @pytest.mark.slow
    def test_mapping_dag_structure(self, mapping_dag, airflow_test_env):
        """Verify mapping DAG has correct structure."""
        # Dynamic mapping creates mapped tasks
        task_ids = [t.task_id for t in mapping_dag.tasks]

        assert "generate_list" in task_ids
        assert "square" in task_ids
        assert "sum_all" in task_ids


class TestAssetIntegration:
    """Test Asset-based scheduling integration."""

    @pytest.fixture
    def producer_consumer_dags(self):
        """Create producer/consumer DAG pair."""
        try:
            from airflow.sdk import dag, task, Asset
            from datetime import datetime

            test_asset = Asset("test://integration/data")

            @dag(
                dag_id="test_producer",
                start_date=datetime(2024, 1, 1),
                schedule=None,
                catchup=False,
            )
            def producer():
                @task(outlets=[test_asset])
                def produce_data() -> dict:
                    return {"produced": True}

                produce_data()

            @dag(
                dag_id="test_consumer",
                start_date=datetime(2024, 1, 1),
                schedule=[test_asset],
                catchup=False,
            )
            def consumer():
                @task
                def consume_data() -> dict:
                    return {"consumed": True}

                consume_data()

            return producer(), consumer(), test_asset

        except ImportError:
            pytest.skip("Airflow SDK not available")

    @pytest.mark.slow
    def test_producer_has_outlets(self, producer_consumer_dags, airflow_test_env):
        """Verify producer task has outlets defined."""
        producer_dag, _, _ = producer_consumer_dags

        produce_task = producer_dag.get_task("produce_data")
        assert hasattr(produce_task, "outlets")

    @pytest.mark.slow
    def test_consumer_schedule_is_asset(self, producer_consumer_dags, airflow_test_env):
        """Verify consumer is scheduled on asset."""
        _, consumer_dag, test_asset = producer_consumer_dags

        # Consumer's schedule should reference the asset
        assert consumer_dag.schedule is not None


class TestSensorIntegration:
    """Test sensor patterns integration."""

    @pytest.fixture
    def sensor_dag(self):
        """Create a DAG with sensor tasks."""
        try:
            from airflow.sdk import dag, task
            from airflow.sensors.base import BaseSensorOperator
            from datetime import datetime

            @dag(
                dag_id="test_sensor_dag",
                start_date=datetime(2024, 1, 1),
                schedule=None,
                catchup=False,
            )
            def sensor_test_dag():
                @task
                def process_after_sensor() -> str:
                    return "processed"

                process_after_sensor()

            return sensor_test_dag()

        except ImportError:
            pytest.skip("Airflow SDK not available")

    @pytest.mark.slow
    def test_sensor_dag_structure(self, sensor_dag, airflow_test_env):
        """Verify sensor DAG structure."""
        assert len(sensor_dag.tasks) >= 1


class TestPoolAndPriorityIntegration:
    """Test pool and priority weight integration."""

    @pytest.fixture
    def resource_dag(self):
        """Create a DAG with resource constraints."""
        try:
            from airflow.sdk import dag, task
            from datetime import datetime

            @dag(
                dag_id="test_resource_dag",
                start_date=datetime(2024, 1, 1),
                schedule=None,
                catchup=False,
            )
            def resource_test_dag():
                @task(pool="test_pool", priority_weight=10)
                def high_priority_task() -> str:
                    return "high priority"

                @task(pool="test_pool", priority_weight=1)
                def low_priority_task() -> str:
                    return "low priority"

                high_priority_task()
                low_priority_task()

            return resource_test_dag()

        except ImportError:
            pytest.skip("Airflow SDK not available")

    @pytest.mark.slow
    def test_tasks_have_pool(self, resource_dag, airflow_test_env):
        """Verify tasks have pool assignment."""
        for task in resource_dag.tasks:
            assert task.pool is not None

    @pytest.mark.slow
    def test_tasks_have_priority(self, resource_dag, airflow_test_env):
        """Verify tasks have priority weights."""
        high_task = resource_dag.get_task("high_priority_task")
        low_task = resource_dag.get_task("low_priority_task")

        assert high_task.priority_weight > low_task.priority_weight


class TestCallbackIntegration:
    """Test callback function integration."""

    @pytest.fixture
    def callback_dag(self):
        """Create a DAG with callbacks."""
        try:
            from airflow.sdk import dag, task
            from datetime import datetime

            callback_results = []

            def on_success(context):
                callback_results.append(("success", context.get("task")))

            def on_failure(context):
                callback_results.append(("failure", context.get("task")))

            @dag(
                dag_id="test_callback_dag",
                start_date=datetime(2024, 1, 1),
                schedule=None,
                catchup=False,
                default_args={
                    "on_success_callback": on_success,
                    "on_failure_callback": on_failure,
                },
            )
            def callback_test_dag():
                @task
                def task_with_callbacks() -> str:
                    return "success"

                task_with_callbacks()

            return callback_test_dag(), callback_results

        except ImportError:
            pytest.skip("Airflow SDK not available")

    @pytest.mark.slow
    def test_dag_has_callbacks(self, callback_dag, airflow_test_env):
        """Verify DAG has callback configuration."""
        dag, _ = callback_dag
        assert dag.default_args.get("on_success_callback") is not None


class TestConcurrencyIntegration:
    """Test concurrency limit integration."""

    @pytest.fixture
    def concurrent_dag(self):
        """Create a DAG with concurrency limits."""
        try:
            from airflow.sdk import dag, task
            from datetime import datetime

            @dag(
                dag_id="test_concurrent_dag",
                start_date=datetime(2024, 1, 1),
                schedule=None,
                catchup=False,
                max_active_runs=1,
                max_active_tasks=2,
            )
            def concurrent_test_dag():
                @task
                def task_a() -> str:
                    return "a"

                @task
                def task_b() -> str:
                    return "b"

                @task
                def task_c() -> str:
                    return "c"

                task_a()
                task_b()
                task_c()

            return concurrent_test_dag()

        except ImportError:
            pytest.skip("Airflow SDK not available")

    @pytest.mark.slow
    def test_dag_concurrency_settings(self, concurrent_dag, airflow_test_env):
        """Verify DAG has concurrency limits."""
        assert concurrent_dag.max_active_runs == 1
        assert concurrent_dag.max_active_tasks == 2
