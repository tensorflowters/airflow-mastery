"""
Example DAG: Task Groups

Demonstrates TaskGroups for organizing complex DAGs into
logical sections. Shows nesting, parallel groups, and UI benefits.

Module: 06-dynamic-tasks
"""

from datetime import datetime

from airflow.sdk import DAG, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator


# =============================================================================
# DAG 1: Basic Task Groups
# =============================================================================

with DAG(
    dag_id="09_task_groups_basic",
    description="Demonstrates basic TaskGroup usage",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-06", "task-groups"],
):
    start = EmptyOperator(task_id="start")

    # TaskGroup creates a collapsible section in the UI
    with TaskGroup(group_id="extract") as extract_group:
        @task
        def extract_users():
            """Extract user data."""
            print("Extracting users...")
            return {"users": 100}

        @task
        def extract_orders():
            """Extract order data."""
            print("Extracting orders...")
            return {"orders": 500}

        @task
        def extract_products():
            """Extract product data."""
            print("Extracting products...")
            return {"products": 50}

        # Tasks run in parallel within group
        users = extract_users()
        orders = extract_orders()
        products = extract_products()

    with TaskGroup(group_id="transform") as transform_group:
        @task
        def transform_data(users: dict, orders: dict, products: dict):
            """Transform all extracted data."""
            print("Transforming data...")
            return {
                "total_users": users["users"],
                "total_orders": orders["orders"],
                "total_products": products["products"],
            }

        transformed = transform_data(users, orders, products)

    with TaskGroup(group_id="load") as load_group:
        @task
        def load_warehouse(data: dict):
            """Load data to warehouse."""
            print(f"Loading to warehouse: {data}")
            return {"loaded": True}

        @task
        def load_reports(data: dict):
            """Load data for reports."""
            print(f"Loading to reports: {data}")
            return {"loaded": True}

        load_warehouse(transformed)
        load_reports(transformed)

    end = EmptyOperator(task_id="end")

    # Group dependencies
    start >> extract_group >> transform_group >> load_group >> end


# =============================================================================
# DAG 2: Nested Task Groups
# =============================================================================

with DAG(
    dag_id="09_task_groups_nested",
    description="Demonstrates nested TaskGroups",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-06", "task-groups", "nested"],
):
    start = EmptyOperator(task_id="start")

    # Outer group for data processing
    with TaskGroup(group_id="data_processing") as data_processing:

        # Nested group for validation
        with TaskGroup(group_id="validation") as validation:
            @task
            def validate_schema():
                print("Validating schema...")
                return True

            @task
            def validate_types():
                print("Validating data types...")
                return True

            validate_schema()
            validate_types()

        # Nested group for cleaning
        with TaskGroup(group_id="cleaning") as cleaning:
            @task
            def remove_duplicates():
                print("Removing duplicates...")
                return {"removed": 10}

            @task
            def fill_missing():
                print("Filling missing values...")
                return {"filled": 5}

            remove_duplicates()
            fill_missing()

        # Sequential: validate then clean
        validation >> cleaning

    # Outer group for ML pipeline
    with TaskGroup(group_id="ml_pipeline") as ml_pipeline:

        with TaskGroup(group_id="feature_engineering") as features:
            @task
            def create_features():
                print("Creating features...")
                return {"features": 50}

            create_features()

        with TaskGroup(group_id="training") as training:
            @task
            def train_model():
                print("Training model...")
                return {"accuracy": 0.95}

            train_model()

        features >> training

    end = EmptyOperator(task_id="end")

    start >> data_processing >> ml_pipeline >> end


# =============================================================================
# DAG 3: Dynamic Task Groups
# =============================================================================

with DAG(
    dag_id="09_task_groups_dynamic",
    description="Demonstrates dynamically created TaskGroups",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-06", "task-groups", "dynamic"],
):
    start = EmptyOperator(task_id="start")

    # Create task groups dynamically based on data
    regions = ["us_east", "us_west", "eu", "asia"]

    all_groups = []

    for region in regions:
        with TaskGroup(group_id=f"process_{region}") as region_group:
            @task(task_id=f"extract_{region}")
            def extract_region_data(region=region):
                print(f"Extracting data for {region}")
                return {"region": region, "records": 1000}

            @task(task_id=f"transform_{region}")
            def transform_region_data(data: dict, region=region):
                print(f"Transforming data for {region}")
                return {"region": region, "processed": True}

            @task(task_id=f"load_{region}")
            def load_region_data(data: dict, region=region):
                print(f"Loading data for {region}")
                return {"region": region, "loaded": True}

            extracted = extract_region_data()
            transformed = transform_region_data(extracted)
            load_region_data(transformed)

            all_groups.append(region_group)

    @task
    def aggregate_results():
        """Aggregate results from all regions."""
        print("Aggregating all region results...")
        return {"total_regions": len(regions)}

    end = EmptyOperator(task_id="end")

    # All region groups run in parallel
    start >> all_groups >> aggregate_results() >> end


# =============================================================================
# DAG 4: Task Group Best Practices
# =============================================================================

with DAG(
    dag_id="09_task_groups_best_practices",
    description="TaskGroup organization best practices",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "module-06", "task-groups", "best-practices"],
):
    @task
    def show_best_practices():
        """Display TaskGroup best practices."""
        practices = """
        TaskGroup Best Practices:

        1. LOGICAL GROUPING:
           - Group by business function (extract, transform, load)
           - Group by data source or destination
           - Group by stage of pipeline

        2. NAMING CONVENTIONS:
           - Use snake_case for group_id
           - Prefix with stage: extract_, transform_, load_
           - Be descriptive but concise

        3. NESTING GUIDELINES:
           - Max 2-3 levels of nesting
           - Keep nested groups small (2-5 tasks)
           - Don't over-nest simple workflows

        4. UI CONSIDERATIONS:
           - Groups collapse in Graph view
           - Use tooltip_text for descriptions
           - Consider grid view for monitoring

        5. DEPENDENCY PATTERNS:
           - Set dependencies at group level when possible
           - Use >> between groups for clarity
           - Avoid cross-group dependencies within tasks

        6. REUSABILITY:
           - Create functions that return TaskGroups
           - Parameterize group creation
           - Share common patterns across DAGs
        """
        print(practices)
        return {"practices": "displayed"}

    # Demonstrate group-level prefix
    with TaskGroup(
        group_id="etl_pipeline",
        tooltip="Complete ETL pipeline with all stages",
        prefix_group_id=True,  # Task IDs will be: etl_pipeline.task_name
    ) as etl:
        @task
        def step_one():
            return "step_one"

        @task
        def step_two():
            return "step_two"

        step_one() >> step_two()

    show_best_practices() >> etl
