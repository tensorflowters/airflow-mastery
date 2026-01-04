# Exercise 9.2: CI/CD Pipeline for DAG Deployment

## Objective

Create a complete CI/CD pipeline using GitHub Actions that validates, tests, and deploys Airflow DAGs automatically.

## Background

CI/CD for Airflow DAGs ensures:
- DAGs are syntactically correct before deployment
- No import errors in DAG files
- Unit tests pass
- Code quality standards are met
- Deployments are automated and consistent

### Pipeline Stages

```
┌─────────────┐    ┌──────────┐    ┌─────────┐    ┌────────────┐
│   Lint &    │───▶│  Validate │───▶│  Test   │───▶│   Deploy   │
│   Format    │    │   DAGs    │    │         │    │            │
└─────────────┘    └──────────┘    └─────────┘    └────────────┘
     │                  │               │               │
     ▼                  ▼               ▼               ▼
  ruff check       DagBag load     pytest run     sync to prod
```

## Requirements

### Part 1: Create Validation Workflow

Create `.github/workflows/validate-dags.yml`:

```yaml
name: Validate DAGs

on:
  pull_request:
    branches: [main]
    paths:
      - 'dags/**'
      - 'tests/**'
      - 'requirements.txt'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install apache-airflow==3.0.2 ruff pytest

      - name: Lint with Ruff
        run: |
          ruff check dags/ --select AIR
          ruff format dags/ --check

      - name: Validate DAG integrity
        run: |
          export AIRFLOW_HOME=$(pwd)/.airflow
          airflow db migrate
          python -c "
          from airflow.models import DagBag
          dag_bag = DagBag(dag_folder='dags/', include_examples=False)
          if dag_bag.import_errors:
              for path, error in dag_bag.import_errors.items():
                  print(f'ERROR in {path}:')
                  print(error)
              exit(1)
          print(f'Successfully loaded {len(dag_bag.dags)} DAGs')
          "

      - name: Run unit tests
        run: pytest tests/ -v --tb=short
```

### Part 2: Create Deployment Workflow

Create `.github/workflows/deploy-dags.yml`:

```yaml
name: Deploy DAGs

on:
  push:
    branches: [main]
    paths:
      - 'dags/**'

jobs:
  # TODO: Implement the deployment job
  # Should:
  # 1. Run validation first
  # 2. Deploy to staging environment
  # 3. Run smoke tests
  # 4. Deploy to production (with approval)
```

### Part 3: DAG Integrity Test Script

Create `tests/test_dag_integrity.py`:

```python
"""
DAG Integrity Tests

These tests ensure all DAGs are valid and follow best practices.
Run during CI/CD to catch issues before deployment.
"""

import pytest
from datetime import datetime, timedelta
from airflow.models import DagBag


@pytest.fixture(scope="session")
def dag_bag():
    """Load all DAGs once for the test session."""
    return DagBag(dag_folder="dags/", include_examples=False)


class TestDagIntegrity:
    """Test suite for DAG integrity."""

    def test_no_import_errors(self, dag_bag):
        """Verify all DAGs load without import errors."""
        # TODO: Implement
        pass

    def test_dag_ids_unique(self, dag_bag):
        """Verify all DAG IDs are unique."""
        # TODO: Implement
        pass

    def test_all_dags_have_description(self, dag_bag):
        """Verify all DAGs have descriptions."""
        # TODO: Implement
        pass

    def test_all_dags_have_owner(self, dag_bag):
        """Verify all DAGs have an owner defined."""
        # TODO: Implement
        pass

    def test_no_cycles(self, dag_bag):
        """Verify no DAGs have circular dependencies."""
        # TODO: Implement
        pass

    def test_start_dates_not_dynamic(self, dag_bag):
        """Verify start_date is not datetime.now()."""
        # TODO: Implement
        pass
```

### Part 4: Deployment Strategy Configuration

Choose and implement a deployment strategy:

**Option A: Git-Sync (Continuous)**
- DAGs sync automatically from Git
- Configure in Helm values.yaml

**Option B: S3/GCS Sync (On-Demand)**
- DAGs copied to cloud storage
- Airflow reads from storage

**Option C: Container Image (Immutable)**
- DAGs baked into Airflow image
- Full control over versions

## Deliverables

1. **`.github/workflows/validate-dags.yml`** - PR validation workflow
2. **`.github/workflows/deploy-dags.yml`** - Deployment workflow
3. **`tests/test_dag_integrity.py`** - Complete integrity test suite
4. **`DEPLOYMENT.md`** - Documentation of deployment strategy

## Starter Files

See `exercise_9_2_cicd_starter/` directory for:
- Workflow templates
- Test file skeleton
- Sample configuration

## Hints

<details>
<summary>Hint 1: Ruff AIR rules</summary>

Ruff has Airflow-specific rules:
```bash
# Check for Airflow best practices
ruff check dags/ --select AIR

# AIR001: Task variable name different from task_id
# AIR002: Task has no owner set
# etc.
```

</details>

<details>
<summary>Hint 2: Test for dynamic start_date</summary>

```python
def test_start_dates_not_dynamic(self, dag_bag):
    """Dynamic start_date causes issues with catchup."""
    for dag_id, dag in dag_bag.dags.items():
        assert dag.start_date is not None, f"{dag_id} has no start_date"
        # Start date should be in the past
        assert dag.start_date < datetime.now(), \
            f"{dag_id} has future start_date"
```

</details>

<details>
<summary>Hint 3: GitHub environments for approval</summary>

```yaml
deploy-production:
  needs: deploy-staging
  environment: production  # Requires approval
  steps:
    - name: Deploy to production
      run: ...
```

</details>

## Success Criteria

- [ ] PR workflow validates DAG syntax and imports
- [ ] PR workflow runs linting with Airflow rules
- [ ] PR workflow executes unit tests
- [ ] Deploy workflow syncs DAGs to target environment
- [ ] Staging deployment happens automatically
- [ ] Production deployment requires approval
- [ ] DAG integrity tests cover all requirements
- [ ] DEPLOYMENT.md documents the chosen strategy

---

Next: [Exercise 9.3: Monitoring Dashboard →](exercise_9_3_monitoring.md)
