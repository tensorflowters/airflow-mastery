# Contributing to Airflow Mastery

Thank you for your interest in contributing to the Airflow Mastery curriculum! This guide will help you get started.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Contribution Types](#contribution-types)
- [Style Guide](#style-guide)
- [Testing](#testing)
- [Submitting Changes](#submitting-changes)

## Code of Conduct

This project follows a simple code of conduct:
- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and grow

## Getting Started

### Prerequisites

- Python 3.9+
- Apache Airflow 3.x
- Docker and Docker Compose (for local development)
- Git

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/airflow-mastery.git
   cd airflow-mastery
   ```

3. Add upstream remote:
   ```bash
   git remote add upstream https://github.com/ORIGINAL_OWNER/airflow-mastery.git
   ```

## Development Setup

### Using Docker (Recommended)

```bash
cd infrastructure/docker-compose
cp .env.example .env
docker-compose up -d
```

### Local Python Environment

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows

# Install dependencies
pip install apache-airflow==3.1.5
pip install pytest pytest-cov

# Run tests
pytest tests/ -v
```

## Contribution Types

### Bug Fixes

1. Check existing issues for duplicates
2. Create a clear bug report with:
   - Steps to reproduce
   - Expected vs actual behavior
   - Airflow version and environment

### New Content

#### Adding a New Module

1. Create directory structure:
   ```
   modules/XX-module-name/
   ├── README.md           # Module overview and learning objectives
   ├── exercises/          # Exercise starter files
   │   ├── exercise_X_1_name_starter.py
   │   └── ...
   └── solutions/          # Complete solutions
       ├── solution_X_1_name.py
       └── ...
   ```

2. Follow the module template in existing modules

#### Adding Exercises

1. Create exercise file following naming convention:
   ```
   exercise_X_Y_descriptive_name_starter.py
   ```

2. Include:
   - Clear docstring with objectives
   - TODO sections for learners
   - Success criteria checklist
   - Hints where helpful

3. Create matching solution file:
   ```
   solution_X_Y_descriptive_name.py
   ```

#### Adding Example DAGs

1. Add to `dags/examples/` directory
2. Follow naming convention: `XX_descriptive_name.py`
3. Include comprehensive comments explaining concepts

### Documentation Improvements

- Fix typos and clarify explanations
- Add missing documentation
- Improve code comments
- Update outdated information

## Style Guide

### Python Code

Follow Airflow 3.x patterns:

```python
"""
Module docstring explaining purpose.
"""

from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="descriptive_dag_name",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["category", "module-XX"],
    description="Clear description of what this DAG does",
)
def my_dag():
    """DAG-level docstring."""

    @task
    def my_task() -> dict:
        """Task docstring explaining purpose."""
        return {"status": "success"}

    my_task()


# Instantiate the DAG
my_dag()
```

### Key Conventions

1. **DAG Decorator Pattern**: Use `@dag` decorator for TaskFlow DAGs
2. **Type Hints**: Include return type hints on `@task` functions
3. **Tags**: Always include `module-XX` tag and descriptive category tags
4. **Catchup**: Default to `catchup=False` for learning exercises
5. **Docstrings**: Every function and module should have docstrings
6. **Comments**: Explain the "why", not the "what"

### File Naming

| Type | Pattern | Example |
|------|---------|---------|
| Exercise | `exercise_X_Y_name_starter.py` | `exercise_2_1_etl_pipeline_starter.py` |
| Solution | `solution_X_Y_name.py` | `solution_2_1_etl_pipeline.py` |
| Example DAG | `XX_descriptive_name.py` | `02_taskflow_etl.py` |

### Commit Messages

Follow conventional commits format:

```
type(scope): description

feat(module-05): add multi-asset trigger exercise
fix(infrastructure): update pod template version
docs(readme): clarify installation steps
test(solutions): add parametrized tests for module 3
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `style`, `chore`

## Testing

### Running Tests

```bash
# All tests
pytest tests/ -v

# Specific test file
pytest tests/test_solutions.py -v

# With coverage
pytest tests/ --cov=dags --cov-report=html

# Skip slow tests
pytest tests/ -v -m "not slow"

# Include integration tests
pytest tests/ -v --run-integration
```

### Writing Tests

Add tests for:
- New solutions (DAG parsing, structure validation)
- TaskFlow patterns (XCom, mapping, context)
- Exercise structure (TODO sections, hints)

Example test:

```python
def test_solution_has_correct_tags(self, solution_file):
    """Verify solution includes required tags."""
    content = solution_file.read_text()

    assert '"solution"' in content
    assert f'"module-{module_num:02d}"' in content
```

## Submitting Changes

### Pull Request Process

1. Create feature branch:
   ```bash
   git checkout -b feat/add-module-15
   ```

2. Make changes and commit:
   ```bash
   git add .
   git commit -m "feat(module-15): add advanced orchestration module"
   ```

3. Push to your fork:
   ```bash
   git push origin feat/add-module-15
   ```

4. Create Pull Request with:
   - Clear title and description
   - Link to related issues
   - Screenshots/examples if applicable

### PR Checklist

- [ ] Tests pass locally (`pytest tests/ -v`)
- [ ] Code follows style guide
- [ ] Docstrings and comments included
- [ ] README updated if adding new module
- [ ] No credentials or secrets in code

### Review Process

1. Maintainers will review within 1-2 weeks
2. Address feedback and push updates
3. Once approved, changes will be merged

## Questions?

- Open an issue for general questions
- Tag with `question` label
- Check existing issues first

Thank you for contributing to Airflow Mastery!
