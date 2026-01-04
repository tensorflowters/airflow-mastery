# Module 00: Development Environment & Modern Python Tooling

## ⚠️ Prerequisites

Before starting this module, ensure you have:

- **Operating System**: macOS, Linux, or Windows with WSL2
- **Terminal Access**: Comfortable with basic command-line operations
- **Docker**: Docker Desktop or Docker Engine installed and running (`docker --version`)
- **Git**: Version control installed (`git --version`)
- **Text Editor**: VS Code (recommended) or any code editor
- **No Python Required**: uv will handle Python installation for you!

### System Requirements

| Component  | Minimum   | Recommended   |
| ---------- | --------- | ------------- |
| RAM        | 4 GB      | 8+ GB         |
| Disk Space | 5 GB free | 10+ GB free   |
| Docker     | 20.10+    | Latest stable |

### Quick Environment Check

```bash
# Run these commands to verify your environment
docker --version      # Should show Docker 20.10+
git --version         # Should show git 2.x+
```

---

## 🎯 Learning Objectives

By the end of this module, you will:

- Install and configure **uv** for lightning-fast Python package management
- Create a professional **pyproject.toml** with proper Airflow dependency groups
- Set up **ruff** for linting and formatting (replaces flake8, black, isort)
- Configure **pre-commit hooks** for automated code quality
- Build **optimized Docker images** using uv for Airflow deployments
- Establish a **testing workflow** with pytest and DAG validation

## ⏱️ Estimated Time: 2-3 hours

---

## 1. Why Modern Python Tooling Matters

If you've worked with Python projects before, you've likely experienced:

- **Slow installs**: `pip install` taking minutes for large dependency trees
- **Tool fragmentation**: Juggling black, flake8, isort, pyupgrade separately
- **Lock file confusion**: requirements.txt vs Pipfile.lock vs poetry.lock
- **Environment mismatches**: "Works on my machine" across team members

### The Modern Stack

| Traditional            | Modern             | Improvement                    |
| ---------------------- | ------------------ | ------------------------------ |
| pip + venv             | **uv**             | 10-100x faster, unified tool   |
| black + flake8 + isort | **ruff**           | Single tool, instant feedback  |
| requirements.txt       | **pyproject.toml** | Standard format, rich metadata |
| setup.py               | **pyproject.toml** | Declarative, no code execution |

### Why uv?

**uv** (by Astral, the ruff creators) is a Python package manager written in Rust:

- **Speed**: Installs packages 10-100x faster than pip
- **Unified**: Replaces pip, pip-tools, virtualenv, pyenv in one tool
- **Reproducible**: Cross-platform lock files (`uv.lock`)
- **Compatible**: Works with existing pyproject.toml files

```bash
# Speed comparison (example: installing airflow + dependencies)
# pip:  45-90 seconds
# uv:   3-8 seconds
```

---

## 2. Installing and Understanding uv

### Installation

Choose one method:

```bash
# Recommended: Standalone installer (no Python required)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Alternative: Via pip (if you have Python already)
pip install uv

# Alternative: Homebrew (macOS)
brew install uv
```

After installation, verify:

```bash
uv --version
# Output: uv 0.5.x (or higher)
```

### uv vs pip: Mental Model

| pip Command                       | uv Equivalent                        | Notes                            |
| --------------------------------- | ------------------------------------ | -------------------------------- |
| `python -m venv .venv`            | `uv venv`                            | Creates virtual environment      |
| `pip install package`             | `uv pip install package`             | Installs package                 |
| `pip install -r requirements.txt` | `uv pip install -r requirements.txt` | From requirements                |
| `pip freeze > requirements.txt`   | `uv pip compile`                     | Generate lock file               |
| N/A                               | `uv sync`                            | Sync from pyproject.toml + lock  |
| N/A                               | `uv add package`                     | Add dependency to pyproject.toml |

### Project Workflow with uv

```bash
# Initialize a new project
uv init my-airflow-project
cd my-airflow-project

# Add dependencies
uv add apache-airflow
uv add --dev pytest ruff pre-commit

# Create virtual environment and install
uv venv
uv sync

# Run commands in the environment
uv run python -c "import airflow; print(airflow.__version__)"
uv run pytest
```

### Understanding Lock Files

uv generates `uv.lock`—a cross-platform lock file that:

- Records exact versions of all dependencies (including transitive)
- Ensures reproducible builds across machines
- Is human-readable TOML but managed by uv

**Key Rule**: Commit `uv.lock` to version control. Never edit it manually.

---

## 3. Project Configuration with pyproject.toml

`pyproject.toml` is the modern standard for Python project configuration (PEP 518, 621). It replaces `setup.py`, `setup.cfg`, and `requirements.txt`.

### Anatomy for Airflow Projects

```toml
[project]
name = "my-airflow-project"
version = "0.1.0"
description = "My Airflow DAGs and workflows"
readme = "README.md"
requires-python = ">=3.9,<3.13"
license = { text = "MIT" }
authors = [
    { name = "Your Name", email = "you@example.com" }
]

# Core dependencies (installed by default)
dependencies = [
    "apache-airflow>=3.0.0,<4.0.0",
]

# Optional dependency groups
[project.optional-dependencies]
# Development tools
dev = [
    "ruff>=0.8.0",
    "pre-commit>=4.0.0",
    "mypy>=1.0.0",
]

# Testing dependencies
test = [
    "pytest>=8.0.0",
    "pytest-cov>=4.0.0",
    "pytest-airflow>=0.1.0",
]

# Common Airflow providers
providers = [
    "apache-airflow-providers-postgres>=5.0.0",
    "apache-airflow-providers-http>=4.0.0",
    "apache-airflow-providers-celery>=3.0.0",
    "apache-airflow-providers-standard>=0.1.0",
]

# All optional dependencies
all = [
    "my-airflow-project[dev,test,providers]",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

### Dependency Groups Explained

| Group          | Purpose           | When to Install     |
| -------------- | ----------------- | ------------------- |
| `dependencies` | Core runtime      | Always (production) |
| `dev`          | Development tools | Local development   |
| `test`         | Testing tools     | CI/CD + local       |
| `providers`    | Airflow providers | As needed           |

### Installing Groups with uv

```bash
# Core dependencies only
uv sync

# With development tools
uv sync --group dev

# With testing
uv sync --group test

# Everything
uv sync --all-groups
```

---

## 4. Code Quality with Ruff

**Ruff** is a Python linter and formatter written in Rust that replaces:

- flake8 (linting)
- black (formatting)
- isort (import sorting)
- pyupgrade (Python version upgrades)
- And 20+ other tools

### Speed Comparison

```bash
# Linting a large codebase (example: 1000 files)
# flake8: 45 seconds
# ruff:   0.3 seconds (150x faster)
```

### Configuration in pyproject.toml

```toml
[tool.ruff]
# Same line length as Black default
line-length = 88

# Target Python version
target-version = "py39"

# Directories to exclude
exclude = [
    ".git",
    ".venv",
    "__pycache__",
    "build",
    "dist",
]

[tool.ruff.lint]
# Enable these rule sets
select = [
    "E",      # pycodestyle errors
    "W",      # pycodestyle warnings
    "F",      # Pyflakes
    "I",      # isort (import sorting)
    "B",      # flake8-bugbear
    "C4",     # flake8-comprehensions
    "UP",     # pyupgrade
    "SIM",    # flake8-simplify
    "AIR",    # Airflow-specific rules
]

# Ignore specific rules
ignore = [
    "E501",   # Line too long (handled by formatter)
]

# Allow autofix for these
fixable = ["ALL"]

[tool.ruff.lint.isort]
known-first-party = ["dags", "plugins", "tests"]

[tool.ruff.format]
# Use double quotes (like Black)
quote-style = "double"

# Indent with spaces
indent-style = "space"
```

### Airflow-Specific Rules (AIR)

Ruff includes Airflow-specific rules:

| Rule   | Description                                |
| ------ | ------------------------------------------ |
| AIR001 | Task variable name doesn't match `task_id` |
| AIR301 | DAG uses deprecated `schedule_interval`    |
| AIR302 | Uses deprecated Airflow 2.x import paths   |

### Running Ruff

```bash
# Check for issues
uv run ruff check .

# Check and fix auto-fixable issues
uv run ruff check --fix .

# Format code
uv run ruff format .

# Check if code is formatted
uv run ruff format --check .
```

### IDE Integration

**VS Code**: Install the "Ruff" extension by Astral:

```json
// .vscode/settings.json
{
  "editor.formatOnSave": true,
  "editor.codeActionsOnSave": {
    "source.fixAll.ruff": "explicit",
    "source.organizeImports.ruff": "explicit"
  },
  "[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff"
  }
}
```

---

## 5. Pre-commit Hooks

Pre-commit hooks run automated checks before every commit, catching issues early.

### Installation

```bash
# Add to dev dependencies
uv add --dev pre-commit

# Create configuration file
touch .pre-commit-config.yaml
```

### Configuration

```yaml
# .pre-commit-config.yaml
repos:
  # Ruff for linting and formatting
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.8.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  # General file checks
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
        args: [--unsafe] # Allows custom YAML tags
      - id: check-added-large-files
        args: [--maxkb=500]
      - id: check-merge-conflict

  # Security checks
  - repo: https://github.com/Yelp/detect-secrets
    rev: v1.5.0
    hooks:
      - id: detect-secrets
        args: [--baseline, .secrets.baseline]
```

### Activating Hooks

```bash
# Install hooks into your git repository
uv run pre-commit install

# Run manually on all files (first time)
uv run pre-commit run --all-files
```

After installation, hooks run automatically on `git commit`. If a hook fails, the commit is blocked until you fix the issue.

### CI Integration

Add to your CI pipeline to ensure consistency:

```yaml
# .github/workflows/lint.yml
name: Lint
on: [push, pull_request]
jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - run: uv sync --group dev
      - run: uv run pre-commit run --all-files
```

---

## 6. Docker Builds with uv

Building Docker images for Airflow with uv is faster and produces smaller images.

### Multi-Stage Dockerfile Pattern

```dockerfile
# syntax=docker/dockerfile:1

# ============================================
# Stage 1: Build environment with uv
# ============================================
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

# Set environment variables
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Copy dependency files first (for layer caching)
COPY pyproject.toml uv.lock ./

# Install dependencies (without the project itself)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev

# Copy the rest of the application
COPY dags/ ./dags/
COPY plugins/ ./plugins/

# Install the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# ============================================
# Stage 2: Runtime image (no uv)
# ============================================
FROM python:3.11-slim-bookworm

# Create non-root user for security
RUN groupadd --system --gid 1000 airflow \
    && useradd --system --gid 1000 --uid 1000 airflow

# Copy virtual environment from builder
COPY --from=builder --chown=airflow:airflow /app/.venv /app/.venv

# Copy application code
COPY --from=builder --chown=airflow:airflow /app/dags /app/dags
COPY --from=builder --chown=airflow:airflow /app/plugins /app/plugins

# Set path to use virtual environment
ENV PATH="/app/.venv/bin:$PATH" \
    AIRFLOW_HOME=/app

USER airflow
WORKDIR /app
```

### Why This Pattern?

1. **Layer Caching**: Dependencies change less often than code. By copying `pyproject.toml` and `uv.lock` first, Docker can cache the dependency installation layer.

2. **Smaller Images**: The final image doesn't include uv, build tools, or cache—only the runtime environment.

3. **Security**: Runs as non-root user with minimal installed packages.

### Build Commands

```bash
# Build the image
docker build -t my-airflow:latest .

# Build with cache (for faster rebuilds)
docker build --build-arg BUILDKIT_INLINE_CACHE=1 -t my-airflow:latest .
```

### Comparison: pip vs uv Docker Builds

| Metric          | pip               | uv       |
| --------------- | ----------------- | -------- |
| Build time      | 45-90s            | 5-15s    |
| Layer cache hit | Often invalidated | Reliable |
| Image size      | Similar           | Similar  |

---

## 7. Testing Setup

A proper testing setup catches DAG issues before deployment.

### pytest Configuration in pyproject.toml

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_functions = ["test_*"]
addopts = [
    "-v",
    "--tb=short",
    "-ra",
]
filterwarnings = [
    "ignore::DeprecationWarning",
]

[tool.coverage.run]
source = ["dags", "plugins"]
omit = ["tests/*"]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "if TYPE_CHECKING:",
]
```

### Conftest for Airflow Testing

```python
# tests/conftest.py
"""Pytest fixtures for Airflow DAG testing."""

import os
from pathlib import Path

import pytest

# Set Airflow home before importing airflow
os.environ.setdefault("AIRFLOW_HOME", str(Path(__file__).parent.parent))


@pytest.fixture(scope="session")
def dags_folder() -> Path:
    """Return path to DAGs folder."""
    return Path(__file__).parent.parent / "dags"


@pytest.fixture(scope="session")
def dag_files(dags_folder: Path) -> list[Path]:
    """Return list of all DAG files."""
    return list(dags_folder.rglob("*.py"))
```

### DAG Integrity Tests

```python
# tests/test_dag_integrity.py
"""Tests for DAG integrity and validity."""

import importlib.util
from pathlib import Path

import pytest


def test_dags_can_be_imported(dag_files: list[Path]) -> None:
    """Verify all DAG files can be imported without errors."""
    for dag_file in dag_files:
        spec = importlib.util.spec_from_file_location(dag_file.stem, dag_file)
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            pytest.fail(f"Failed to import {dag_file}: {e}")


def test_no_import_errors(dag_files: list[Path]) -> None:
    """Check DAGs don't have import errors."""
    from airflow.models import DagBag

    dag_bag = DagBag(include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"


def test_no_cycles(dag_files: list[Path]) -> None:
    """Verify DAGs don't have circular dependencies."""
    from airflow.models import DagBag

    dag_bag = DagBag(include_examples=False)
    for dag_id, dag in dag_bag.dags.items():
        # This will raise if there are cycles
        _ = dag.topological_sort()
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=dags --cov-report=term-missing

# Run specific test file
uv run pytest tests/test_dag_integrity.py -v
```

---

## 8. Putting It All Together

### Quick Reference Commands

```bash
# Project setup
uv init my-project && cd my-project
uv add apache-airflow
uv add --dev ruff pre-commit pytest pytest-cov
uv venv && uv sync --all-groups
uv run pre-commit install

# Daily workflow
uv run ruff check --fix .     # Lint and fix
uv run ruff format .          # Format
uv run pytest                 # Test
git commit -m "feature"       # Pre-commit runs automatically

# Docker
docker build -t my-airflow .
docker run -p 8080:8080 my-airflow

# Dependency management
uv add package               # Add dependency
uv add --dev package         # Add dev dependency
uv remove package            # Remove dependency
uv sync                      # Install from lock file
uv lock --upgrade            # Update all dependencies
```

### Complete Project Template

```
my-airflow-project/
├── pyproject.toml           # Project configuration
├── uv.lock                  # Lock file (auto-generated)
├── .pre-commit-config.yaml  # Pre-commit hooks
├── .gitignore
├── README.md
├── Dockerfile
├── docker-compose.yaml
├── dags/
│   ├── __init__.py
│   └── example_dag.py
├── plugins/
│   └── __init__.py
└── tests/
    ├── __init__.py
    ├── conftest.py
    └── test_dag_integrity.py
```

### GitHub Actions CI/CD Example

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  lint-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up uv
        uses: astral-sh/setup-uv@v4

      - name: Install dependencies
        run: uv sync --all-groups

      - name: Run linting
        run: uv run ruff check .

      - name: Check formatting
        run: uv run ruff format --check .

      - name: Run tests
        run: uv run pytest --cov=dags

  docker:
    runs-on: ubuntu-latest
    needs: lint-and-test
    steps:
      - uses: actions/checkout@v4

      - name: Build Docker image
        run: docker build -t airflow-test .
```

---

## 📝 Exercises

Complete these exercises to set up your own development environment:

1. **[Exercise 0.1](exercises/exercise_0_1.md)**: Install uv and initialize an Airflow project
2. **[Exercise 0.2](exercises/exercise_0_2.md)**: Configure pyproject.toml with dependency groups
3. **[Exercise 0.3](exercises/exercise_0_3.md)**: Set up ruff and pre-commit hooks
4. **[Exercise 0.4](exercises/exercise_0_4.md)**: Build an optimized Airflow Docker image
5. **[Exercise 0.5](exercises/exercise_0_5.md)**: Create a pytest testing workflow

---

## ✅ Checkpoint

Before moving to Module 01, ensure you can:

- [ ] Create a new project with `uv init` and add dependencies
- [ ] Understand the structure of pyproject.toml
- [ ] Run ruff to lint and format your code
- [ ] Have pre-commit hooks running on every commit
- [ ] Build a Docker image with uv
- [ ] Run pytest to validate DAGs

### Verification Commands

Run these commands to verify your environment is properly configured:

```bash
# 1. Verify uv installation
uv --version
# Expected: uv 0.5.x or higher

# 2. Verify project structure
ls pyproject.toml uv.lock
# Expected: Both files should exist

# 3. Verify Airflow installation
uv run python -c "import airflow; print(f'Airflow {airflow.__version__}')"
# Expected: Airflow 3.x.x

# 4. Verify ruff works
uv run ruff check --version
# Expected: ruff 0.8.x

# 5. Verify pre-commit hooks
test -f .git/hooks/pre-commit && echo "Pre-commit installed" || echo "Not installed"
# Expected: Pre-commit installed

# 6. Verify Docker build
docker images | grep airflow-learning
# Expected: airflow-learning image listed

# 7. Verify tests pass
uv run pytest tests/ -q
# Expected: All tests passed
```

If any command fails, revisit the corresponding exercise before proceeding.

---

## 📚 Further Reading

- [uv Documentation](https://docs.astral.sh/uv/)
- [Ruff Documentation](https://docs.astral.sh/ruff/)
- [PEP 621 - pyproject.toml metadata](https://peps.python.org/pep-0621/)
- [Pre-commit Documentation](https://pre-commit.com/)
- [Airflow Docker Best Practices](https://airflow.apache.org/docs/docker-stack/build.html)

---

Next: [Module 01: Foundations →](../01-foundations/README.md)
