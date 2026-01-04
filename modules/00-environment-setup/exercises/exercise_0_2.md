# Exercise 0.2: Configure pyproject.toml for Airflow

## 🎯 Objective

Set up a production-ready `pyproject.toml` with proper dependency groups for an Airflow project.

## ⏱️ Estimated Time: 20-25 minutes

---

## Prerequisites

- Completed Exercise 0.1
- uv installed and working
- Project directory with initial `pyproject.toml`

---

## Background

A well-structured `pyproject.toml` separates dependencies into groups:

| Group          | Purpose              | Example Packages         |
| -------------- | -------------------- | ------------------------ |
| `dependencies` | Runtime (production) | apache-airflow           |
| `dev`          | Development tools    | ruff, pre-commit         |
| `test`         | Testing              | pytest, pytest-cov       |
| `providers`    | Airflow providers    | postgres, http providers |

---

## Tasks

### Task 1: Start Fresh or Continue

If continuing from Exercise 0.1:

```bash
cd airflow-learning
```

Or create a new project:

```bash
mkdir airflow-config-exercise
cd airflow-config-exercise
uv init
uv python pin 3.11
```

### Task 2: Edit pyproject.toml

Open `pyproject.toml` in your editor and replace its contents with:

```toml
[project]
name = "airflow-learning"
version = "0.1.0"
description = "Learning Apache Airflow 3.x"
readme = "README.md"
requires-python = ">=3.9,<3.13"
license = { text = "MIT" }
authors = [
    { name = "Your Name", email = "you@example.com" }
]

# Core dependencies - always installed
dependencies = [
    "apache-airflow>=3.0.0,<4.0.0",
]

# Optional dependency groups
[project.optional-dependencies]
# Development tools
dev = [
    "ruff>=0.8.0",
    "pre-commit>=4.0.0",
    "ipython>=8.0.0",
]

# Testing dependencies
test = [
    "pytest>=8.0.0",
    "pytest-cov>=4.0.0",
]

# Common Airflow providers
providers = [
    "apache-airflow-providers-postgres>=5.0.0",
    "apache-airflow-providers-http>=4.0.0",
    "apache-airflow-providers-standard>=0.1.0",
]

# Install all optional dependencies
all = [
    "airflow-learning[dev,test,providers]",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

# Tool configurations will be added in later exercises
[tool.ruff]
line-length = 88
target-version = "py311"

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
```

### Task 3: Update the Lock File

After editing `pyproject.toml`, regenerate the lock file:

```bash
uv lock
```

This resolves all dependencies (including optional groups) and updates `uv.lock`.

### Task 4: Install Core Dependencies Only

```bash
uv sync
```

This installs only the packages in `dependencies` (apache-airflow).

### Task 5: Install Development Dependencies

```bash
uv sync --group dev
```

Verify the dev tools are installed:

```bash
uv run ruff --version
uv run ipython --version
```

### Task 6: Install Testing Dependencies

```bash
uv sync --group test
```

Verify pytest is available:

```bash
uv run pytest --version
```

### Task 7: Install Airflow Providers

```bash
uv sync --group providers
```

Verify a provider is available:

```bash
uv run python -c "from airflow.providers.http.operators.http import HttpOperator; print('HTTP provider loaded!')"
```

### Task 8: Install Everything

To install all dependency groups at once:

```bash
uv sync --all-groups
```

### Task 9: View Installed Packages

```bash
# List all installed packages
uv pip list

# Count packages (should be 50+)
uv pip list | wc -l
```

---

## Understanding Version Constraints

| Syntax           | Meaning               | Example                         |
| ---------------- | --------------------- | ------------------------------- |
| `>=3.0.0`        | At least this version | `>=3.0.0` means 3.0.0 or higher |
| `<4.0.0`         | Below this version    | `<4.0.0` means any 3.x version  |
| `>=3.0.0,<4.0.0` | Range                 | Any 3.x version                 |
| `~=3.0`          | Compatible release    | 3.0.x (patch updates only)      |
| `==3.0.0`        | Exact version         | Only 3.0.0                      |

For Airflow projects, we typically use `>=3.0.0,<4.0.0` to accept any Airflow 3.x version while preventing accidental upgrades to Airflow 4.x (which may have breaking changes).

---

## Verification Checklist

Your `pyproject.toml` should have:

- [ ] Project metadata (name, version, description)
- [ ] Python version constraint (`requires-python`)
- [ ] Core `dependencies` with apache-airflow
- [ ] `dev` optional group with ruff, pre-commit
- [ ] `test` optional group with pytest
- [ ] `providers` optional group with at least one provider
- [ ] `all` group that combines all optional groups

Verify all groups install correctly:

```bash
uv sync --all-groups
uv run python -c "import airflow; import ruff; import pytest; print('All groups installed!')"
```

---

## 💡 Key Learnings

1. **Dependency groups** keep production images lean (no dev/test tools)
2. **Version constraints** prevent unexpected breaking changes
3. **`uv lock`** must be run after changing dependencies
4. **`uv sync`** installs from the lock file for reproducibility

---

## 🚀 Bonus Challenge

Add these additional configurations to your `pyproject.toml`:

1. Add your own custom metadata:

   ```toml
   keywords = ["airflow", "data-engineering", "etl"]
   classifiers = [
       "Development Status :: 3 - Alpha",
       "Intended Audience :: Developers",
       "Programming Language :: Python :: 3.11",
   ]
   ```

2. Add a `docs` dependency group:

   ```toml
   docs = [
       "mkdocs>=1.5.0",
       "mkdocs-material>=9.0.0",
   ]
   ```

3. Update the `all` group to include `docs`

---

## 📚 Reference

- [PEP 621 - pyproject.toml metadata](https://peps.python.org/pep-0621/)
- [uv Dependency Groups](https://docs.astral.sh/uv/concepts/projects/dependencies/)
- [Airflow Installation](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)

---

Next: [Exercise 0.3: Set up ruff and pre-commit →](exercise_0_3.md)
