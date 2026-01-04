# Exercise 0.3: Code Quality Setup (Ruff + Pre-commit)

## 🎯 Objective

Configure ruff for linting and formatting, and set up pre-commit hooks for automated code quality checks.

## ⏱️ Estimated Time: 20-25 minutes

---

## Prerequisites

- Completed Exercise 0.2
- Project with `pyproject.toml` and dev dependencies installed
- Git installed (`git --version`)

---

## Tasks

### Task 1: Initialize Git Repository

If not already initialized:

```bash
cd airflow-learning
git init
```

### Task 2: Configure Ruff in pyproject.toml

Add or update the ruff configuration in your `pyproject.toml`:

```toml
[tool.ruff]
# Match Black's default line length
line-length = 88

# Target Python version
target-version = "py311"

# Exclude common directories
exclude = [
    ".git",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    ".ruff_cache",
]

[tool.ruff.lint]
# Enable these rule sets
select = [
    "E",      # pycodestyle errors
    "W",      # pycodestyle warnings
    "F",      # Pyflakes (unused imports, variables)
    "I",      # isort (import sorting)
    "B",      # flake8-bugbear (common bugs)
    "C4",     # flake8-comprehensions
    "UP",     # pyupgrade (modern Python syntax)
    "SIM",    # flake8-simplify
    "AIR",    # Airflow-specific rules
]

# Rules to ignore
ignore = [
    "E501",   # Line too long (handled by formatter)
]

# Allow autofix for all rules
fixable = ["ALL"]

[tool.ruff.lint.isort]
# Your project's first-party packages
known-first-party = ["dags", "plugins", "tests"]

[tool.ruff.format]
# Use double quotes like Black
quote-style = "double"

# Use spaces for indentation
indent-style = "space"

# Format docstrings
docstring-code-format = true
```

### Task 3: Create a Sample DAG File

Create a DAG file with intentional style issues:

```bash
mkdir -p dags
```

Create `dags/sample_dag.py` with this content (intentionally messy):

```python
"""Sample DAG demonstrating TaskFlow API with ruff linting."""

from datetime import datetime

from airflow.sdk import dag, task

default_args = {
    "owner": "learner",
    "retries": 1,
}


@dag(
    dag_id="sample_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["exercise"],
    description="Sample DAG for Exercise 0.3",
)
def sample_dag():
    """Sample ETL pipeline using TaskFlow API."""

    @task
    def extract():
        """Extract data from source."""
        data = {"users": 100, "events": 500}
        return data

    @task
    def transform(data: dict) -> dict:
        """Transform extracted data."""
        data["processed"] = True
        return data

    @task
    def load(data: dict) -> None:
        """Load data to destination."""
        print(f"Loading: {data}")

    raw = extract()
    transformed = transform(raw)
    load(transformed)


# Instantiate the DAG
sample_dag()
```

### Task 4: Run Ruff Linter

Check for issues:

```bash
uv run ruff check dags/
```

You should see errors about:

- Import sorting (I001)
- Line length (if any lines are too long)
- Unused imports (F401 - json, os are unused)

### Task 5: Auto-fix Issues

Let ruff fix what it can:

```bash
uv run ruff check --fix dags/
```

Check the file again—many issues should be fixed automatically.

### Task 6: Format the Code

```bash
uv run ruff format dags/
```

View the formatted file:

```bash
cat dags/sample_dag.py
```

The code should now be properly formatted with:

- Correct spacing around operators
- Proper line breaks
- Sorted imports

### Task 7: Create Pre-commit Configuration

Create `.pre-commit-config.yaml`:

```yaml
# .pre-commit-config.yaml
repos:
  # Ruff for linting and formatting
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.8.0
    hooks:
      - id: ruff
        name: ruff (lint)
        args: [--fix]
      - id: ruff-format
        name: ruff (format)

  # General file hygiene
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v5.0.0
    hooks:
      - id: trailing-whitespace
        name: trim trailing whitespace
      - id: end-of-file-fixer
        name: fix end of files
      - id: check-yaml
        name: check yaml syntax
        args: [--unsafe] # Allow custom YAML tags (Airflow uses them)
      - id: check-added-large-files
        name: check for large files
        args: [--maxkb=500]
      - id: check-merge-conflict
        name: check for merge conflicts
      - id: check-toml
        name: check toml syntax
```

### Task 8: Install Pre-commit Hooks

```bash
# Ensure pre-commit is installed
uv sync --group dev

# Install hooks into git
uv run pre-commit install
```

Expected output: `pre-commit installed at .git/hooks/pre-commit`

### Task 9: Run Pre-commit on All Files

```bash
uv run pre-commit run --all-files
```

On first run, this will:

1. Download and cache the hook environments
2. Run all hooks on all files
3. Report any issues found

### Task 10: Test the Hook with a Commit

Make a change and try to commit:

```bash
# Add all files
git add .

# Try to commit
git commit -m "Add sample DAG with code quality tools"
```

If pre-commit finds issues, it will:

1. Block the commit
2. Fix what it can automatically
3. Show you what was changed

After fixes, re-add and commit:

```bash
git add .
git commit -m "Add sample DAG with code quality tools"
```

---

## Verification Checklist

- [ ] `[tool.ruff]` configuration in pyproject.toml
- [ ] `.pre-commit-config.yaml` file exists
- [ ] `uv run ruff check .` runs without errors
- [ ] `uv run ruff format --check .` shows no formatting needed
- [ ] `uv run pre-commit run --all-files` passes
- [ ] Git hooks installed (`.git/hooks/pre-commit` exists)

---

## 💡 Key Learnings

1. **Ruff replaces multiple tools**: One tool for linting, formatting, and import sorting
2. **Auto-fix is powerful**: `--fix` automatically corrects many issues
3. **Pre-commit catches issues early**: Before code enters version control
4. **Airflow-specific rules**: AIR rules catch common Airflow mistakes

---

## Airflow-Specific Rules Reference

| Rule   | Description                             | Example                 |
| ------ | --------------------------------------- | ----------------------- |
| AIR001 | Task variable name should match task_id | `extract = extract()` ✓ |
| AIR301 | Avoid deprecated `schedule_interval`    | Use `schedule` instead  |
| AIR302 | Avoid deprecated imports                | Use `airflow.sdk`       |

---

## 🚀 Bonus Challenge

1. Add a security check hook:

   ```yaml
   - repo: https://github.com/Yelp/detect-secrets
     rev: v1.5.0
     hooks:
       - id: detect-secrets
         args: [--baseline, .secrets.baseline]
   ```

2. Create VS Code settings for automatic formatting:

   ```bash
   mkdir -p .vscode
   ```

   Create `.vscode/settings.json`:

   ```json
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

## 📚 Reference

- [Ruff Configuration](https://docs.astral.sh/ruff/configuration/)
- [Ruff Rules](https://docs.astral.sh/ruff/rules/)
- [Pre-commit Hooks](https://pre-commit.com/hooks.html)

---

Next: [Exercise 0.4: Docker Image Build →](exercise_0_4.md)
