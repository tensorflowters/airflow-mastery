# Airflow Mastery - Command Runner
# Usage: just <command>
# Requires: https://github.com/casey/just

# Show available commands
default:
    @just --list

# ============================================
# Student Commands
# ============================================

# One-command quickstart for new students
quickstart:
    @echo "🚀 Starting Airflow Mastery quickstart..."
    ./scripts/quickstart.sh

# ============================================
# Documentation
# ============================================

# Serve docs locally with hot reload (http://localhost:8000)
docs:
    uv run mkdocs serve

# Build static documentation site
docs-build:
    uv run mkdocs build

# Deploy docs to GitHub Pages
docs-deploy:
    uv run mkdocs gh-deploy --force

# ============================================
# Development
# ============================================

# Install all dependencies (dev + docs + test)
install:
    uv sync --all-groups

# Install only dev dependencies
install-dev:
    uv sync --group dev

# Install only docs dependencies
install-docs:
    uv sync --group docs

# Run ruff linter
lint:
    uv run ruff check .

# Run ruff linter with auto-fix
lint-fix:
    uv run ruff check --fix .

# Format code with ruff
format:
    uv run ruff format .

# Check formatting without changes
format-check:
    uv run ruff format --check .

# Run pytest
test:
    uv run pytest tests/ -v

# Run pytest with coverage
test-cov:
    uv run pytest tests/ -v --cov=dags --cov=modules --cov-report=term-missing

# Run all checks (equivalent to CI)
check: lint format-check test

# ============================================
# Airflow (Docker Compose)
# ============================================

# Start local Airflow environment
up:
    docker compose -f infrastructure/docker-compose/docker-compose.yml up -d
    @echo "✅ Airflow starting at http://localhost:8080"
    @echo "   Username: admin | Password: admin"

# Stop local Airflow environment
down:
    docker compose -f infrastructure/docker-compose/docker-compose.yml down

# Stop and remove volumes (clean slate)
down-clean:
    docker compose -f infrastructure/docker-compose/docker-compose.yml down -v

# View Airflow logs
logs:
    docker compose -f infrastructure/docker-compose/docker-compose.yml logs -f

# View specific service logs (usage: just logs-service webserver)
logs-service service:
    docker compose -f infrastructure/docker-compose/docker-compose.yml logs -f {{service}}

# Restart Airflow services
restart:
    docker compose -f infrastructure/docker-compose/docker-compose.yml restart

# Show running containers
ps:
    docker compose -f infrastructure/docker-compose/docker-compose.yml ps

# ============================================
# Utility
# ============================================

# Clean build artifacts and caches
clean:
    rm -rf .ruff_cache .pytest_cache .coverage htmlcov site build dist *.egg-info
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Update all dependencies
update:
    uv lock --upgrade
    uv sync --all-groups

# Run pre-commit on all files
pre-commit:
    uv run pre-commit run --all-files

# Install pre-commit hooks
pre-commit-install:
    uv run pre-commit install
