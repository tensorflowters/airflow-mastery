# Exercise 0.4: Build Airflow Docker Image with uv

## 🎯 Objective

Build an optimized Airflow Docker image using uv with multi-stage builds and proper layer caching.

## ⏱️ Estimated Time: 25-30 minutes

---

## Prerequisites

- Completed Exercise 0.3
- Docker installed (`docker --version`)
- Docker daemon running

---

## Background

Building Airflow images with uv provides:

- **Faster builds**: 5-15 seconds vs 45-90 seconds with pip
- **Better caching**: Dependency layers cache reliably
- **Smaller images**: Multi-stage builds exclude build tools

---

## Tasks

### Task 1: Prepare Project Structure

Ensure your project has this structure:

```bash
cd airflow-learning

# Create required directories
mkdir -p dags plugins
touch plugins/__init__.py
```

Verify structure:

```
airflow-learning/
├── dags/
│   └── sample_dag.py
├── plugins/
│   └── __init__.py
├── pyproject.toml
└── uv.lock
```

### Task 2: Create the Dockerfile

Create `Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1

# ============================================
# Stage 1: Build environment with uv
# ============================================
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim AS builder

# Configure uv for optimal Docker builds
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Copy dependency files first (for layer caching)
# These files change less frequently than source code
COPY pyproject.toml uv.lock ./

# Install dependencies WITHOUT installing the project
# This layer is cached unless pyproject.toml or uv.lock changes
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-install-project --no-dev

# Copy application source code
COPY dags/ ./dags/
COPY plugins/ ./plugins/

# Install the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# ============================================
# Stage 2: Runtime image (minimal, no uv)
# ============================================
FROM python:3.11-slim-bookworm

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN groupadd --system --gid 1000 airflow \
    && useradd --system --gid 1000 --uid 1000 --create-home airflow

# Copy virtual environment from builder stage
COPY --from=builder --chown=airflow:airflow /app/.venv /app/.venv

# Copy application code
COPY --from=builder --chown=airflow:airflow /app/dags /opt/airflow/dags
COPY --from=builder --chown=airflow:airflow /app/plugins /opt/airflow/plugins

# Set environment variables
ENV PATH="/app/.venv/bin:$PATH" \
    AIRFLOW_HOME=/opt/airflow \
    PYTHONUNBUFFERED=1

# Switch to non-root user
USER airflow
WORKDIR /opt/airflow

# Default command (can be overridden)
CMD ["airflow", "version"]
```

### Task 3: Create docker-compose.yaml

Create `docker-compose.yaml` for local testing:

```yaml
# docker-compose.yaml
services:
  # Database for Airflow metadata
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 5s
      timeout: 5s
      retries: 5

  # Initialize the database
  airflow-init:
    build: .
    depends_on:
      postgres:
        condition: service_healthy
    environment: &airflow-env
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__WEBSERVER__SECRET_KEY: "local-dev-secret-key"
    command: >
      bash -c "
        airflow db migrate &&
        airflow users create \
          --username admin \
          --password admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com || true
      "

  # Airflow API Server (Web UI)
  airflow-webserver:
    build: .
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      <<: *airflow-env
    ports:
      - "8080:8080"
    command: airflow api-server --port 8080
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5

  # Airflow Scheduler
  airflow-scheduler:
    build: .
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    environment:
      <<: *airflow-env
    command: airflow scheduler

volumes:
  postgres_data:
```

### Task 4: Add .dockerignore

Create `.dockerignore` to exclude unnecessary files:

```
# .dockerignore
.git
.gitignore
.venv
__pycache__
*.pyc
*.pyo
.pytest_cache
.ruff_cache
.coverage
htmlcov/
*.egg-info/
dist/
build/
.env
.env.*
*.md
!README.md
tests/
.pre-commit-config.yaml
.vscode/
.idea/
```

### Task 5: Build the Docker Image

```bash
# Build the image
docker build -t airflow-learning:latest .

# Watch the build output - notice:
# 1. First run downloads and caches dependencies
# 2. Subsequent runs are fast if only DAGs change
```

**Expected Results**:

| Metric      | First Build        | Subsequent Builds |
| ----------- | ------------------ | ----------------- |
| Build Time  | 2-5 minutes        | 10-30 seconds     |
| Image Size  | ~800 MB - 1.2 GB   | Same              |
| Layer Cache | Miss (downloading) | Hit (cached)      |

Check your image size:

```bash
docker images airflow-learning:latest --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"
```

### Task 6: Verify the Build

```bash
# Check Airflow version in the image
docker run --rm airflow-learning:latest airflow version

# List installed providers
docker run --rm airflow-learning:latest airflow providers list
```

### Task 7: Test Layer Caching

Make a small change to a DAG file:

```bash
# Edit dags/sample_dag.py - add a comment
echo "# Updated" >> dags/sample_dag.py

# Rebuild - notice how fast it is!
docker build -t airflow-learning:latest .
```

The dependency installation layer should be cached, making the rebuild very fast.

### Task 8: Run the Full Stack

```bash
# Start all services
docker compose up -d

# Watch the logs
docker compose logs -f airflow-webserver

# Wait for "Running on http://0.0.0.0:8080"
```

### Task 9: Access the UI

1. Open http://localhost:8080 in your browser
2. Login with:
   - Username: `admin`
   - Password: `admin`
3. Navigate to DAGs and verify `sample_dag` is visible

### Task 10: Cleanup

```bash
# Stop all services
docker compose down

# Remove volumes (optional, removes database)
docker compose down -v
```

---

## Understanding the Dockerfile

### Why Multi-Stage?

| Stage   | Purpose                      | Included in Final Image |
| ------- | ---------------------------- | ----------------------- |
| Builder | Install dependencies with uv | No                      |
| Runtime | Run Airflow                  | Yes                     |

Benefits:

- Final image doesn't contain uv, build tools, or cache
- Smaller image size
- Faster container startup

### Why Copy Dependencies First?

```dockerfile
# These change rarely
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-install-project

# These change often
COPY dags/ ./dags/
```

Docker caches layers. If `pyproject.toml` and `uv.lock` don't change, the dependency installation layer is cached, making subsequent builds very fast.

---

## Verification Checklist

- [ ] `Dockerfile` created with multi-stage build
- [ ] `docker-compose.yaml` created with postgres, init, webserver, scheduler
- [ ] `.dockerignore` excludes unnecessary files
- [ ] `docker build` completes successfully
- [ ] `docker run airflow-learning:latest airflow version` works
- [ ] `docker compose up` starts all services
- [ ] Web UI accessible at http://localhost:8080
- [ ] DAG visible in the UI

---

## 💡 Key Learnings

1. **Multi-stage builds** separate build and runtime environments
2. **Layer caching** requires ordering COPY commands by change frequency
3. **BuildKit cache mounts** speed up repeated builds
4. **Non-root user** improves security

---

## 🚀 Bonus Challenge

1. Add health checks to the Dockerfile:

   ```dockerfile
   HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
       CMD airflow jobs check || exit 1
   ```

2. Create a development docker-compose override:

   ```yaml
   # docker-compose.override.yaml
   services:
     airflow-webserver:
       volumes:
         - ./dags:/opt/airflow/dags:ro
   ```

   This mounts local DAGs for live development.

3. Add a Makefile for convenience:

   ```makefile
   .PHONY: build up down logs

   build:
   	docker build -t airflow-learning:latest .

   up:
   	docker compose up -d

   down:
   	docker compose down

   logs:
   	docker compose logs -f
   ```

---

## 📚 Reference

- [uv Docker Guide](https://docs.astral.sh/uv/guides/integration/docker/)
- [Airflow Docker Stack](https://airflow.apache.org/docs/docker-stack/index.html)
- [Docker Multi-Stage Builds](https://docs.docker.com/build/building/multi-stage/)

---

Next: [Exercise 0.5: Testing Integration →](exercise_0_5.md)
