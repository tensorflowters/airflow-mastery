# Airflow Mastery - Interactive Learning Project

A comprehensive, hands-on curriculum for mastering Apache Airflow 3.x with a focus on self-hosted Kubernetes deployments.

## 🎯 Learning Objectives

By completing this project, you will be able to:

- Design and implement production-grade DAGs using Airflow 3's TaskFlow API
- Understand the new client-server architecture and Task Execution Interface
- Deploy and operate Airflow on Kubernetes using the official Helm chart
- Implement data-aware scheduling with Assets (formerly Datasets)
- Write testable, maintainable DAG code following best practices
- Debug, monitor, and optimize Airflow in production environments
- Handle complex orchestration patterns: dynamic tasks, branching, sensors

## 📚 Curriculum Structure

```
Module 00: Environment Setup            ████░░░░░░  Week 0 (Prerequisite) 🆕
Module 01: Foundations                  ████░░░░░░  Week 1
Module 02: TaskFlow API                 ████░░░░░░  Week 1-2
Module 03: Operators & Hooks            ████░░░░░░  Week 2
Module 04: Scheduling & Triggers        ████░░░░░░  Week 2-3
Module 05: Assets & Data-Aware          ████░░░░░░  Week 3
Module 06: Dynamic Task Mapping         ████░░░░░░  Week 3-4
Module 07: Testing & Debugging          ████░░░░░░  Week 4
Module 08: Kubernetes Executor          ████░░░░░░  Week 4-5
Module 09: Production Patterns          ████░░░░░░  Week 5-6
Module 10: Advanced Topics              ████░░░░░░  Week 6
Module 11: Sensors & Deferrable         ████░░░░░░  Week 7
Module 12: REST API                     ████░░░░░░  Week 7
Module 13: Connections & Secrets        ████░░░░░░  Week 8
Module 14: Resource Management          ████░░░░░░  Week 8
Module 15: AI/ML Orchestration          ████░░░░░░  Week 9
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Kubernetes cluster (minikube, kind, or production cluster)
- kubectl & helm CLI tools
- Python 3.9+
- Basic understanding of Python decorators and context managers

### Local Development Setup

```bash
# Clone and enter the project
cd ~/Workspace/airflow-mastery

# Start local Airflow environment
cd infrastructure/docker-compose
docker compose up -d

# Access the UI at http://localhost:8080
# Default credentials: airflow / airflow

# Run your first DAG
cp ../../dags/examples/01_hello_airflow.py ../../dags/playground/
```

### Kubernetes Deployment (After Module 08)

```bash
cd infrastructure/helm
./scripts/deploy.sh
```

## 📁 Project Structure

```
airflow-mastery/
├── README.md                           # You are here
├── docs/
│   ├── airflow3-k8s-guide.md          # Comprehensive Airflow 3 + K8s guide
│   └── references.md                   # All learning resources & citations
│
├── modules/                            # Learning modules with exercises
│   ├── 00-environment-setup/           # uv, pyproject.toml, ruff, Docker 🆕
│   ├── 01-foundations/                 # Core concepts, architecture
│   ├── 02-taskflow-api/                # @task decorator, XCom, dependencies
│   ├── 03-operators-hooks/             # Built-in operators, custom operators
│   ├── 04-scheduling-triggers/         # Cron, timetables, data intervals
│   ├── 05-assets-data-aware/           # Assets, @asset decorator, watchers
│   ├── 06-dynamic-tasks/               # expand(), map(), partial()
│   ├── 07-testing-debugging/           # pytest, dag.test(), debugging
│   ├── 08-kubernetes-executor/         # K8s deployment, pod templates
│   ├── 09-production-patterns/         # HA, monitoring, CI/CD
│   ├── 10-advanced-topics/             # Edge Executor, multi-executor, SDK
│   ├── 11-sensors-deferrable/          # Sensors, deferrable operators, triggers
│   ├── 12-rest-api/                    # REST API v2, automation, clients
│   ├── 13-connections-secrets/         # Connections, secrets backends
│   ├── 14-resource-management/         # Pools, priorities, concurrency
│   └── 15-ai-ml-orchestration/         # RAG pipelines, LLM chains, ML workflows 🆕
│
├── infrastructure/
│   ├── docker-compose/                 # Local development environment
│   ├── helm/                           # Kubernetes deployment configs
│   └── scripts/                        # Utility scripts
│
├── dags/
│   ├── examples/                       # Reference implementations
│   └── playground/                     # Your experimentation space
│
└── tests/                              # DAG tests and fixtures
```

## 🎓 How to Use This Project

### 1. Start with Module 00 (Environment Setup)

Before diving into Airflow concepts, set up your modern Python development environment:

- Install **uv** - the fast Python package manager
- Configure **pyproject.toml** with Airflow dependencies
- Set up **ruff** for linting and **pre-commit** hooks
- Build Docker images with uv for fast, reproducible builds

### 2. Continue with Module 01

Each module contains:

- `README.md` - Concept explanations and learning objectives
- `exercises/` - Hands-on tasks to complete
- `solutions/` - Reference implementations (try first!)

### 3. Follow the Progressive Path

Modules build on each other. Complete them in order:

```
Environment → Foundations → TaskFlow → Operators → Scheduling → Assets → Dynamic → Testing → K8s → Production → Advanced
```

### 4. Practice in the Playground

The `dags/playground/` directory is git-ignored. Use it for experimentation without cluttering your examples.

### 5. Validate Your Learning

Each module has "checkpoint" exercises that test your understanding. Complete these before moving on.

## 📈 Progress Tracking

Track your progress by marking modules complete:

- [ ] Module 00: Environment Setup 🆕
- [ ] Module 01: Foundations
- [ ] Module 02: TaskFlow API
- [ ] Module 03: Operators & Hooks
- [ ] Module 04: Scheduling & Triggers
- [ ] Module 05: Assets & Data-Aware Scheduling
- [ ] Module 06: Dynamic Task Mapping
- [ ] Module 07: Testing & Debugging
- [ ] Module 08: Kubernetes Executor
- [ ] Module 09: Production Patterns
- [ ] Module 10: Advanced Topics
- [ ] Module 11: Sensors & Deferrable Operators
- [ ] Module 12: REST API
- [ ] Module 13: Connections & Secrets
- [ ] Module 14: Resource Management
- [ ] Module 15: AI/ML Orchestration 🆕

## 🔧 Development Commands

```bash
# Validate all DAGs
airflow dags list

# Test a specific DAG
airflow dags test <dag_id> <execution_date>

# Run pytest suite
pytest tests/ -v

# Lint DAGs with Ruff (Airflow rules)
ruff check dags/ --select AIR

# Format code
ruff format dags/
```

## 📖 Key Documentation

- [Airflow 3 K8s Deployment Guide](docs/airflow3-k8s-guide.md) - Start here for architecture overview
- [References & Resources](docs/references.md) - Official docs, tutorials, community resources
- [Case Studies](docs/case-studies/) - Real-world production patterns from Spotify, Stripe, Airbnb, and more 🆕

## 🤝 Contributing to Your Learning

This is YOUR learning repository. Feel free to:

- Add notes to module READMEs
- Create additional exercises
- Document patterns you discover
- Build a portfolio of production-ready DAGs

## License

This learning project is for personal educational use.
