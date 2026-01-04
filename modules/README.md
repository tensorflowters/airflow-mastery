# Airflow Mastery Curriculum

Complete Apache Airflow 3.x learning path — from environment setup to production deployment.

---

## Learning Modules

### :seedling: Fundamentals

| Module                                     | Topic                  | Description                                 |
| ------------------------------------------ | ---------------------- | ------------------------------------------- |
| [**00**](00-environment-setup/README.md)   | Environment Setup      | Modern Python tooling with uv, ruff, Docker |
| [**01**](01-foundations/README.md)         | Airflow Foundations    | Core concepts, TaskFlow API, first DAGs     |
| [**02**](02-taskflow-api/README.md)        | TaskFlow API Deep Dive | @dag, @task decorators, XCom                |
| [**03**](03-operators-hooks/README.md)     | Operators & Hooks      | Built-in operators, custom hooks            |
| [**04**](04-scheduling-triggers/README.md) | Scheduling & Triggers  | Cron, timetables, external triggers         |

### :zap: Intermediate

| Module                                   | Topic               | Description                           |
| ---------------------------------------- | ------------------- | ------------------------------------- |
| [**05**](05-assets-data-aware/README.md) | Assets & Data-Aware | Modern Asset-based scheduling         |
| [**06**](06-dynamic-tasks/README.md)     | Dynamic Tasks       | Task mapping, runtime task generation |
| [**07**](07-testing-debugging/README.md) | Testing & Debugging | pytest, DAG validation, debugging     |

### :rocket: Advanced

| Module                                     | Topic               | Description                                  |
| ------------------------------------------ | ------------------- | -------------------------------------------- |
| [**08**](08-kubernetes-executor/README.md) | Kubernetes Executor | K8s pods, resource management, Helm          |
| [**09**](09-production-patterns/README.md) | Production Patterns | Error handling, retry strategies, monitoring |
| [**10**](10-advanced-topics/README.md)     | Advanced Topics     | Custom executors, plugins, optimization      |

### :wrench: Specialized

| Module                                     | Topic                 | Description                        |
| ------------------------------------------ | --------------------- | ---------------------------------- |
| [**11**](11-sensors-deferrable/README.md)  | Sensors & Deferrable  | Efficient waiting, async execution |
| [**12**](12-rest-api/README.md)            | REST API              | Programmatic Airflow control       |
| [**13**](13-connections-secrets/README.md) | Connections & Secrets | Secure credential management       |
| [**14**](14-resource-management/README.md) | Resource Management   | Pools, priorities, SLAs            |
| [**15**](15-ai-ml-orchestration/README.md) | AI/ML Orchestration   | LLM pipelines, embeddings, RAG     |

---

## Recommended Path

1. **Start with Module 00** — Get your environment properly configured
2. **Complete Modules 01-04** — Build solid fundamentals
3. **Progress through 05-07** — Intermediate patterns
4. **Advanced topics 08-10** — Production readiness
5. **Specialized modules** — As needed for your use case

---

## Prerequisites

- Python 3.11+
- Docker Desktop
- Basic command line knowledge
- Familiarity with Python syntax

---

[:arrow_left: Back to Home](../index.md) | [:arrow_right: Start with Module 00](00-environment-setup/README.md)
