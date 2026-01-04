# Design: Module 00 - Development Environment & Modern Python Tooling

**Date**: 2026-01-04
**Status**: Approved
**Author**: Brainstorming Session

---

## Overview

Create a new prerequisite module (Module 00) that teaches modern Python development tooling for Airflow projects. This module sets up learners with a professional development environment before they begin learning Airflow concepts.

## Target Audience

- **Experience Level**: Intermediate Python developers
- **Background**: Familiar with pip/virtualenv basics, unfamiliar with modern tooling (uv, pyproject.toml, ruff)
- **Goal**: Learn modern Python project setup specifically for Airflow development

## Learning Objectives

By the end of this module, learners will:

1. Install and configure **uv** for fast Python package management
2. Create a professional **pyproject.toml** with Airflow dependency groups
3. Set up **ruff** for linting and formatting
4. Configure **pre-commit hooks** for automated code quality
5. Build **optimized Docker images** using uv
6. Establish a **testing workflow** with pytest

## Module Structure

```
modules/00-environment-setup/
├── README.md                    # Main module content (~2500 words)
├── exercises/
│   ├── exercise_0_1.md          # Install uv and create project
│   ├── exercise_0_2.md          # Configure pyproject.toml for Airflow
│   ├── exercise_0_3.md          # Set up ruff and pre-commit
│   ├── exercise_0_4.md          # Build Airflow Docker image with uv
│   └── exercise_0_5.md          # Integrate testing workflow
└── solutions/
    ├── solution_0_1/            # Directory with pyproject.toml template
    ├── solution_0_2.toml        # Complete pyproject.toml
    ├── solution_0_3/            # pre-commit config + ruff.toml
    ├── solution_0_4/            # Dockerfile + docker-compose
    └── solution_0_5/            # pytest setup + example tests
```

## Content Outline

### README.md Sections

1. **Why Modern Python Tooling Matters** (~200 words)
    - Traditional pain points: pip slowness, tool fragmentation
    - The modern stack: uv + ruff + pyproject.toml
    - Speed comparison: uv vs pip (10-100x faster)

2. **Installing and Understanding uv** (~400 words)
    - Installation methods
    - uv vs pip mental model
    - Virtual environment management
    - Lock files explained

3. **Project Configuration with pyproject.toml** (~400 words)
    - Anatomy for Airflow projects
    - Dependency groups: core, dev, test, providers
    - Version constraints

4. **Code Quality with Ruff** (~300 words)
    - One tool replacing flake8, black, isort
    - Airflow-specific rules
    - IDE integration

5. **Pre-commit Hooks** (~250 words)
    - Why pre-commit matters
    - Configuration
    - CI integration

6. **Docker Builds with uv** (~400 words)
    - Multi-stage Dockerfile pattern
    - Layer caching optimization
    - Comparison with traditional pip

7. **Testing Setup** (~300 words)
    - pytest configuration
    - Conftest patterns
    - Coverage

8. **Quick Reference** (~250 words)
    - Command cheat sheet
    - CI/CD example

## Exercises

### Exercise 0.1: Project Initialization with uv

**Objective**: Install uv and create a new Airflow project
**Deliverable**: Working directory with `.venv`, `pyproject.toml`, `uv.lock`

### Exercise 0.2: Configure pyproject.toml for Airflow

**Objective**: Set up production-ready dependency groups
**Deliverable**: Complete pyproject.toml with all groups and providers

### Exercise 0.3: Code Quality Setup

**Objective**: Configure ruff and pre-commit
**Deliverable**: Working pre-commit setup with Airflow-aware rules

### Exercise 0.4: Docker Image Build

**Objective**: Build optimized Airflow Docker image with uv
**Deliverable**: Dockerfile + docker-compose.yaml

### Exercise 0.5: Testing Integration

**Objective**: Set up pytest for DAG validation
**Deliverable**: Working test suite with DAG integrity tests

## Integration Points

### Files to Modify

| File                 | Change                                             |
| -------------------- | -------------------------------------------------- |
| `README.md`          | Add Module 00 to curriculum, update week structure |
| `docs/references.md` | Add uv/ruff/pre-commit resources                   |
| `CHANGELOG.md`       | Document Module 00 addition                        |

### Curriculum Placement

```
Week 0 (NEW): Development Environment Setup
├── Module 00: Modern Python Tooling

Week 1-2: Foundations (existing)
├── Module 01: Foundations
├── Module 02: TaskFlow API
... (unchanged)
```

### Cross-References

Forward references to:

- Module 07: Testing & Debugging (deeper pytest patterns)
- Module 08: Kubernetes Executor (Docker builds for K8s)
- Module 09: Production Patterns (CI/CD integration)

## Technical Decisions

1. **uv over poetry/pipenv**: Fastest, simplest, best Docker support
2. **ruff over black+flake8**: Single tool, faster, better Airflow rules
3. **pyproject.toml only**: No requirements.txt, modern standard
4. **Multi-stage Docker**: Smaller images, better caching

## Success Criteria

- [ ] All 5 exercises complete with solutions
- [ ] README covers all learning objectives
- [ ] Solutions pass ruff checks
- [ ] Docker image builds successfully
- [ ] Tests run with `uv run pytest`

## Estimated Effort

- README.md: ~2500 words, 2-3 hours
- Exercises (5): ~500 words each, 3-4 hours
- Solutions: 4-5 hours
- Integration updates: 1 hour
- **Total**: ~10-12 hours

---

## Approval

**Validated through brainstorming session on 2026-01-04**

Decisions confirmed:

- Module 00 placement (prerequisite)
- Scope: uv + pyproject.toml + ruff + pre-commit + Docker + testing
- Audience: Intermediate Python developers
- Context: Local development + Docker (no K8s in this module)
- Exercises: 5 comprehensive exercises
