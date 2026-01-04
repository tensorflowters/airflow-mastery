# Changelog

All notable changes to the Airflow Mastery curriculum will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-01-04

### Added

#### Curriculum Platform Infrastructure

Complete curriculum platform implementation with documentation, tooling, and automation.

**Documentation Site** (`docs/`, `mkdocs.yml`):

- Added MkDocs Material site configuration with navigation, theming, and extensions
- Created landing page with curriculum overview and feature highlights
- Added getting-started section: quickstart, manual setup, troubleshooting guides
- Added custom CSS for hero sections, feature cards, and admonition styling
- Created abbreviations system for tooltips (DAG, XCom, SDK, etc.)
- Created curriculum index (`modules/README.md`) with learning path overview

**One-Command Quickstart** (`scripts/`):

- `quickstart.sh`: Bash script for macOS/Linux with prerequisite checking, uv installation, Airflow startup, and browser opening
- `quickstart.ps1`: PowerShell script for Windows with equivalent functionality
- Both scripts check for Git, Docker, Docker Compose before proceeding
- Automatic `uv` installation if not present
- Health polling to wait for Airflow webserver before opening browser

**Developer Tooling** (`pyproject.toml`, `justfile`):

- Root `pyproject.toml` with dependency groups: dev, docs, test
- Ruff configuration with security rules ("S", "AIR", "B", "UP")
- 26 justfile recipes including: install, docs, lint, test, up, down, logs
- Pre-commit integration recipe

**CI/CD Automation** (`.github/workflows/`):

- `ci.yml`: Lint, test, DAG validation, and docs build on PRs and pushes
- `docs.yml`: Automatic GitHub Pages deployment on push to main
- Both workflows use uv with caching for fast dependency installation
- Concurrency controls to cancel in-progress runs

### Changed

- Updated project version to 1.2.0
- CI workflow now includes full platform validation

---

## [1.1.1] - 2026-01-04

### Improved

#### Module 00: Exhaustive Quality Enhancement

Comprehensive quality improvements across code, documentation, and security (75+ items addressed).

**Python Code Quality** (`solutions/`):

- `solution_0_5_conftest.py`:
  - Added `from __future__ import annotations` for Python 3.9 compatibility
  - Added `TYPE_CHECKING` pattern for type hints without runtime imports
  - Moved imports to module level (removed imports inside fixtures)
  - Added complete type hints to all fixtures (`-> Path`, `-> list[Path]`, `-> DagBag`)
  - Updated `mock_context` fixture for Airflow 3.x (`logical_date`, `data_interval_start/end`)
- `solution_0_5_test_dags.py`:
  - Added `-> None` return type hints to all test methods
  - Replaced generic `Exception` catches with specific `AirflowException`
  - Fixed O(n²) duplicate detection algorithm to O(n) using set comparison

**Security/Docker Hardening** (`solutions/`):

- `solution_0_4_docker-compose.yaml`:
  - Added `security_opt: no-new-privileges:true` to all Airflow services
  - Added network segmentation (`airflow-backend` with `internal: true`)
  - Added resource limits (`deploy.resources.limits/reservations`)
  - Added comprehensive security warning header with production checklist
- `solution_0_4_Dockerfile`:
  - Added `LABEL` directive with maintainer and description
  - Improved documentation comments for build stages
- `solution_0_3_ruff.toml`:
  - Added "S" (flake8-bandit) security rules
  - Added "PTH" (flake8-use-pathlib) rules
  - Added "T20" (flake8-print) rules
  - Added per-file-ignores for tests and solutions
- `solution_0_3_pre-commit-config.yaml`:
  - Added bandit pre-commit hook with severity filtering
  - Added CI configuration section with fail-fast settings
  - Added baseline creation instructions for detect-secrets

**Documentation Improvements** (`exercises/`, `README.md`):

- `README.md`:
  - Added Prerequisites section with system requirements table
  - Added Verification Commands section (7 checkpoint commands)
- `exercise_0_1.md`:
  - Added comprehensive Troubleshooting section (5 common issues with solutions)
- `exercise_0_3.md`:
  - Fixed DAG import syntax to use `@dag` decorator pattern (Airflow 3.x standard)
  - Updated sample code to match TaskFlow API best practices
- `exercise_0_4.md`:
  - Added Expected Results table with image size info (~800 MB - 1.2 GB)
  - Added build time expectations for first vs subsequent builds
- `exercise_0_5.md`:
  - Added "Common Edge Cases and How Tests Handle Them" table (8 scenarios)
  - Added "Tests You Should Always Have" guidance section

### Technical Notes

- All Python files pass `ruff check` with no warnings
- All Python files formatted with `ruff format`
- Improvements maintain backward compatibility with existing content

---

## [1.1.0] - 2026-01-04

### Added

#### Module 00: Development Environment & Modern Python Tooling (NEW)

A comprehensive prerequisite module teaching modern Python development practices:

- **README.md** (~2500 words) covering:
  - Why modern tooling matters for Airflow development
  - uv package manager installation and usage
  - pyproject.toml configuration with dependency groups
  - Ruff linter/formatter with Airflow-specific rules
  - Pre-commit hooks for automated quality checks
  - Docker image builds with uv
  - pytest integration for DAG testing

- **5 Exercises**:
  - `exercise_0_1.md` - uv installation and project initialization
  - `exercise_0_2.md` - pyproject.toml configuration with dependency groups
  - `exercise_0_3.md` - Ruff and pre-commit setup
  - `exercise_0_4.md` - Docker image build with uv
  - `exercise_0_5.md` - pytest testing integration

- **8 Solution Files**:
  - `solution_0_1_pyproject.toml` - Basic project template
  - `solution_0_2_pyproject.toml` - Complete with all dependency groups
  - `solution_0_3_pre-commit-config.yaml` - Pre-commit configuration
  - `solution_0_3_ruff.toml` - Ruff configuration with Airflow rules
  - `solution_0_4_Dockerfile` - Multi-stage Docker build
  - `solution_0_4_docker-compose.yaml` - Local Airflow development stack
  - `solution_0_5_conftest.py` - pytest fixtures for DAG testing
  - `solution_0_5_test_dags.py` - Comprehensive DAG integrity tests

#### Documentation Updates

- `docs/references.md` - Added Modern Python Tooling section with uv, ruff, pre-commit resources
- `README.md` - Updated curriculum to include Module 00 as prerequisite (Week 0)

---

## [1.0.0] - 2026-01-03

### Added

#### Infrastructure & Security

- `.env.example` file with secure credential patterns for Docker Compose setup
- `values-production.yaml` Helm configuration for production deployments
- Fernet key generation documentation and security guidance
- Security warnings throughout infrastructure documentation
- External PostgreSQL configuration templates for production

#### Modules & Exercises

- Module 08 (Kubernetes Executor):
  - `exercise_8_1_helm_deployment_starter.py` - Helm deployment configuration
  - `exercise_8_3_resource_management_starter.py` - Resource quota management
  - `solution_8_1_helm_deployment.py` - Complete Helm deployment solution
  - `solution_8_3_resource_management.py` - Resource management solution
- Module 09 (Production Patterns):
  - `exercise_9_3_monitoring_dag_starter.py` - Monitoring DAG implementation

#### Test Suite

- `tests/test_solutions.py` - Comprehensive parametrized tests for all 40+ solutions
  - DAG parsing validation
  - SDK import verification
  - @dag decorator usage
  - Tag and description requirements
  - Return type hint validation
- `tests/test_taskflow_patterns.py` - TaskFlow API pattern tests
  - XCom serialization patterns
  - Multiple outputs handling
  - Context parameter usage
  - Dynamic task mapping
  - Asset patterns
- `tests/test_module_exercises.py` - Exercise structure validation
  - TODO section verification
  - Docstring requirements
  - Exercise/solution pairing
- `tests/test_integration.py` - Integration tests with pytest markers
  - DAG.test() method validation
  - XCom integration
  - Dynamic mapping execution

#### Documentation

- `CONTRIBUTING.md` - Comprehensive contribution guidelines
- `CHANGELOG.md` - Project change history

### Changed

#### Infrastructure Updates

- Docker Compose configuration updated to use environment variable substitution
- Pod templates updated from Airflow 3.0.2 to 3.1.5
- Helm values separated into development and production configurations

#### Code Standardization

- Example DAGs converted to `@dag` decorator pattern:
  - `01_hello_airflow.py` - Hello World example
  - `02_taskflow_etl.py` - ETL pipeline example
  - `05_asset_pipeline.py` - Asset scheduling example (5 DAGs)
  - `06_dynamic_task_mapping.py` - Dynamic mapping example
- Standardized imports to `from airflow.sdk import dag, task, Asset`

#### Test Configuration

- `tests/conftest.py` enhanced with:
  - Custom pytest markers (`slow`, `integration`, `airflow`)
  - Command line options (`--run-slow`, `--run-integration`)
  - Additional fixtures for testing

### Security

- Removed hardcoded credentials from Docker Compose configuration
- Added environment variable substitution for sensitive values
- Documented Fernet key generation for encryption
- Added production security checklist to documentation
- Separated development and production Helm configurations

### Educational Notes

The following files intentionally retain the `with DAG()` context manager pattern
for educational purposes, as they demonstrate classic operator usage:

- `03_branching_patterns.py` - BranchPythonOperator
- `04_schedule_examples.py` - Timetable demonstrations
- `07_xcom_patterns.py` - Traditional XCom with operators
- `08_sensors_triggers.py` - Sensor operator patterns
- `09_backfill_catchup.py` - Catchup configuration
- `10_custom_operators.py` - Custom operator development
- `11_error_handling.py` - Exception handling patterns
- `12_connections_hooks.py` - Hook and connection patterns
- `13_security_best_practices.py` - Security implementation

---

## Version History

| Version | Date       | Description                                   |
| ------- | ---------- | --------------------------------------------- |
| 1.1.1   | 2026-01-04 | Module 00: Exhaustive quality improvements    |
| 1.1.0   | 2026-01-04 | Module 00: Modern Python Tooling (uv, ruff)   |
| 1.0.0   | 2026-01-03 | Initial release with comprehensive curriculum |

## Upgrade Guide

### From Pre-release to 1.0.0

1. **Update Infrastructure**:

   ```bash
   cd infrastructure/docker-compose
   cp .env.example .env
   # Edit .env with your credentials
   docker-compose up -d
   ```

2. **Update Kubernetes Deployments**:
   - Use `values-production.yaml` for production
   - Update pod templates to 3.1.5 if using custom templates

3. **Run Tests**:

   ```bash
   # Install test dependencies
   pip install pytest pytest-cov

   # Run all tests
   pytest tests/ -v

   # Run with integration tests
   pytest tests/ -v --run-integration
   ```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing to this project.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
