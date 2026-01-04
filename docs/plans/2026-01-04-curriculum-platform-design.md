# Curriculum Platform Design

**Design Date**: 2026-01-04
**Status**: Implemented
**Version**: 1.2.0

---

## Overview

This document describes the design and implementation of the Airflow Mastery curriculum platform, transforming the learning content into a complete, professional educational platform.

### Goals

1. **One-command quickstart** — Students can start learning with a single command
2. **Professional documentation** — MkDocs Material site hosted on GitHub Pages
3. **Unified tooling** — Root pyproject.toml and justfile for all project operations
4. **Automated CI/CD** — Testing and docs deployment via GitHub Actions

---

## Architecture

```
airflow-mastery/
├── docs/                          # MkDocs source
│   ├── index.md                   # Landing page
│   ├── getting-started/           # Quickstart guides
│   │   ├── quickstart.md
│   │   ├── manual-setup.md
│   │   └── troubleshooting.md
│   ├── case-studies/              # Real-world examples
│   ├── modules -> ../modules      # Symlink to curriculum
│   ├── CONTRIBUTING.md -> ../...  # Symlink to contributing
│   ├── includes/                  # MkDocs includes
│   │   └── abbreviations.md
│   └── stylesheets/               # Custom CSS
│       └── extra.css
├── modules/                       # Curriculum content
│   ├── README.md                  # Curriculum index
│   ├── 00-environment-setup/
│   └── 01-15...                   # Learning modules
├── scripts/
│   ├── quickstart.sh              # macOS/Linux installer
│   └── quickstart.ps1             # Windows installer
├── .github/workflows/
│   ├── docs.yml                   # Deploy docs on push
│   └── ci.yml                     # Test on PRs
├── mkdocs.yml                     # MkDocs configuration
├── pyproject.toml                 # Project dependencies
└── justfile                       # Developer commands
```

---

## Components

### 1. Documentation Site (MkDocs Material)

**Configuration** (`mkdocs.yml`):

- Material theme with dark/light mode toggle
- Navigation tabs with sections and expansion
- Code highlighting with line numbers and copy button
- Search with suggestions
- Mermaid diagram support
- pymdownx extensions (superfences, tabbed, details, snippets)

**Content Structure**:

- **Home**: Hero section with quick links
- **Getting Started**: Quickstart, manual setup, troubleshooting
- **Curriculum**: All 16 learning modules
- **Case Studies**: Real-world implementations
- **Reference**: External resources, guides

**Symlink Strategy**:

Modules live outside `docs/` for standalone use. Symlinks provide MkDocs access:

```
docs/modules -> ../modules
docs/CONTRIBUTING.md -> ../CONTRIBUTING.md
```

### 2. Quickstart Scripts

**Bash Script** (`scripts/quickstart.sh`):

```
1. Check prerequisites (git, docker, docker compose)
2. Clone repository to ~/airflow-mastery
3. Install uv if missing
4. Run uv sync
5. Start Airflow with docker compose
6. Wait for health check (poll /health endpoint)
7. Open browser tabs (Airflow UI, docs)
8. Print success message with credentials
```

**PowerShell Script** (`scripts/quickstart.ps1`):

Same flow adapted for Windows, using:

- `Invoke-RestMethod` for health checks
- `Start-Process` for browser opening
- `irm | iex` installation pattern

### 3. Developer Tooling

**pyproject.toml**:

```toml
[dependency-groups]
dev = ["ruff", "pytest", "pytest-cov", "pre-commit"]
docs = ["mkdocs-material", "mkdocs-awesome-pages-plugin", "mkdocs-minify-plugin"]
test = ["pytest", "pytest-cov"]
```

**justfile** (26 recipes):

| Category    | Recipes                            |
| ----------- | ---------------------------------- |
| Student     | quickstart                         |
| Docs        | docs, docs-build, docs-deploy      |
| Development | install, lint, format, test, check |
| Airflow     | up, down, logs, restart, ps        |
| Maintenance | clean, update, pre-commit          |

### 4. CI/CD Workflows

**ci.yml** — Runs on PRs and pushes to main:

1. **lint** — ruff check and format verification
2. **test** — pytest with verbose output
3. **validate-dags** — Import check for all DAG files
4. **docs** — MkDocs build verification

**docs.yml** — Runs on push to main when docs change:

1. Checkout with full history (for git-revision-date)
2. Install docs dependencies
3. Build and deploy with `mkdocs gh-deploy --force`

---

## Design Decisions

### Why Symlinks for Modules?

Modules are standalone learning units that can be used without the docs site. Symlinks allow:

- MkDocs to include them in navigation
- Modules to remain self-contained
- Easy updates without moving files

### Why justfile over Makefile?

- Cleaner syntax without `.PHONY` declarations
- Better cross-platform support (Windows)
- Built-in help with `@just --list`
- Simpler recipe definitions

### Why Separate Dependency Groups?

- `dev`: Minimal for code work
- `docs`: Only for documentation
- `test`: Only for testing
- Reduces install time for focused tasks

### Why Not Strict Mode in CI?

The curriculum content has pre-existing broken links (case studies linking to exercises not yet created). Rather than blocking CI, we:

- Build without `--strict` in CI
- Document the known issues
- Fix incrementally as modules are completed

---

## Usage

### For Students

```bash
# One-command start
curl -fsSL https://raw.githubusercontent.com/YOUR_ORG/airflow-mastery/main/scripts/quickstart.sh | bash
```

### For Developers

```bash
just install      # Install all dependencies
just docs         # Serve docs locally
just up           # Start Airflow
just check        # Run all checks
```

### For Maintainers

```bash
just docs-deploy  # Manual docs deployment
just clean        # Clean build artifacts
just update       # Update dependencies
```

---

## Post-Implementation

- [x] Enable GitHub Pages (Settings → Pages → gh-pages branch)
- [ ] Update README.md with quickstart one-liner
- [ ] Update repository URL placeholders (YOUR_ORG)
- [ ] Create first release tag (v1.2.0)

---

## Files Created

| File                             | Purpose                    |
| -------------------------------- | -------------------------- |
| `pyproject.toml`                 | Root project configuration |
| `justfile`                       | Developer command runner   |
| `mkdocs.yml`                     | Documentation site config  |
| `docs/index.md`                  | Landing page               |
| `docs/getting-started/*.md`      | Setup guides               |
| `docs/includes/abbreviations.md` | Tooltip definitions        |
| `docs/stylesheets/extra.css`     | Custom styling             |
| `modules/README.md`              | Curriculum index           |
| `scripts/quickstart.sh`          | macOS/Linux installer      |
| `scripts/quickstart.ps1`         | Windows installer          |
| `.github/workflows/docs.yml`     | Docs deployment            |
| `.github/workflows/ci.yml`       | CI pipeline                |
