# Exercise 0.1: Project Initialization with uv

## 🎯 Objective

Install uv and create a new Airflow project from scratch with proper virtual environment management.

## ⏱️ Estimated Time: 15-20 minutes

---

## Prerequisites

- macOS, Linux, or Windows with WSL
- Terminal/command line access
- No prior Python installation required (uv handles this)

---

## Tasks

### Task 1: Install uv

Choose the appropriate installation method for your system:

**macOS/Linux (recommended)**:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows (PowerShell)**:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**Alternative: Via Homebrew (macOS)**:

```bash
brew install uv
```

After installation, restart your terminal and verify:

```bash
uv --version
```

**Expected output**: `uv 0.5.x` or higher

### Task 2: Create a New Project Directory

```bash
# Create and enter project directory
mkdir airflow-learning
cd airflow-learning
```

### Task 3: Initialize the Project with uv

```bash
# Initialize a new Python project
uv init
```

This creates:

- `pyproject.toml` - Project configuration
- `.python-version` - Python version specification
- `README.md` - Project readme
- `hello.py` - Sample Python file

Examine the generated `pyproject.toml`:

```bash
cat pyproject.toml
```

### Task 4: Set Python Version

Ensure you're using a compatible Python version:

```bash
# Set Python version (uv will download if needed)
uv python pin 3.11
```

### Task 5: Add Apache Airflow as a Dependency

```bash
# Add Airflow to the project
uv add apache-airflow
```

Watch the output—notice how fast uv resolves and installs dependencies compared to pip!

### Task 6: Create Virtual Environment and Sync

```bash
# Create virtual environment
uv venv

# Sync dependencies from lock file
uv sync
```

### Task 7: Verify the Installation

```bash
# Run Python in the virtual environment
uv run python -c "import airflow; print(f'Airflow version: {airflow.__version__}')"
```

**Expected output**: `Airflow version: 3.x.x`

### Task 8: Explore the Lock File

Open `uv.lock` and examine its contents:

```bash
head -50 uv.lock
```

Notice how it records:

- Exact versions of all packages
- Package hashes for security
- Platform-specific markers

---

## Verification Checklist

Confirm your project has:

- [ ] `pyproject.toml` with `apache-airflow` in dependencies
- [ ] `uv.lock` file (do not edit manually)
- [ ] `.venv/` directory (virtual environment)
- [ ] `.python-version` file

Your directory structure should look like:

```
airflow-learning/
├── .python-version
├── .venv/
├── README.md
├── hello.py
├── pyproject.toml
└── uv.lock
```

---

## 💡 Key Learnings

1. **uv handles everything**: Python installation, virtual environments, package installation
2. **Speed**: Notice how fast dependency resolution is compared to pip
3. **Lock files**: `uv.lock` ensures reproducible builds across machines
4. **No activation needed**: `uv run` executes commands in the virtual environment without manual activation

---

## 🔧 Troubleshooting

### "command not found: uv"

**Cause**: uv is not in your PATH or shell configuration wasn't reloaded.

**Solution**:

```bash
# Reload your shell configuration
source ~/.bashrc  # or ~/.zshrc for zsh

# Or add manually to PATH
export PATH="$HOME/.local/bin:$PATH"

# Verify
which uv
```

### "Permission denied" during installation

**Cause**: Installation script doesn't have execute permissions.

**Solution**:

```bash
# Use sudo for system-wide installation
curl -LsSf https://astral.sh/uv/install.sh | sudo sh

# Or install to user directory (preferred)
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### "Python 3.11 not found" error

**Cause**: uv needs to download Python but can't.

**Solution**:

```bash
# Let uv download Python
uv python install 3.11

# Or use system Python if available
uv venv --python $(which python3)
```

### "No space left on device"

**Cause**: Insufficient disk space for dependencies.

**Solution**: Free up at least 5GB of disk space. Airflow with dependencies requires significant space.

### Windows-Specific Issues

If using Windows without WSL, some commands may differ:

- Use PowerShell instead of bash
- Paths use backslashes (`\`) instead of forward slashes
- Consider using WSL2 for a more consistent Linux-like experience

---

## 🚀 Bonus Challenge

Try these additional commands to explore uv:

```bash
# List installed packages
uv pip list

# Show package info
uv pip show apache-airflow

# Check for outdated packages
uv pip list --outdated

# Add a development dependency
uv add --dev ipython
```

---

## 📚 Reference

- [uv Documentation](https://docs.astral.sh/uv/)
- [uv Project Management](https://docs.astral.sh/uv/guides/projects/)

---

Next: [Exercise 0.2: Configure pyproject.toml →](exercise_0_2.md)
