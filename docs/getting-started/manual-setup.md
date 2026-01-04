# Manual Setup

Prefer to set things up step by step? Follow this guide.

---

## :one: Clone the Repository

```bash
git clone https://github.com/YOUR_ORG/airflow-mastery.git
cd airflow-mastery
```

---

## :two: Install uv (Python Package Manager)

=== "macOS / Linux"

    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

=== "Windows (PowerShell)"

    ```powershell
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

=== "Homebrew (macOS)"

    ```bash
    brew install uv
    ```

Verify installation:

```bash
uv --version
# Expected: uv 0.5.x or higher
```

---

## :three: Install just (Command Runner)

=== "macOS (Homebrew)"

    ```bash
    brew install just
    ```

=== "Linux"

    ```bash
    # Using cargo
    cargo install just

    # Or download binary
    curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to ~/bin
    ```

=== "Windows"

    ```powershell
    # Using scoop
    scoop install just

    # Or using winget
    winget install Casey.Just
    ```

---

## :four: Install Dependencies

```bash
# Install all dependency groups (dev, docs, test)
just install
```

This runs `uv sync --all-groups` under the hood.

---

## :five: Start Airflow

```bash
# Start the Docker Compose stack
just up
```

Wait for the services to be healthy (~1-2 minutes on first run).

Check status:

```bash
just ps
```

You should see:

```
NAME                    STATUS
airflow-postgres        running (healthy)
airflow-webserver       running (healthy)
airflow-scheduler       running (healthy)
```

---

## :six: Access Airflow

Open [http://localhost:8080](http://localhost:8080) in your browser.

- **Username**: `admin`
- **Password**: `admin`

---

## :seven: Start the Documentation (Optional)

To serve the docs locally:

```bash
just docs
```

Open [http://localhost:8000](http://localhost:8000).

---

## :tools: Available Commands

Run `just` to see all available commands:

```bash
just
```

| Command        | Description                  |
| -------------- | ---------------------------- |
| `just install` | Install all dependencies     |
| `just up`      | Start Airflow                |
| `just down`    | Stop Airflow                 |
| `just logs`    | View Airflow logs            |
| `just docs`    | Serve docs locally           |
| `just lint`    | Run linter                   |
| `just test`    | Run tests                    |
| `just check`   | Run all checks (lint + test) |

---

## :next_track_button: Next Steps

1. **Start learning** — [Module 00: Environment Setup](../modules/00-environment-setup/README.md)
2. **Explore the codebase** — Check out the `dags/` and `modules/` directories
3. **Run the sample DAGs** — Unpause them in the Airflow UI

---

## :sos: Having Issues?

See the [Troubleshooting Guide](troubleshooting.md) for common problems and solutions.
