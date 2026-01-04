# Quickstart

Get Airflow Mastery running in under 5 minutes with our one-command setup.

---

## :rocket: One-Command Setup

=== "macOS / Linux"

    ```bash
    curl -fsSL https://raw.githubusercontent.com/YOUR_ORG/airflow-mastery/main/scripts/quickstart.sh | bash
    ```

=== "Windows (PowerShell)"

    ```powershell
    irm https://raw.githubusercontent.com/YOUR_ORG/airflow-mastery/main/scripts/quickstart.ps1 | iex
    ```

---

## :eyes: What the Script Does

The quickstart script automates everything:

| Step | Action                                               | Time     |
| ---- | ---------------------------------------------------- | -------- |
| 1    | Check prerequisites (Git, Docker)                    | ~1s      |
| 2    | Clone repository to `~/airflow-mastery`              | ~5s      |
| 3    | Install [uv](https://docs.astral.sh/uv/) (if needed) | ~3s      |
| 4    | Install Python dependencies                          | ~5s      |
| 5    | Start Airflow with Docker Compose                    | ~60-120s |
| 6    | Wait for Airflow to be healthy                       | ~30s     |
| 7    | Open browser tabs                                    | ~1s      |

**Total time**: ~2-3 minutes (mostly Docker image download on first run)

---

## :computer: What Opens in Your Browser

After the script completes, you'll have three tabs:

1. **Airflow UI** — [http://localhost:8080](http://localhost:8080)
    - Username: `admin`
    - Password: `admin`

2. **Documentation** — This site

3. **First Exercise** — Module 00, Exercise 0.1

---

## :white_check_mark: Verify It's Working

Check these endpoints:

```bash
# Airflow health check
curl http://localhost:8080/health

# Should return: {"metadatabase":{"status":"healthy"},"scheduler":{"status":"healthy"}}
```

In the Airflow UI, you should see:

- :white_check_mark: Green "scheduler" status in the top bar
- :white_check_mark: Sample DAGs listed (may be paused)
- :white_check_mark: No import errors

---

## :warning: Prerequisites

The script checks for these automatically, but ensure you have:

| Requirement    | Minimum Version | Check Command            |
| -------------- | --------------- | ------------------------ |
| Git            | Any             | `git --version`          |
| Docker         | 20.10+          | `docker --version`       |
| Docker Compose | 2.0+            | `docker compose version` |
| 5GB disk space | —               | `df -h`                  |

!!! note "Docker Desktop"
On macOS and Windows, [Docker Desktop](https://www.docker.com/products/docker-desktop/) is the easiest way to get Docker and Docker Compose.

---

## :next_track_button: Next Steps

Once Airflow is running:

1. **Explore the UI** — Familiarize yourself with DAGs, runs, and logs
2. **Start Module 00** — [Exercise 0.1: Project Initialization](../modules/00-environment-setup/exercises/exercise_0_1.md)
3. **Join the community** — [GitHub Discussions](https://github.com/YOUR_ORG/airflow-mastery/discussions)

---

## :sos: Need Help?

- :question: [Troubleshooting Guide](troubleshooting.md)
- :bug: [Report an Issue](https://github.com/YOUR_ORG/airflow-mastery/issues)
- :speech_balloon: [Ask a Question](https://github.com/YOUR_ORG/airflow-mastery/discussions)
