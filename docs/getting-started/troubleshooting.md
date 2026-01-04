# Troubleshooting

Common issues and their solutions.

---

## :whale: Docker Issues

### Docker daemon not running

**Error:**

```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock
```

**Solution:**

=== "macOS"

    Start Docker Desktop from Applications, or:
    ```bash
    open -a Docker
    ```

=== "Linux"

    ```bash
    sudo systemctl start docker
    ```

=== "Windows"

    Start Docker Desktop from the Start menu.

---

### Port 8080 already in use

**Error:**

```
Bind for 0.0.0.0:8080 failed: port is already allocated
```

**Solution:**

1. Find what's using the port:

    ```bash
    lsof -i :8080
    # or on Linux
    ss -tlnp | grep 8080
    ```

2. Either stop that service, or modify `docker-compose.yml` to use a different port:
    ```yaml
    ports:
        - "8081:8080" # Use 8081 instead
    ```

---

### Containers keep restarting

**Error:**

```
airflow-scheduler exited with code 1
```

**Solution:**

1. Check the logs:

    ```bash
    just logs
    # or
    docker compose -f infrastructure/docker-compose/docker-compose.yml logs scheduler
    ```

2. Common causes:
    - Database not ready — wait and retry
    - Out of memory — increase Docker memory allocation
    - Invalid DAG — check for Python syntax errors in `dags/`

---

### Out of disk space

**Error:**

```
No space left on device
```

**Solution:**

```bash
# Remove unused Docker resources
docker system prune -a

# Remove all volumes (WARNING: deletes data)
docker volume prune
```

---

## :snake: Python / uv Issues

### uv command not found

**Error:**

```
command not found: uv
```

**Solution:**

1. Reinstall uv:

    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

2. Reload your shell:

    ```bash
    source ~/.bashrc  # or ~/.zshrc
    ```

3. Or add to PATH manually:
    ```bash
    export PATH="$HOME/.local/bin:$PATH"
    ```

---

### Python version mismatch

**Error:**

```
No interpreter found for Python >=3.11,<3.13
```

**Solution:**

Let uv install Python:

```bash
uv python install 3.11
```

---

### Dependency resolution failed

**Error:**

```
error: No solution found when resolving dependencies
```

**Solution:**

1. Clear the cache:

    ```bash
    uv cache clean
    ```

2. Remove lockfile and regenerate:
    ```bash
    rm uv.lock
    uv lock
    uv sync
    ```

---

## :airplane: Airflow Issues

### DAG import errors

**Symptom:** Red error banner in Airflow UI

**Solution:**

1. Check the import errors page in the UI
2. Or via CLI:

    ```bash
    docker compose -f infrastructure/docker-compose/docker-compose.yml exec webserver airflow dags list-import-errors
    ```

3. Common causes:
    - Missing dependencies — add to `pyproject.toml`
    - Syntax errors — run `uv run ruff check dags/`
    - Wrong imports — check Airflow 3.x migration notes

---

### Tasks stuck in queued state

**Symptom:** Tasks never start running

**Solution:**

1. Check scheduler is running:

    ```bash
    just ps
    ```

2. Restart scheduler:

    ```bash
    docker compose -f infrastructure/docker-compose/docker-compose.yml restart scheduler
    ```

3. Check for resource limits in Docker settings

---

### "Broken DAG" after code changes

**Symptom:** DAG worked before, now shows errors

**Solution:**

1. Check for syntax errors:

    ```bash
    uv run ruff check dags/
    ```

2. Validate the DAG:
    ```bash
    docker compose -f infrastructure/docker-compose/docker-compose.yml exec webserver python /opt/airflow/dags/your_dag.py
    ```

---

## :globe_with_meridians: Network Issues

### Cannot access localhost:8080

**Symptom:** Browser shows "connection refused"

**Solution:**

1. Verify containers are running:

    ```bash
    just ps
    ```

2. Check webserver logs:

    ```bash
    docker compose -f infrastructure/docker-compose/docker-compose.yml logs webserver
    ```

3. Ensure port mapping is correct:
    ```bash
    docker compose -f infrastructure/docker-compose/docker-compose.yml port webserver 8080
    ```

---

## :recycle: Reset Everything

If all else fails, start fresh:

```bash
# Stop and remove everything
just down-clean

# Remove Python environment
rm -rf .venv

# Reinstall
just install

# Start fresh
just up
```

---

## :sos: Still Stuck?

- :mag: Search [existing issues](https://github.com/YOUR_ORG/airflow-mastery/issues)
- :bug: [Open a new issue](https://github.com/YOUR_ORG/airflow-mastery/issues/new)
- :speech_balloon: Ask in [Discussions](https://github.com/YOUR_ORG/airflow-mastery/discussions)

When reporting issues, include:

1. Operating system and version
2. Docker version (`docker --version`)
3. uv version (`uv --version`)
4. Full error message
5. Steps to reproduce
