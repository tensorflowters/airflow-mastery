# Exercise 13.2: Secrets Backends

## Objective

Implement and configure secrets backends for secure credential management in production Airflow environments.

## Background

Secrets backends allow Airflow to retrieve credentials from external secret managers like AWS Secrets Manager, HashiCorp Vault, or Google Cloud Secret Manager instead of storing them in Airflow's database.

## Requirements

### Part 1: Local Secrets Backend

Create a file-based secrets backend for local development:

```python
class LocalFileSecretsBackend(BaseSecretsBackend):
    """
    Secrets backend that reads from local JSON files.

    File structure:
    secrets/
    ├── connections/
    │   └── my_postgres.json
    └── variables/
        └── api_key.json
    """

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """Get connection URI from file."""
        pass

    def get_variable(self, key: str) -> Optional[str]:
        """Get variable value from file."""
        pass
```

### Part 2: Environment Variable Backend

Create a backend that reads from prefixed environment variables:

```python
class EnvVarSecretsBackend(BaseSecretsBackend):
    """
    Backend using environment variables with custom prefixes.

    Examples:
    AIRFLOW_CONN_MY_POSTGRES=postgresql://...
    AIRFLOW_VAR_API_KEY=secret123
    """

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        pass

    def get_variable(self, key: str) -> Optional[str]:
        pass
```

### Part 3: Caching Secrets Backend

Create a wrapper that adds caching to any secrets backend:

```python
class CachingSecretsBackend(BaseSecretsBackend):
    """
    Wrapper that adds TTL-based caching to reduce
    external API calls.
    """

    def __init__(self, backend: BaseSecretsBackend, ttl: int = 300):
        pass

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """Get with caching."""
        pass
```

### Part 4: Multi-Backend Support

Create a fallback chain of backends:

```python
class ChainedSecretsBackend(BaseSecretsBackend):
    """
    Try multiple backends in order until one succeeds.

    Example:
    1. Try Vault first
    2. Fall back to AWS Secrets Manager
    3. Fall back to environment variables
    """
    pass
```

## Starter Code

See `exercise_13_2_secrets_backends_starter.py`

## Hints

<details>
<summary>Hint 1: BaseSecretsBackend interface</summary>

```python
from airflow.secrets import BaseSecretsBackend

class MyBackend(BaseSecretsBackend):
    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """Return connection as URI string or None."""
        pass

    def get_variable(self, key: str) -> Optional[str]:
        """Return variable value or None."""
        pass
```

</details>

<details>
<summary>Hint 2: TTL caching</summary>

```python
from time import time

class Cache:
    def __init__(self, ttl: int):
        self.ttl = ttl
        self._cache = {}

    def get(self, key: str):
        if key in self._cache:
            value, timestamp = self._cache[key]
            if time() - timestamp < self.ttl:
                return value
            del self._cache[key]
        return None

    def set(self, key: str, value: str):
        self._cache[key] = (value, time())
```

</details>

## Success Criteria

- [ ] Local file backend reads JSON secrets
- [ ] Environment backend uses custom prefixes
- [ ] Caching reduces external calls
- [ ] Chain backend implements fallback
- [ ] All backends handle missing secrets gracefully

---

Next: [Exercise 13.3: Variable Patterns →](exercise_13_3_variable_patterns.md)
