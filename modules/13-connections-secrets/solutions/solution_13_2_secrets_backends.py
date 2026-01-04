"""
Solution 13.2: Secrets Backends
================================

Complete implementation of custom secrets backends for
secure credential management.
"""

import os
import json
import logging
from pathlib import Path
from typing import Optional
from time import time
from urllib.parse import quote_plus

logger = logging.getLogger(__name__)


# =============================================================================
# BASE CLASS
# =============================================================================


class BaseSecretsBackend:
    """
    Base class for secrets backends.

    In actual Airflow: from airflow.secrets import BaseSecretsBackend
    """

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """Get connection value as URI or JSON string."""
        raise NotImplementedError

    def get_variable(self, key: str) -> Optional[str]:
        """Get variable value."""
        raise NotImplementedError

    def get_connection(self, conn_id: str) -> Optional[dict]:
        """Get connection as dict."""
        value = self.get_conn_value(conn_id)
        if value:
            return {"conn_id": conn_id, "value": value}
        return None


# =============================================================================
# PART 1: LOCAL FILE BACKEND
# =============================================================================


class LocalFileSecretsBackend(BaseSecretsBackend):
    """
    Secrets backend that reads from local JSON files.

    Useful for local development and testing without external
    secret managers.

    File structure:
    secrets_dir/
    ├── connections/
    │   ├── my_postgres.json
    │   └── aws_default.json
    └── variables/
        ├── api_key.json
        └── config.json
    """

    def __init__(
        self,
        secrets_dir: str = "secrets",
        connections_prefix: str = "connections",
        variables_prefix: str = "variables",
    ):
        """
        Initialize the backend.

        Args:
            secrets_dir: Root directory for secrets
            connections_prefix: Subdirectory for connections
            variables_prefix: Subdirectory for variables
        """
        self.secrets_dir = Path(secrets_dir)
        self.connections_dir = self.secrets_dir / connections_prefix
        self.variables_dir = self.secrets_dir / variables_prefix

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """
        Get connection value from JSON file.

        Returns:
            Connection URI string, or None if not found
        """
        file_path = self.connections_dir / f"{conn_id}.json"

        if not file_path.exists():
            logger.debug(f"Connection file not found: {file_path}")
            return None

        try:
            with open(file_path) as f:
                conn_data = json.load(f)

            # Check if it's already a URI
            if "uri" in conn_data:
                return conn_data["uri"]

            # Build URI from connection data
            return self._build_uri(conn_data)

        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Error reading connection {conn_id}: {e}")
            return None

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get variable value from JSON file.

        Returns:
            Variable value or None
        """
        file_path = self.variables_dir / f"{key}.json"

        if not file_path.exists():
            logger.debug(f"Variable file not found: {file_path}")
            return None

        try:
            with open(file_path) as f:
                var_data = json.load(f)

            # Return the value field
            if isinstance(var_data, dict):
                return var_data.get("value", json.dumps(var_data))
            return str(var_data)

        except (json.JSONDecodeError, OSError) as e:
            logger.error(f"Error reading variable {key}: {e}")
            return None

    def _build_uri(self, conn_data: dict) -> str:
        """
        Build connection URI from JSON data.

        Args:
            conn_data: Connection configuration dict

        Returns:
            Connection URI string
        """
        conn_type = conn_data.get("conn_type", "generic")
        host = conn_data.get("host", "")
        port = conn_data.get("port")
        login = conn_data.get("login", "")
        password = conn_data.get("password", "")
        schema = conn_data.get("schema", "")

        # Build URI
        uri = f"{conn_type}://"

        if login:
            uri += quote_plus(login)
            if password:
                uri += f":{quote_plus(password)}"
            uri += "@"

        if host:
            uri += host

        if port:
            uri += f":{port}"

        if schema:
            uri += f"/{schema}"

        # Add extra parameters
        extra = conn_data.get("extra", {})
        if extra:
            params = "&".join(
                f"{k}={quote_plus(str(v))}" for k, v in extra.items()
            )
            uri += f"?{params}"

        return uri


# =============================================================================
# PART 2: ENVIRONMENT VARIABLE BACKEND
# =============================================================================


class EnvVarSecretsBackend(BaseSecretsBackend):
    """
    Backend using environment variables with custom prefixes.

    Environment variables:
    - Connections: {conn_prefix}_{CONN_ID} = postgresql://...
    - Variables: {var_prefix}_{KEY} = value

    This is similar to Airflow's built-in environment variable support
    but with configurable prefixes.
    """

    def __init__(
        self,
        conn_prefix: str = "AIRFLOW_CONN",
        var_prefix: str = "AIRFLOW_VAR",
    ):
        """
        Initialize the backend.

        Args:
            conn_prefix: Prefix for connection env vars
            var_prefix: Prefix for variable env vars
        """
        self.conn_prefix = conn_prefix
        self.var_prefix = var_prefix

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """
        Get connection from environment variable.

        Returns:
            Connection URI or None
        """
        # Convert conn_id to environment variable name
        env_key = f"{self.conn_prefix}_{conn_id.upper()}"

        value = os.environ.get(env_key)
        if value:
            logger.debug(f"Found connection in env: {env_key}")
        return value

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get variable from environment variable.

        Returns:
            Variable value or None
        """
        env_key = f"{self.var_prefix}_{key.upper()}"

        value = os.environ.get(env_key)
        if value:
            logger.debug(f"Found variable in env: {env_key}")
        return value

    def list_connections(self) -> list[str]:
        """List all connection IDs found in environment."""
        prefix = f"{self.conn_prefix}_"
        connections = []

        for key in os.environ:
            if key.startswith(prefix):
                conn_id = key[len(prefix):].lower()
                connections.append(conn_id)

        return connections

    def list_variables(self) -> list[str]:
        """List all variable keys found in environment."""
        prefix = f"{self.var_prefix}_"
        variables = []

        for key in os.environ:
            if key.startswith(prefix):
                var_key = key[len(prefix):].lower()
                variables.append(var_key)

        return variables


# =============================================================================
# PART 3: CACHING BACKEND
# =============================================================================


class CachingSecretsBackend(BaseSecretsBackend):
    """
    Wrapper that adds TTL-based caching to any backend.

    This reduces external API calls for backends that fetch
    from remote services (Vault, AWS Secrets Manager, etc.).
    """

    def __init__(
        self,
        backend: BaseSecretsBackend,
        ttl: int = 300,
    ):
        """
        Initialize the caching wrapper.

        Args:
            backend: The underlying secrets backend
            ttl: Cache time-to-live in seconds (default 5 minutes)
        """
        self.backend = backend
        self.ttl = ttl
        self._conn_cache: dict[str, tuple[str, float]] = {}
        self._var_cache: dict[str, tuple[str, float]] = {}
        self._hits = 0
        self._misses = 0

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """
        Get connection with caching.

        Returns:
            Cached or fresh connection value
        """
        # Check cache first
        cached = self._get_cached(self._conn_cache, conn_id)
        if cached is not None:
            self._hits += 1
            logger.debug(f"Cache hit for connection: {conn_id}")
            return cached

        # Cache miss - fetch from backend
        self._misses += 1
        value = self.backend.get_conn_value(conn_id)

        if value is not None:
            self._set_cached(self._conn_cache, conn_id, value)

        return value

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get variable with caching.

        Returns:
            Cached or fresh variable value
        """
        cached = self._get_cached(self._var_cache, key)
        if cached is not None:
            self._hits += 1
            logger.debug(f"Cache hit for variable: {key}")
            return cached

        self._misses += 1
        value = self.backend.get_variable(key)

        if value is not None:
            self._set_cached(self._var_cache, key, value)

        return value

    def _get_cached(self, cache: dict, key: str) -> Optional[str]:
        """Get value from cache if not expired."""
        if key in cache:
            value, timestamp = cache[key]
            if time() - timestamp < self.ttl:
                return value
            # Expired - remove from cache
            del cache[key]
        return None

    def _set_cached(self, cache: dict, key: str, value: str):
        """Store value in cache with timestamp."""
        cache[key] = (value, time())

    def clear_cache(self):
        """Clear all cached values."""
        self._conn_cache.clear()
        self._var_cache.clear()
        logger.info("Cache cleared")

    def invalidate_connection(self, conn_id: str):
        """Invalidate a specific connection from cache."""
        if conn_id in self._conn_cache:
            del self._conn_cache[conn_id]

    def invalidate_variable(self, key: str):
        """Invalidate a specific variable from cache."""
        if key in self._var_cache:
            del self._var_cache[key]

    @property
    def cache_stats(self) -> dict:
        """Get cache statistics."""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0

        return {
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{hit_rate:.1%}",
            "cached_connections": len(self._conn_cache),
            "cached_variables": len(self._var_cache),
        }


# =============================================================================
# PART 4: CHAINED BACKEND
# =============================================================================


class ChainedSecretsBackend(BaseSecretsBackend):
    """
    Try multiple backends in order until one succeeds.

    This enables fallback patterns:
    1. Try Vault (primary)
    2. Try AWS Secrets Manager (secondary)
    3. Try environment variables (fallback)
    """

    def __init__(self, backends: list[BaseSecretsBackend]):
        """
        Initialize with ordered list of backends.

        Args:
            backends: List of backends to try in order
        """
        if not backends:
            raise ValueError("At least one backend is required")
        self.backends = backends

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """
        Get connection from first backend that has it.

        Returns:
            Connection value from first successful backend
        """
        for i, backend in enumerate(self.backends):
            try:
                value = backend.get_conn_value(conn_id)
                if value is not None:
                    logger.debug(
                        f"Found connection {conn_id} in backend {i}: "
                        f"{backend.__class__.__name__}"
                    )
                    return value
            except Exception as e:
                logger.warning(
                    f"Backend {backend.__class__.__name__} error: {e}"
                )
                continue

        logger.debug(f"Connection {conn_id} not found in any backend")
        return None

    def get_variable(self, key: str) -> Optional[str]:
        """
        Get variable from first backend that has it.

        Returns:
            Variable value from first successful backend
        """
        for i, backend in enumerate(self.backends):
            try:
                value = backend.get_variable(key)
                if value is not None:
                    logger.debug(
                        f"Found variable {key} in backend {i}: "
                        f"{backend.__class__.__name__}"
                    )
                    return value
            except Exception as e:
                logger.warning(
                    f"Backend {backend.__class__.__name__} error: {e}"
                )
                continue

        logger.debug(f"Variable {key} not found in any backend")
        return None

    def add_backend(self, backend: BaseSecretsBackend, priority: int = -1):
        """
        Add a backend at specified priority.

        Args:
            backend: Backend to add
            priority: Index to insert at (-1 for end)
        """
        if priority == -1:
            self.backends.append(backend)
        else:
            self.backends.insert(priority, backend)


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test secrets backends."""
    print("Secrets Backends - Solution 13.2")
    print("=" * 60)

    # Test 1: Local File Backend
    print("\n1. LOCAL FILE BACKEND")
    print("-" * 40)

    # Create test directory structure
    test_dir = Path("test_secrets")
    conn_dir = test_dir / "connections"
    var_dir = test_dir / "variables"
    conn_dir.mkdir(parents=True, exist_ok=True)
    var_dir.mkdir(parents=True, exist_ok=True)

    # Create test connection file
    test_conn = {
        "conn_type": "postgres",
        "host": "localhost",
        "port": 5432,
        "login": "user",
        "password": "secret",
        "schema": "mydb",
    }
    with open(conn_dir / "test_postgres.json", "w") as f:
        json.dump(test_conn, f)

    # Create test variable file
    with open(var_dir / "api_key.json", "w") as f:
        json.dump({"value": "secret-api-key-123"}, f)

    # Test the backend
    local_backend = LocalFileSecretsBackend(secrets_dir=str(test_dir))

    conn = local_backend.get_conn_value("test_postgres")
    print(f"   Connection URI: {conn[:50] if conn else 'Not found'}...")

    var = local_backend.get_variable("api_key")
    print(f"   Variable: {var}")

    # Test 2: Environment Variable Backend
    print("\n2. ENVIRONMENT VARIABLE BACKEND")
    print("-" * 40)

    # Set test environment variables
    os.environ["TEST_CONN_MY_POSTGRES"] = "postgresql://user:pass@host:5432/db"
    os.environ["TEST_VAR_SECRET_KEY"] = "my-secret-value"

    env_backend = EnvVarSecretsBackend(
        conn_prefix="TEST_CONN",
        var_prefix="TEST_VAR",
    )

    conn = env_backend.get_conn_value("my_postgres")
    print(f"   Connection: {conn}")

    var = env_backend.get_variable("secret_key")
    print(f"   Variable: {var}")

    print(f"   Found connections: {env_backend.list_connections()}")
    print(f"   Found variables: {env_backend.list_variables()}")

    # Test 3: Caching Backend
    print("\n3. CACHING BACKEND")
    print("-" * 40)

    cached_backend = CachingSecretsBackend(local_backend, ttl=60)

    # First call - cache miss
    conn1 = cached_backend.get_conn_value("test_postgres")
    print(f"   First call (miss): {cached_backend.cache_stats}")

    # Second call - cache hit
    conn2 = cached_backend.get_conn_value("test_postgres")
    print(f"   Second call (hit): {cached_backend.cache_stats}")

    # Test 4: Chained Backend
    print("\n4. CHAINED BACKEND")
    print("-" * 40)

    chained = ChainedSecretsBackend([
        env_backend,     # Try environment first
        local_backend,   # Then local files
    ])

    # This should come from environment
    conn = chained.get_conn_value("my_postgres")
    print(f"   my_postgres (env): {conn[:30] if conn else 'Not found'}...")

    # This should come from local files
    conn = chained.get_conn_value("test_postgres")
    print(f"   test_postgres (local): {conn[:30] if conn else 'Not found'}...")

    # Cleanup test files
    import shutil
    shutil.rmtree(test_dir, ignore_errors=True)

    # Cleanup test env vars
    del os.environ["TEST_CONN_MY_POSTGRES"]
    del os.environ["TEST_VAR_SECRET_KEY"]

    print("\n" + "=" * 60)
    print("Test complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
