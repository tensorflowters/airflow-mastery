# Exercise 13.1: Connection Types

## Objective

Learn to configure and manage different connection types in Airflow including database, cloud, and API connections.

## Background

Airflow connections provide a standardized way to store and retrieve credentials. Understanding connection types and their configuration is essential for integrating with external systems.

## Requirements

### Part 1: Connection Manager Class

Create a utility class for managing connections:

```python
class ConnectionManager:
    def create_connection(self, conn_id: str, conn_type: str, **kwargs) -> bool:
        """Create a new connection."""
        pass

    def update_connection(self, conn_id: str, **kwargs) -> bool:
        """Update existing connection properties."""
        pass

    def delete_connection(self, conn_id: str) -> bool:
        """Delete a connection."""
        pass

    def test_connection(self, conn_id: str) -> dict:
        """Test if a connection works."""
        pass
```

### Part 2: Connection Factories

Create factory functions for common connection types:

```python
def create_postgres_connection(
    conn_id: str,
    host: str,
    database: str,
    user: str,
    password: str,
    port: int = 5432,
    ssl_mode: str = None,
) -> Connection:
    """Create a PostgreSQL connection."""
    pass

def create_aws_connection(
    conn_id: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    region_name: str = "us-east-1",
    role_arn: str = None,
) -> Connection:
    """Create an AWS connection."""
    pass

def create_http_connection(
    conn_id: str,
    host: str,
    headers: dict = None,
    auth_type: str = None,
    token: str = None,
) -> Connection:
    """Create an HTTP/API connection."""
    pass
```

### Part 3: Connection URI Builder

Build connections from URI strings:

```python
class ConnectionURIBuilder:
    def from_uri(self, uri: str) -> Connection:
        """Parse URI and create Connection object."""
        pass

    def to_uri(self, connection: Connection) -> str:
        """Convert Connection object to URI string."""
        pass

    def validate_uri(self, uri: str) -> tuple[bool, list]:
        """Validate URI format and return (valid, errors)."""
        pass
```

## Starter Code

See `exercise_13_1_connection_types_starter.py`

## Hints

<details>
<summary>Hint 1: Connection creation</summary>

```python
from airflow.models import Connection
from airflow.utils.session import create_session

def create_connection(conn_id, conn_type, **kwargs):
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=kwargs.get("host"),
        login=kwargs.get("login"),
        password=kwargs.get("password"),
        port=kwargs.get("port"),
        schema=kwargs.get("schema"),
        extra=json.dumps(kwargs.get("extra", {})),
    )
    with create_session() as session:
        session.add(conn)
        session.commit()
    return True
```

</details>

<details>
<summary>Hint 2: URI parsing</summary>

```python
from urllib.parse import urlparse, parse_qs, unquote

def parse_connection_uri(uri):
    parsed = urlparse(uri)
    return {
        "conn_type": parsed.scheme,
        "host": parsed.hostname,
        "port": parsed.port,
        "login": unquote(parsed.username) if parsed.username else None,
        "password": unquote(parsed.password) if parsed.password else None,
        "schema": parsed.path.lstrip("/") if parsed.path else None,
        "extra": parse_qs(parsed.query),
    }
```

</details>

## Success Criteria

- [ ] ConnectionManager creates connections
- [ ] ConnectionManager updates connections
- [ ] ConnectionManager tests connections
- [ ] Factory functions work for all types
- [ ] URI builder parses and generates URIs
- [ ] Error handling is comprehensive

---

Next: [Exercise 13.2: Secrets Backends →](exercise_13_2_secrets_backends.md)
