# Module 13: Connections & Secrets

Master secure credential management in Airflow 3.x including connections, variables, and secrets backends.

## Learning Objectives

By the end of this module, you will:
- Understand Airflow's connection architecture
- Configure connections programmatically and via UI
- Implement secrets backends for production
- Manage variables securely
- Use connection URI format
- Apply best practices for credential management

## Prerequisites

- Module 01: Foundations
- Module 03: Operators & Hooks
- Understanding of environment variables

## Connection Fundamentals

### What Are Connections?

Connections store credentials and settings for external systems:

```python
from airflow.hooks.base import BaseHook

# Get connection from Airflow's metadata database
conn = BaseHook.get_connection("my_postgres")

# Access connection properties
print(conn.host)      # Database host
print(conn.login)     # Username
print(conn.password)  # Password (decrypted)
print(conn.schema)    # Database name
print(conn.port)      # Port number
print(conn.extra)     # JSON string of extra parameters
```

### Connection Properties

| Property | Description | Example |
|----------|-------------|---------|
| `conn_id` | Unique identifier | `"my_postgres"` |
| `conn_type` | Type of connection | `"postgres"`, `"aws"` |
| `host` | Server hostname | `"db.example.com"` |
| `schema` | Database/namespace | `"airflow_db"` |
| `login` | Username | `"admin"` |
| `password` | Password | `"secret123"` |
| `port` | Port number | `5432` |
| `extra` | JSON extra config | `'{"timeout": 30}'` |

### Connection URI Format

Connections can be defined as URIs:

```
<conn_type>://<login>:<password>@<host>:<port>/<schema>?<extra_params>
```

Examples:

```bash
# PostgreSQL
postgresql://user:password@localhost:5432/mydb

# MySQL with SSL
mysql://user:password@host:3306/db?ssl_mode=required

# S3 (with extra JSON)
aws://access_key:secret_key@?region_name=us-east-1

# HTTP with headers
http://@api.example.com?headers={"Authorization":"Bearer token"}
```

### Connection Types

#### Database Connections

```python
# PostgreSQL
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="my_postgres")
df = hook.get_pandas_df("SELECT * FROM users")
```

#### Cloud Provider Connections

```python
# AWS
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

hook = S3Hook(aws_conn_id="aws_default")
hook.load_string("data", bucket_name="mybucket", key="file.txt")
```

```python
# Google Cloud
from airflow.providers.google.cloud.hooks.gcs import GCSHook

hook = GCSHook(gcp_conn_id="google_cloud_default")
hook.upload(bucket_name="mybucket", object_name="file.txt", data="content")
```

#### HTTP Connections

```python
from airflow.providers.http.hooks.http import HttpHook

hook = HttpHook(http_conn_id="my_api", method="GET")
response = hook.run(endpoint="/users")
```

## Programmatic Connection Management

### Creating Connections

```python
from airflow.models import Connection
from airflow.utils.session import create_session
import json

# Create a new connection
new_conn = Connection(
    conn_id="my_new_postgres",
    conn_type="postgres",
    host="db.example.com",
    schema="production",
    login="app_user",
    password="secure_password",
    port=5432,
    extra=json.dumps({
        "sslmode": "require",
        "connect_timeout": 10,
    }),
)

# Add to database
with create_session() as session:
    session.add(new_conn)
    session.commit()
```

### Updating Connections

```python
from airflow.models import Connection
from airflow.utils.session import create_session

with create_session() as session:
    conn = session.query(Connection).filter(
        Connection.conn_id == "my_postgres"
    ).first()

    if conn:
        conn.password = "new_secure_password"
        session.commit()
```

### Deleting Connections

```python
from airflow.models import Connection
from airflow.utils.session import create_session

with create_session() as session:
    session.query(Connection).filter(
        Connection.conn_id == "old_connection"
    ).delete()
    session.commit()
```

## Environment Variable Connections

### AIRFLOW_CONN_ Prefix

Define connections via environment variables:

```bash
# Format: AIRFLOW_CONN_<CONN_ID_UPPERCASE>
export AIRFLOW_CONN_MY_POSTGRES="postgresql://user:pass@host:5432/db"

# With extra parameters
export AIRFLOW_CONN_MY_API="http://@api.example.com?headers=%7B%22Auth%22%3A%22Bearer+token%22%7D"
```

### URL Encoding

Special characters must be URL encoded:

```python
from urllib.parse import quote_plus

password = "p@ss#word!"
encoded = quote_plus(password)
# Result: p%40ss%23word%21
```

### Best Practice: Connection Factory

```python
import os
from urllib.parse import quote_plus

def create_conn_uri(
    conn_type: str,
    host: str,
    login: str,
    password: str,
    port: int = None,
    schema: str = None,
    extra: dict = None,
) -> str:
    """Create a properly formatted connection URI."""
    uri = f"{conn_type}://{quote_plus(login)}:{quote_plus(password)}@{host}"

    if port:
        uri += f":{port}"
    if schema:
        uri += f"/{schema}"
    if extra:
        params = "&".join(f"{k}={quote_plus(str(v))}" for k, v in extra.items())
        uri += f"?{params}"

    return uri
```

## Secrets Backends

### What Is a Secrets Backend?

A secrets backend retrieves credentials from external secret managers instead of Airflow's database:

```
┌─────────────────┐
│   Airflow DAG   │
│                 │
│  get_connection │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Secrets Backend │
│                 │
│ 1. Check cache  │
│ 2. Query backend│
│ 3. Return conn  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────┐
│           External Secrets Manager           │
│                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐     │
│  │ Vault   │  │ AWS SM  │  │ GCP SM  │     │
│  └─────────┘  └─────────┘  └─────────┘     │
└─────────────────────────────────────────────┘
```

### Available Backends

| Backend | Provider | Use Case |
|---------|----------|----------|
| HashiCorp Vault | `apache-airflow-providers-hashicorp` | Self-hosted secrets |
| AWS Secrets Manager | `apache-airflow-providers-amazon` | AWS environments |
| AWS SSM Parameter Store | `apache-airflow-providers-amazon` | AWS with SSM |
| GCP Secret Manager | `apache-airflow-providers-google` | Google Cloud |
| Azure Key Vault | `apache-airflow-providers-microsoft-azure` | Azure environments |

### Configuring Secrets Backend

```ini
# airflow.cfg
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {"connections_path": "airflow/connections", "variables_path": "airflow/variables", "url": "http://vault:8200", "token": "hvs.xxxxx"}
```

Or via environment:

```bash
export AIRFLOW__SECRETS__BACKEND="airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend"
export AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}'
```

### AWS Secrets Manager Backend

```python
# Configuration
backend_kwargs = {
    "connections_prefix": "airflow/connections",
    "variables_prefix": "airflow/variables",
    "profile_name": "default",  # AWS profile
    "full_url_mode": False,     # Store as JSON, not URI
}

# Secret format in AWS Secrets Manager
# Secret name: airflow/connections/my_postgres
# Secret value (JSON):
{
    "conn_type": "postgres",
    "host": "db.example.com",
    "login": "user",
    "password": "secret",
    "port": 5432,
    "schema": "mydb"
}
```

### HashiCorp Vault Backend

```python
# Configuration
backend_kwargs = {
    "connections_path": "secret/airflow/connections",
    "variables_path": "secret/airflow/variables",
    "url": "https://vault.example.com:8200",
    "auth_type": "approle",
    "role_id": "airflow-role",
    "secret_id": "secret-id-here",
}

# Store connection in Vault
# vault kv put secret/airflow/connections/my_postgres \
#   conn_type=postgres host=db.example.com login=user password=secret
```

### GCP Secret Manager Backend

```python
# Configuration
backend_kwargs = {
    "connections_prefix": "airflow-connections",
    "variables_prefix": "airflow-variables",
    "project_id": "my-gcp-project",
    "gcp_keyfile_dict": {"type": "service_account", ...}
}

# Secret name format: airflow-connections-my_postgres
```

### Custom Secrets Backend

```python
from airflow.secrets import BaseSecretsBackend
from typing import Optional
import json

class CustomSecretsBackend(BaseSecretsBackend):
    """Custom secrets backend example."""

    def __init__(self, api_url: str, api_key: str, **kwargs):
        super().__init__(**kwargs)
        self.api_url = api_url
        self.api_key = api_key

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """
        Get connection value (URI format).

        Returns None if connection doesn't exist.
        """
        try:
            # Fetch from your secret store
            response = self._fetch_secret(f"connections/{conn_id}")
            if response:
                return response.get("uri")
        except Exception:
            return None
        return None

    def get_variable(self, key: str) -> Optional[str]:
        """Get variable value."""
        try:
            response = self._fetch_secret(f"variables/{key}")
            return response.get("value") if response else None
        except Exception:
            return None

    def _fetch_secret(self, path: str) -> Optional[dict]:
        """Fetch secret from external API."""
        import requests

        response = requests.get(
            f"{self.api_url}/{path}",
            headers={"Authorization": f"Bearer {self.api_key}"},
        )
        if response.status_code == 200:
            return response.json()
        return None
```

## Variables

### Variable Basics

```python
from airflow.models import Variable

# Get variable (returns None if not found)
value = Variable.get("my_var", default_var=None)

# Get JSON variable
config = Variable.get("my_config", deserialize_json=True)

# Set variable
Variable.set("my_var", "new_value")

# Delete variable
Variable.delete("my_var")
```

### Variables in Templates

```python
# In Jinja templates
bash_command = "echo {{ var.value.my_var }}"

# JSON variable
bash_command = "echo {{ var.json.my_config.key }}"
```

### Variable Best Practices

```python
# DON'T: Multiple Variable.get() calls
def bad_task():
    var1 = Variable.get("var1")  # Database query
    var2 = Variable.get("var2")  # Another query
    var3 = Variable.get("var3")  # Another query

# DO: Single call with JSON
def good_task():
    config = Variable.get("my_config", deserialize_json=True)
    # config = {"var1": "...", "var2": "...", "var3": "..."}
```

### Sensitive Variables

```python
# Mark variables as sensitive in the UI
# They won't be shown in logs or rendered templates

# Or use secrets backend for sensitive variables
# Store in Vault: airflow/variables/api_key
api_key = Variable.get("api_key")  # Retrieved from secrets backend
```

## Connection Encryption

### Fernet Encryption

Airflow encrypts connection passwords using Fernet:

```bash
# Generate Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Set in environment
export AIRFLOW__CORE__FERNET_KEY="your-generated-key"
```

### Key Rotation

```python
from airflow.models import Connection
from airflow.utils.session import create_session

def rotate_fernet_key(old_key: str, new_key: str):
    """Rotate Fernet encryption key."""
    from cryptography.fernet import Fernet, MultiFernet

    # Create MultiFernet with new key first
    f = MultiFernet([Fernet(new_key), Fernet(old_key)])

    with create_session() as session:
        connections = session.query(Connection).all()
        for conn in connections:
            if conn.password:
                # Decrypt with old key, encrypt with new
                decrypted = f.decrypt(conn.password.encode())
                new_fernet = Fernet(new_key)
                conn.password = new_fernet.encrypt(decrypted).decode()
        session.commit()
```

## Best Practices

### Connection Naming

```python
# Use descriptive, hierarchical naming
good_names = [
    "prod_postgres_main",
    "dev_mysql_analytics",
    "aws_s3_data_lake",
    "api_payment_gateway",
]

# Avoid ambiguous names
bad_names = [
    "db1",
    "conn",
    "my_connection",
]
```

### Security Guidelines

1. **Never hardcode credentials** in DAG files
2. **Use secrets backends** in production
3. **Rotate credentials** regularly
4. **Audit connection access** via logs
5. **Limit connection permissions** per role
6. **Encrypt at rest** with Fernet keys

### Testing Connections

```python
from airflow.hooks.base import BaseHook

def test_connection(conn_id: str) -> bool:
    """Test if a connection works."""
    try:
        conn = BaseHook.get_connection(conn_id)
        # Try to use the connection
        hook = conn.get_hook()
        hook.test_connection()
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
```

## Exercises

### Exercise 13.1: Connection Types
Configure different connection types including database, cloud, and API connections.

[Start Exercise 13.1 →](exercises/exercise_13_1_connection_types.md)

### Exercise 13.2: Secrets Backends
Implement a secrets backend for secure credential management.

[Start Exercise 13.2 →](exercises/exercise_13_2_secrets_backends.md)

### Exercise 13.3: Variable Patterns
Implement secure variable patterns for configuration management.

[Start Exercise 13.3 →](exercises/exercise_13_3_variable_patterns.md)

## Key Takeaways

1. **Connections** store credentials for external systems
2. **URI format** provides portable connection definitions
3. **Secrets backends** integrate with enterprise secret managers
4. **Variables** store non-sensitive configuration
5. **Fernet encryption** protects stored passwords
6. **Environment variables** enable secure CI/CD deployment

## Next Steps

Continue to [Module 14: Resource Management](../14-resource-management/README.md) to learn about pools, priorities, and concurrency management.

## Additional Resources

- [Airflow Connections Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Secrets Backend Configuration](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html)
- [Managing Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html)
