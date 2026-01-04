"""
Solution 13.1: Connection Types
================================

Complete implementation of connection management including
different connection types, factories, and URI handling.
"""

import json
import logging
from typing import Optional
from urllib.parse import urlparse, parse_qs, unquote, quote_plus, urlencode

from airflow.models import Connection
from airflow.hooks.base import BaseHook
from airflow.utils.session import create_session

logger = logging.getLogger(__name__)


# =============================================================================
# PART 1: CONNECTION MANAGER
# =============================================================================


class ConnectionManager:
    """
    Utility class for managing Airflow connections.

    Provides CRUD operations and connection testing capabilities.
    """

    # Valid connection types
    VALID_CONN_TYPES = {
        "postgres", "mysql", "sqlite", "oracle", "mssql",
        "aws", "google_cloud_platform", "azure",
        "http", "https", "ftp", "sftp", "ssh",
        "slack", "discord", "email",
        "redis", "mongo", "elasticsearch",
    }

    def create_connection(
        self,
        conn_id: str,
        conn_type: str,
        host: str = None,
        login: str = None,
        password: str = None,
        port: int = None,
        schema: str = None,
        extra: dict = None,
        description: str = None,
    ) -> bool:
        """
        Create a new connection in Airflow.

        Args:
            conn_id: Unique connection identifier
            conn_type: Type of connection (postgres, aws, http, etc.)
            host: Hostname or IP address
            login: Username
            password: Password
            port: Port number
            schema: Database name or namespace
            extra: Additional JSON configuration
            description: Human-readable description

        Returns:
            True if created successfully
        """
        try:
            conn = Connection(
                conn_id=conn_id,
                conn_type=conn_type,
                host=host,
                login=login,
                password=password,
                port=port,
                schema=schema,
                extra=json.dumps(extra) if extra else None,
                description=description,
            )

            with create_session() as session:
                # Check if already exists
                existing = session.query(Connection).filter(
                    Connection.conn_id == conn_id
                ).first()

                if existing:
                    logger.warning(f"Connection {conn_id} already exists")
                    return False

                session.add(conn)
                session.commit()
                logger.info(f"Created connection: {conn_id}")
                return True

        except Exception as e:
            logger.error(f"Failed to create connection {conn_id}: {e}")
            return False

    def update_connection(self, conn_id: str, **kwargs) -> bool:
        """
        Update existing connection properties.

        Args:
            conn_id: Connection to update
            **kwargs: Properties to update

        Returns:
            True if updated, False if not found
        """
        try:
            with create_session() as session:
                conn = session.query(Connection).filter(
                    Connection.conn_id == conn_id
                ).first()

                if not conn:
                    logger.warning(f"Connection {conn_id} not found")
                    return False

                # Update provided fields
                for key, value in kwargs.items():
                    if key == "extra" and isinstance(value, dict):
                        value = json.dumps(value)
                    if hasattr(conn, key):
                        setattr(conn, key, value)

                session.commit()
                logger.info(f"Updated connection: {conn_id}")
                return True

        except Exception as e:
            logger.error(f"Failed to update connection {conn_id}: {e}")
            return False

    def delete_connection(self, conn_id: str) -> bool:
        """
        Delete a connection.

        Args:
            conn_id: Connection to delete

        Returns:
            True if deleted, False if not found
        """
        try:
            with create_session() as session:
                result = session.query(Connection).filter(
                    Connection.conn_id == conn_id
                ).delete()

                if result == 0:
                    logger.warning(f"Connection {conn_id} not found")
                    return False

                session.commit()
                logger.info(f"Deleted connection: {conn_id}")
                return True

        except Exception as e:
            logger.error(f"Failed to delete connection {conn_id}: {e}")
            return False

    def get_connection(self, conn_id: str) -> Optional[dict]:
        """
        Get connection details (without password).

        Args:
            conn_id: Connection to retrieve

        Returns:
            Connection dict or None
        """
        try:
            with create_session() as session:
                conn = session.query(Connection).filter(
                    Connection.conn_id == conn_id
                ).first()

                if not conn:
                    return None

                # Parse extra JSON
                extra = {}
                if conn.extra:
                    try:
                        extra = json.loads(conn.extra)
                    except json.JSONDecodeError:
                        extra = {"raw": conn.extra}

                return {
                    "conn_id": conn.conn_id,
                    "conn_type": conn.conn_type,
                    "host": conn.host,
                    "port": conn.port,
                    "schema": conn.schema,
                    "login": conn.login,
                    "description": conn.description,
                    "extra": extra,
                    # Note: password intentionally excluded
                }

        except Exception as e:
            logger.error(f"Failed to get connection {conn_id}: {e}")
            return None

    def test_connection(self, conn_id: str) -> dict:
        """
        Test if a connection works.

        Args:
            conn_id: Connection to test

        Returns:
            Dict with "success" bool and "message" str
        """
        try:
            conn = BaseHook.get_connection(conn_id)

            # Try to get the appropriate hook
            hook = conn.get_hook()

            # Try to test the connection
            if hasattr(hook, "test_connection"):
                result = hook.test_connection()
                return {
                    "success": result[0],
                    "message": result[1] if len(result) > 1 else "Connected",
                }
            else:
                # Basic connectivity check
                return {
                    "success": True,
                    "message": "Hook created successfully (no test method)",
                }

        except Exception as e:
            return {
                "success": False,
                "message": str(e),
            }

    def list_connections(self, conn_type: str = None) -> list:
        """
        List all connections, optionally filtered by type.

        Args:
            conn_type: Filter by connection type

        Returns:
            List of connection dicts
        """
        try:
            with create_session() as session:
                query = session.query(Connection)

                if conn_type:
                    query = query.filter(Connection.conn_type == conn_type)

                connections = []
                for conn in query.all():
                    connections.append({
                        "conn_id": conn.conn_id,
                        "conn_type": conn.conn_type,
                        "host": conn.host,
                        "port": conn.port,
                        "schema": conn.schema,
                        "description": conn.description,
                    })

                return connections

        except Exception as e:
            logger.error(f"Failed to list connections: {e}")
            return []


# =============================================================================
# PART 2: CONNECTION FACTORIES
# =============================================================================


def create_postgres_connection(
    conn_id: str,
    host: str,
    database: str,
    user: str,
    password: str,
    port: int = 5432,
    ssl_mode: str = None,
) -> Connection:
    """
    Create a PostgreSQL connection.

    Args:
        conn_id: Connection identifier
        host: Database host
        database: Database name
        user: Username
        password: Password
        port: Port (default 5432)
        ssl_mode: SSL mode (disable, allow, prefer, require, verify-ca, verify-full)

    Returns:
        Connection object
    """
    extra = {}
    if ssl_mode:
        extra["sslmode"] = ssl_mode

    return Connection(
        conn_id=conn_id,
        conn_type="postgres",
        host=host,
        schema=database,
        login=user,
        password=password,
        port=port,
        extra=json.dumps(extra) if extra else None,
    )


def create_mysql_connection(
    conn_id: str,
    host: str,
    database: str,
    user: str,
    password: str,
    port: int = 3306,
    charset: str = "utf8mb4",
) -> Connection:
    """
    Create a MySQL connection.

    Args:
        conn_id: Connection identifier
        host: Database host
        database: Database name
        user: Username
        password: Password
        port: Port (default 3306)
        charset: Character set (default utf8mb4)

    Returns:
        Connection object
    """
    extra = {"charset": charset}

    return Connection(
        conn_id=conn_id,
        conn_type="mysql",
        host=host,
        schema=database,
        login=user,
        password=password,
        port=port,
        extra=json.dumps(extra),
    )


def create_aws_connection(
    conn_id: str,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    region_name: str = "us-east-1",
    role_arn: str = None,
    session_token: str = None,
) -> Connection:
    """
    Create an AWS connection.

    Args:
        conn_id: Connection identifier
        aws_access_key_id: AWS access key (optional for IAM roles)
        aws_secret_access_key: AWS secret key
        region_name: AWS region
        role_arn: IAM role ARN to assume
        session_token: Session token for temporary credentials

    Returns:
        Connection object
    """
    extra = {"region_name": region_name}

    if role_arn:
        extra["role_arn"] = role_arn
    if session_token:
        extra["aws_session_token"] = session_token

    return Connection(
        conn_id=conn_id,
        conn_type="aws",
        login=aws_access_key_id,
        password=aws_secret_access_key,
        extra=json.dumps(extra),
    )


def create_gcp_connection(
    conn_id: str,
    project_id: str,
    keyfile_dict: dict = None,
    keyfile_path: str = None,
    scopes: list = None,
) -> Connection:
    """
    Create a Google Cloud connection.

    Args:
        conn_id: Connection identifier
        project_id: GCP project ID
        keyfile_dict: Service account key as dict
        keyfile_path: Path to service account key file
        scopes: OAuth scopes

    Returns:
        Connection object
    """
    extra = {"project": project_id}

    if keyfile_dict:
        extra["keyfile_dict"] = keyfile_dict
    if keyfile_path:
        extra["key_path"] = keyfile_path
    if scopes:
        extra["scope"] = scopes

    return Connection(
        conn_id=conn_id,
        conn_type="google_cloud_platform",
        extra=json.dumps(extra),
    )


def create_http_connection(
    conn_id: str,
    host: str,
    headers: dict = None,
    auth_type: str = None,
    token: str = None,
    port: int = None,
) -> Connection:
    """
    Create an HTTP/API connection.

    Args:
        conn_id: Connection identifier
        host: API hostname (without scheme)
        headers: Default headers
        auth_type: Authentication type (basic, bearer, api_key)
        token: Authentication token
        port: Port number

    Returns:
        Connection object
    """
    extra = {}

    if headers:
        extra["headers"] = headers

    # Handle different auth types
    if auth_type == "bearer" and token:
        extra.setdefault("headers", {})["Authorization"] = f"Bearer {token}"
    elif auth_type == "api_key" and token:
        extra["api_key"] = token

    # Determine connection type
    conn_type = "https" if port == 443 else "http"

    return Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        port=port,
        password=token if auth_type == "basic" else None,
        extra=json.dumps(extra) if extra else None,
    )


def create_slack_connection(
    conn_id: str,
    token: str,
    channel: str = None,
) -> Connection:
    """
    Create a Slack connection.

    Args:
        conn_id: Connection identifier
        token: Slack API token
        channel: Default channel

    Returns:
        Connection object
    """
    extra = {}
    if channel:
        extra["channel"] = channel

    return Connection(
        conn_id=conn_id,
        conn_type="slack",
        password=token,
        extra=json.dumps(extra) if extra else None,
    )


# =============================================================================
# PART 3: CONNECTION URI BUILDER
# =============================================================================


class ConnectionURIBuilder:
    """
    Build and parse connection URIs.

    URI format: conn_type://login:password@host:port/schema?extra=value
    """

    def from_uri(self, uri: str) -> Connection:
        """
        Parse URI and create Connection object.

        Args:
            uri: Connection URI string

        Returns:
            Connection object
        """
        parsed = urlparse(uri)

        # Parse extra parameters
        extra = {}
        if parsed.query:
            for key, values in parse_qs(parsed.query).items():
                # Handle JSON values
                value = values[0] if len(values) == 1 else values
                try:
                    extra[key] = json.loads(value)
                except (json.JSONDecodeError, TypeError):
                    extra[key] = value

        return Connection(
            conn_type=parsed.scheme,
            host=parsed.hostname,
            port=parsed.port,
            login=unquote(parsed.username) if parsed.username else None,
            password=unquote(parsed.password) if parsed.password else None,
            schema=parsed.path.lstrip("/") if parsed.path else None,
            extra=json.dumps(extra) if extra else None,
        )

    def to_uri(self, connection: Connection) -> str:
        """
        Convert Connection object to URI string.

        Args:
            connection: Connection object

        Returns:
            URI string
        """
        # Build base URI
        uri = f"{connection.conn_type}://"

        # Add credentials
        if connection.login:
            uri += quote_plus(connection.login)
            if connection.password:
                uri += f":{quote_plus(connection.password)}"
            uri += "@"

        # Add host
        if connection.host:
            uri += connection.host

        # Add port
        if connection.port:
            uri += f":{connection.port}"

        # Add schema/database
        if connection.schema:
            uri += f"/{connection.schema}"

        # Add extra as query parameters
        if connection.extra:
            try:
                extra = json.loads(connection.extra)
                if extra:
                    params = []
                    for key, value in extra.items():
                        if isinstance(value, (dict, list)):
                            value = json.dumps(value)
                        params.append(f"{key}={quote_plus(str(value))}")
                    uri += "?" + "&".join(params)
            except json.JSONDecodeError:
                pass

        return uri

    def validate_uri(self, uri: str) -> tuple:
        """
        Validate URI format.

        Args:
            uri: URI to validate

        Returns:
            Tuple of (is_valid, list of errors)
        """
        errors = []

        try:
            parsed = urlparse(uri)

            # Check scheme (connection type)
            if not parsed.scheme:
                errors.append("Missing connection type (scheme)")
            elif parsed.scheme not in ConnectionManager.VALID_CONN_TYPES:
                errors.append(f"Unknown connection type: {parsed.scheme}")

            # For non-AWS/GCP connections, host is usually required
            if parsed.scheme not in ("aws", "google_cloud_platform", "slack"):
                if not parsed.hostname:
                    errors.append("Missing host")

        except Exception as e:
            errors.append(f"Parse error: {e}")

        return (len(errors) == 0, errors)


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test connection management."""
    print("Connection Types - Solution 13.1")
    print("=" * 60)

    manager = ConnectionManager()
    builder = ConnectionURIBuilder()

    # Test 1: Connection Creation
    print("\n1. CONNECTION CREATION")
    print("-" * 40)

    # Create PostgreSQL connection
    postgres = create_postgres_connection(
        conn_id="test_postgres",
        host="localhost",
        database="airflow",
        user="airflow",
        password="airflow",
        ssl_mode="prefer",
    )
    print(f"   PostgreSQL: {postgres.conn_id} -> {postgres.host}")

    # Create AWS connection
    aws = create_aws_connection(
        conn_id="test_aws",
        aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        region_name="us-west-2",
    )
    print(f"   AWS: {aws.conn_id} -> region {json.loads(aws.extra)['region_name']}")

    # Create HTTP connection
    http = create_http_connection(
        conn_id="test_api",
        host="api.example.com",
        auth_type="bearer",
        token="my-secret-token",
    )
    print(f"   HTTP: {http.conn_id} -> {http.host}")

    # Test 2: URI Builder
    print("\n2. URI BUILDER")
    print("-" * 40)

    # Generate URI from connection
    uri = builder.to_uri(postgres)
    print(f"   Generated URI: {uri[:50]}...")

    # Validate URI
    valid, errors = builder.validate_uri(uri)
    print(f"   Validation: {'PASS' if valid else 'FAIL'}")
    if errors:
        for error in errors:
            print(f"      Error: {error}")

    # Parse URI back
    parsed = builder.from_uri("postgresql://user:pass@localhost:5432/mydb?sslmode=require")
    print(f"   Parsed: {parsed.conn_type}://{parsed.host}:{parsed.port}/{parsed.schema}")

    # Test 3: Connection Manager
    print("\n3. CONNECTION MANAGER")
    print("-" * 40)

    # Note: These operations require Airflow's database
    print("   Manager operations require Airflow database connection.")
    print("   Run within an Airflow environment to test:")
    print("     - manager.create_connection(...)")
    print("     - manager.list_connections()")
    print("     - manager.test_connection(...)")

    print("\n" + "=" * 60)
    print("Test complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
