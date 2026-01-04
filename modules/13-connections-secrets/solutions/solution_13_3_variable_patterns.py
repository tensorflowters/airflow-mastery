"""
Solution 13.3: Variable Patterns
=================================

Complete implementation of secure and efficient variable
management patterns for Airflow.
"""

import os
import re
import json
import logging
from typing import Any, Optional
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


# =============================================================================
# MOCK VARIABLE CLASS (for standalone testing)
# =============================================================================


class MockVariable:
    """Mock Variable class for testing without Airflow."""

    _store = {}

    @classmethod
    def get(cls, key: str, default_var=None, deserialize_json=False):
        value = cls._store.get(key, default_var)
        if deserialize_json and value and isinstance(value, str):
            return json.loads(value)
        return value

    @classmethod
    def set(cls, key: str, value, serialize_json=False):
        if serialize_json:
            value = json.dumps(value)
        cls._store[key] = value

    @classmethod
    def delete(cls, key: str):
        if key in cls._store:
            del cls._store[key]


# Use mock for standalone testing
Variable = MockVariable


# =============================================================================
# PART 1: CONFIGURATION MANAGER
# =============================================================================


class ConfigManager:
    """
    Centralized configuration management.

    Features:
    - Single point of access for all config
    - Lazy loading with caching
    - Environment-aware defaults
    - Type conversion
    - Dot notation for nested keys

    Example:
        config = ConfigManager("app_config")
        db_host = config.get("database.host", "localhost")
        timeout = config.get("api.timeout", 30, as_type=int)
    """

    def __init__(self, config_key: str = "app_config"):
        """
        Initialize config manager.

        Args:
            config_key: Variable key containing JSON config
        """
        self.config_key = config_key
        self._config: Optional[dict] = None
        self._loaded_at: Optional[datetime] = None

    def get(
        self,
        key: str,
        default: Any = None,
        as_type: type = None,
    ) -> Any:
        """
        Get configuration value with optional type conversion.

        Args:
            key: Config key (supports dot notation like "db.host")
            default: Default value if not found
            as_type: Type to convert to (int, bool, float, str, list)

        Returns:
            Configuration value (converted if as_type specified)
        """
        self._ensure_loaded()

        # Get value using dot notation
        value = self._get_nested(self._config, key)

        if value is None:
            value = default

        # Apply type conversion
        if as_type is not None and value is not None:
            value = self._convert_type(value, as_type)

        return value

    def get_json(self, key: str, default: dict = None) -> dict:
        """
        Get JSON configuration value.

        Args:
            key: Config key
            default: Default dict if not found

        Returns:
            Parsed JSON dict
        """
        value = self.get(key, default)
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return default or {}
        return value if isinstance(value, dict) else (default or {})

    def refresh(self):
        """Refresh cached configuration."""
        self._config = None
        self._loaded_at = None
        self._ensure_loaded()
        logger.info("Configuration refreshed")

    def _ensure_loaded(self):
        """Load config if not cached."""
        if self._config is None:
            self._load_config()

    def _load_config(self):
        """Load configuration from Variable."""
        try:
            self._config = Variable.get(
                self.config_key,
                default_var={},
                deserialize_json=True,
            )
            self._loaded_at = datetime.utcnow()
            logger.debug(f"Loaded config from {self.config_key}")
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            self._config = {}

    def _get_nested(self, data: dict, key: str) -> Any:
        """
        Get nested value using dot notation.

        Example: _get_nested({"db": {"host": "localhost"}}, "db.host")
        Returns: "localhost"
        """
        if not data:
            return None

        keys = key.split(".")
        value = data

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return None

        return value

    def _convert_type(self, value: Any, target_type: type) -> Any:
        """Convert value to target type."""
        if target_type == bool:
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "on")
            return bool(value)
        elif target_type == int:
            return int(value)
        elif target_type == float:
            return float(value)
        elif target_type == str:
            return str(value)
        elif target_type == list:
            if isinstance(value, str):
                return [v.strip() for v in value.split(",")]
            return list(value)
        return value


# =============================================================================
# PART 2: ENVIRONMENT-AWARE CONFIG
# =============================================================================


class EnvironmentConfig:
    """
    Environment-aware configuration.

    Supports:
    - Environment prefixes (DEV_, PROD_, etc.)
    - Default fallbacks
    - Environment inheritance

    Variable naming convention:
    - DEV_DATABASE_HOST for dev environment
    - PROD_DATABASE_HOST for prod
    - DATABASE_HOST as default
    """

    def __init__(self, environment: str = None):
        """
        Initialize with environment.

        Args:
            environment: Environment name (dev, staging, prod)
                        Defaults to AIRFLOW_ENV or 'dev'
        """
        self.environment = (
            environment or
            os.environ.get("AIRFLOW_ENV", "dev")
        ).upper()
        self._cache: dict[str, Any] = {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get value for current environment.

        Lookup order:
        1. {ENV}_{KEY} (environment-specific)
        2. {KEY} (default)
        3. default parameter

        Returns:
            Configuration value
        """
        # Check cache first
        cache_key = f"{self.environment}_{key}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Try environment-specific key first
        env_key = f"{self.environment}_{key}"
        value = Variable.get(env_key)

        if value is None:
            # Try default key
            value = Variable.get(key)

        if value is None:
            value = default

        # Cache the result
        self._cache[cache_key] = value
        return value

    def get_all_for_prefix(self, prefix: str) -> dict:
        """
        Get all variables matching a prefix for current environment.

        Returns:
            Dict of matching key-value pairs
        """
        result = {}

        # In a real implementation, you'd query all variables
        # For this example, we return cached values
        for key, value in self._cache.items():
            if key.startswith(prefix):
                result[key] = value

        return result

    def set_environment(self, environment: str):
        """Change the active environment."""
        self.environment = environment.upper()
        self._cache.clear()

    def clear_cache(self):
        """Clear the configuration cache."""
        self._cache.clear()


# =============================================================================
# PART 3: SECURE VARIABLE ACCESS
# =============================================================================


class SecureVariables:
    """
    Secure variable access with audit logging.

    Features:
    - Access logging for sensitive variables
    - Masking in logs
    - Rate limiting
    - Pattern-based sensitivity detection
    """

    SENSITIVE_PATTERNS = [
        r".*password.*",
        r".*secret.*",
        r".*api_key.*",
        r".*token.*",
        r".*credential.*",
        r".*private.*",
        r".*auth.*",
    ]

    def __init__(self, rate_limit: int = 100):
        """
        Initialize secure access.

        Args:
            rate_limit: Max access per minute per key
        """
        self.rate_limit = rate_limit
        self._access_log: list[dict] = []
        self._access_counts: dict[str, list[datetime]] = defaultdict(list)

    def get_sensitive(self, key: str, caller: str = None) -> Optional[str]:
        """
        Get sensitive variable with audit logging.

        Args:
            key: Variable key
            caller: Identifier of calling code

        Returns:
            Variable value, or None if rate limited
        """
        # Check rate limit
        if not self._check_rate_limit(key):
            logger.warning(f"Rate limit exceeded for key: {key}")
            self._log_access(key, caller, "RATE_LIMITED", None)
            return None

        # Get the value
        value = Variable.get(key)

        # Log access (with masked value)
        masked = self.mask_value(value) if value else None
        self._log_access(key, caller, "SUCCESS" if value else "NOT_FOUND", masked)

        return value

    def mask_value(self, value: str, visible_chars: int = 4) -> str:
        """
        Mask sensitive value for logging.

        Example: "my-secret-password" -> "my-s************"

        Args:
            value: Value to mask
            visible_chars: Number of chars to show

        Returns:
            Masked string
        """
        if not value:
            return ""

        if len(value) <= visible_chars:
            return "*" * len(value)

        return value[:visible_chars] + "*" * (len(value) - visible_chars)

    def is_sensitive(self, key: str) -> bool:
        """
        Check if a key is considered sensitive.

        Returns:
            True if key matches any sensitive pattern
        """
        key_lower = key.lower()
        for pattern in self.SENSITIVE_PATTERNS:
            if re.match(pattern, key_lower):
                return True
        return False

    def get_access_log(self, key: str = None) -> list:
        """
        Get access log entries.

        Args:
            key: Optional filter by key

        Returns:
            List of access log entries
        """
        if key:
            return [
                entry for entry in self._access_log
                if entry.get("key") == key
            ]
        return self._access_log.copy()

    def _check_rate_limit(self, key: str) -> bool:
        """Check if key is within rate limit."""
        now = datetime.utcnow()
        # Remove old entries (older than 1 minute)
        cutoff = now.timestamp() - 60

        self._access_counts[key] = [
            t for t in self._access_counts[key]
            if t.timestamp() > cutoff
        ]

        if len(self._access_counts[key]) >= self.rate_limit:
            return False

        self._access_counts[key].append(now)
        return True

    def _log_access(
        self,
        key: str,
        caller: Optional[str],
        status: str,
        masked_value: Optional[str],
    ):
        """Log an access attempt."""
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "key": key,
            "caller": caller or "unknown",
            "status": status,
            "masked_value": masked_value,
            "is_sensitive": self.is_sensitive(key),
        }
        self._access_log.append(entry)

        # Log to standard logger
        if self.is_sensitive(key):
            logger.info(
                f"Sensitive variable accessed: {key} by {caller or 'unknown'} "
                f"- {status}"
            )


# =============================================================================
# PART 4: VARIABLE VALIDATION
# =============================================================================


class ConfigValidator:
    """
    Validate configuration on startup.

    Checks:
    - Required variables exist
    - Values match expected patterns
    - No deprecated variables in use
    - Type constraints
    """

    def __init__(self):
        """Initialize validator."""
        self._errors: list[str] = []
        self._warnings: list[str] = []

    def validate_required(self, keys: list) -> list:
        """
        Check all required variables exist.

        Args:
            keys: List of required variable keys

        Returns:
            List of missing variable keys
        """
        missing = []
        for key in keys:
            value = Variable.get(key)
            if value is None:
                missing.append(key)
                self._errors.append(f"Missing required variable: {key}")

        return missing

    def validate_format(self, key: str, pattern: str) -> bool:
        """
        Validate value matches regex pattern.

        Args:
            key: Variable key
            pattern: Regex pattern to match

        Returns:
            True if valid
        """
        value = Variable.get(key)
        if value is None:
            self._errors.append(f"Cannot validate format: {key} not found")
            return False

        if not re.match(pattern, str(value)):
            self._errors.append(
                f"Invalid format for {key}: '{value}' "
                f"doesn't match pattern '{pattern}'"
            )
            return False

        return True

    def validate_deprecated(self, deprecated_map: dict) -> list:
        """
        Check for deprecated variable usage.

        Args:
            deprecated_map: {"old_key": "new_key"} mapping

        Returns:
            List of deprecated keys in use
        """
        deprecated_in_use = []
        for old_key, new_key in deprecated_map.items():
            if Variable.get(old_key) is not None:
                deprecated_in_use.append(old_key)
                self._warnings.append(
                    f"Deprecated variable '{old_key}' in use. "
                    f"Migrate to '{new_key}'"
                )

        return deprecated_in_use

    def validate_type(self, key: str, expected_type: type) -> bool:
        """
        Validate variable can be converted to expected type.

        Args:
            key: Variable key
            expected_type: Expected type (int, float, bool, dict, list)

        Returns:
            True if valid
        """
        value = Variable.get(key)
        if value is None:
            return True  # Missing is handled by validate_required

        try:
            if expected_type == dict:
                if isinstance(value, str):
                    json.loads(value)
            elif expected_type == bool:
                if isinstance(value, str) and value.lower() not in (
                    "true", "false", "1", "0", "yes", "no"
                ):
                    raise ValueError("Invalid boolean")
            else:
                expected_type(value)
            return True
        except (ValueError, TypeError, json.JSONDecodeError) as e:
            self._errors.append(
                f"Invalid type for {key}: expected {expected_type.__name__}, "
                f"got error: {e}"
            )
            return False

    def run_all_checks(self, config: dict) -> dict:
        """
        Run all validation checks.

        Args:
            config: Validation configuration dict with:
                - required: list of required keys
                - formats: dict of {key: pattern}
                - deprecated: dict of {old: new}
                - types: dict of {key: type}

        Returns:
            {"valid": bool, "errors": list, "warnings": list}
        """
        self._errors = []
        self._warnings = []

        # Required variables
        if "required" in config:
            self.validate_required(config["required"])

        # Format validation
        if "formats" in config:
            for key, pattern in config["formats"].items():
                self.validate_format(key, pattern)

        # Deprecated variables
        if "deprecated" in config:
            self.validate_deprecated(config["deprecated"])

        # Type validation
        if "types" in config:
            for key, expected_type in config["types"].items():
                self.validate_type(key, expected_type)

        return {
            "valid": len(self._errors) == 0,
            "errors": self._errors.copy(),
            "warnings": self._warnings.copy(),
        }


# =============================================================================
# TESTING
# =============================================================================


def main():
    """Test variable patterns."""
    print("Variable Patterns - Solution 13.3")
    print("=" * 60)

    # Setup test data
    Variable.set("app_config", json.dumps({
        "database": {
            "host": "localhost",
            "port": 5432,
            "name": "airflow",
        },
        "api": {
            "timeout": 30,
            "retries": 3,
            "enabled": True,
        },
        "features": ["auth", "logging", "metrics"],
    }))

    Variable.set("api_key", "secret-key-12345")
    Variable.set("DEV_DATABASE_HOST", "dev-db.internal")
    Variable.set("PROD_DATABASE_HOST", "prod-db.internal")
    Variable.set("DATABASE_HOST", "default-db.internal")
    Variable.set("old_config_key", "deprecated-value")

    # Test 1: Config Manager
    print("\n1. CONFIG MANAGER")
    print("-" * 40)

    config = ConfigManager("app_config")
    print(f"   database.host: {config.get('database.host')}")
    print(f"   database.port: {config.get('database.port', as_type=int)}")
    print(f"   api.enabled: {config.get('api.enabled', as_type=bool)}")
    print(f"   api.timeout: {config.get('api.timeout', 60, as_type=int)}")
    print(f"   missing.key: {config.get('missing.key', 'default')}")

    # Test 2: Environment Config
    print("\n2. ENVIRONMENT CONFIG")
    print("-" * 40)

    env_config = EnvironmentConfig("dev")
    print(f"   Environment: {env_config.environment}")
    print(f"   DATABASE_HOST: {env_config.get('DATABASE_HOST')}")

    env_config.set_environment("prod")
    print(f"   Environment: {env_config.environment}")
    print(f"   DATABASE_HOST: {env_config.get('DATABASE_HOST')}")

    # Test 3: Secure Variables
    print("\n3. SECURE VARIABLES")
    print("-" * 40)

    secure = SecureVariables(rate_limit=5)

    # Check sensitivity
    print(f"   Is 'api_key' sensitive? {secure.is_sensitive('api_key')}")
    print(f"   Is 'database_host' sensitive? {secure.is_sensitive('database_host')}")

    # Get sensitive value
    value = secure.get_sensitive("api_key", caller="test_script")
    print(f"   api_key value: {value}")
    print(f"   Masked value: {secure.mask_value(value)}")

    # Show access log
    log = secure.get_access_log("api_key")
    print(f"   Access log entries: {len(log)}")
    if log:
        print(f"   Last access: {log[-1]}")

    # Test 4: Config Validation
    print("\n4. CONFIG VALIDATION")
    print("-" * 40)

    validator = ConfigValidator()

    # Run validation
    result = validator.run_all_checks({
        "required": ["app_config", "api_key", "missing_var"],
        "formats": {
            "api_key": r"^secret-.*",
        },
        "deprecated": {
            "old_config_key": "new_config_key",
        },
        "types": {
            "api_key": str,
        },
    })

    print(f"   Valid: {result['valid']}")
    print(f"   Errors: {result['errors']}")
    print(f"   Warnings: {result['warnings']}")

    print("\n" + "=" * 60)
    print("Test complete!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
