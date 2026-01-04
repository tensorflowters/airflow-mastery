# Exercise 13.3: Variable Patterns

## Objective

Implement secure and efficient variable management patterns for configuration management in Airflow.

## Background

Variables in Airflow store configuration that can change between environments or deployments. Proper variable patterns prevent:
- Excessive database queries
- Security vulnerabilities
- Configuration drift
- Hard-to-maintain code

## Requirements

### Part 1: Configuration Manager

Create a centralized configuration manager:

```python
class ConfigManager:
    """
    Centralized configuration management.

    Features:
    - Single point of access for all config
    - Lazy loading with caching
    - Environment-aware defaults
    - Type conversion
    """

    def get(self, key: str, default=None, as_type=None) -> Any:
        """Get config value with type conversion."""
        pass

    def get_json(self, key: str, default=None) -> dict:
        """Get JSON configuration."""
        pass

    def refresh(self):
        """Refresh cached values."""
        pass
```

### Part 2: Environment-Aware Config

Create environment-specific configuration handling:

```python
class EnvironmentConfig:
    """
    Environment-aware configuration.

    Supports:
    - Environment prefixes (DEV_, PROD_, etc.)
    - Default fallbacks
    - Environment inheritance
    """

    def __init__(self, environment: str = None):
        pass

    def get(self, key: str, default=None) -> Any:
        """Get value for current environment."""
        pass
```

### Part 3: Secure Variable Access

Implement secure variable patterns:

```python
class SecureVariables:
    """
    Secure variable access with audit logging.

    Features:
    - Access logging for sensitive variables
    - Masking in logs
    - Rate limiting
    """

    def get_sensitive(self, key: str) -> str:
        """Get sensitive variable with audit log."""
        pass

    def mask_value(self, value: str) -> str:
        """Mask sensitive value for logging."""
        pass
```

### Part 4: Variable Validation

Create a validator for configuration:

```python
class ConfigValidator:
    """
    Validate configuration on startup.

    Checks:
    - Required variables exist
    - Values match expected patterns
    - No deprecated variables in use
    """

    def validate_required(self, keys: list) -> list:
        """Check all required variables exist."""
        pass

    def validate_format(self, key: str, pattern: str) -> bool:
        """Validate value matches regex pattern."""
        pass
```

## Starter Code

See `exercise_13_3_variable_patterns_starter.py`

## Hints

<details>
<summary>Hint 1: Batch variable loading</summary>

```python
# DON'T do this (N database queries)
def bad_get_config():
    return {
        "key1": Variable.get("key1"),
        "key2": Variable.get("key2"),
        "key3": Variable.get("key3"),
    }

# DO this (1 database query)
def good_get_config():
    config = Variable.get("app_config", deserialize_json=True)
    return config  # {"key1": "...", "key2": "...", "key3": "..."}
```

</details>

<details>
<summary>Hint 2: Lazy loading</summary>

```python
class LazyConfig:
    def __init__(self):
        self._config = None

    @property
    def config(self):
        if self._config is None:
            self._config = Variable.get("config", deserialize_json=True)
        return self._config

    def get(self, key):
        return self.config.get(key)
```

</details>

## Success Criteria

- [ ] ConfigManager provides centralized access
- [ ] Environment-aware config works correctly
- [ ] Sensitive access is logged
- [ ] Values are properly masked
- [ ] Validation catches missing variables

---

Next: [Module 14: Resource Management →](../../14-resource-management/README.md)
