# Exercise 3.2: Custom Operator

## Objective

Create a custom `DataValidationOperator` that validates data files against a set of rules, demonstrating how to build reusable operator classes.

## Requirements

Your custom operator should:
1. Inherit from `BaseOperator`
2. Accept parameters for file path and validation rules
3. Execute validation logic in the `execute()` method
4. Return validation results via XCom
5. Log validation failures appropriately

### DataValidationOperator Specifications

```python
class DataValidationOperator(BaseOperator):
    """
    Validates data against specified rules.

    Parameters:
        file_path: Path to the data file (templatable)
        rules: List of validation rule dictionaries
        fail_on_error: Whether to raise exception on validation failure
    """
```

### Validation Rules Format

```python
rules = [
    {"field": "email", "type": "regex", "pattern": r".*@.*\..*"},
    {"field": "age", "type": "range", "min": 0, "max": 150},
    {"field": "status", "type": "allowed_values", "values": ["active", "inactive"]},
]
```

## Starter Code

Create a file `dags/playground/exercise_3_2_custom_operator.py`:

```python
"""
Exercise 3.2: Custom Operator
=============================
Create a reusable DataValidationOperator.
"""

from datetime import datetime
# TODO: Import necessary modules
# from airflow.sdk import DAG
# from airflow.models import BaseOperator


# TODO: Create the DataValidationOperator class
# class DataValidationOperator(BaseOperator):
#     # Define template_fields for Jinja templating
#     template_fields = ("file_path",)
#
#     def __init__(self, file_path, rules, fail_on_error=False, **kwargs):
#         super().__init__(**kwargs)
#         # Store parameters
#
#     def execute(self, context):
#         # Implement validation logic
#         pass


# TODO: Create a DAG that uses your custom operator
```

## Hints

<details>
<summary>Hint 1: BaseOperator structure</summary>

```python
from airflow.models import BaseOperator

class DataValidationOperator(BaseOperator):
    template_fields = ("file_path",)  # Enable Jinja templating

    def __init__(self, file_path, rules, fail_on_error=False, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.rules = rules
        self.fail_on_error = fail_on_error

    def execute(self, context):
        # Your validation logic here
        return validation_results
```

</details>

<details>
<summary>Hint 2: Validation logic</summary>

```python
def _validate_rule(self, data, rule):
    field = rule["field"]
    value = data.get(field)

    if rule["type"] == "regex":
        import re
        return bool(re.match(rule["pattern"], str(value)))
    elif rule["type"] == "range":
        return rule["min"] <= value <= rule["max"]
    elif rule["type"] == "allowed_values":
        return value in rule["values"]
```

</details>

<details>
<summary>Hint 3: Using the operator in a DAG</summary>

```python
validate_users = DataValidationOperator(
    task_id="validate_users",
    file_path="/data/users.json",
    rules=[
        {"field": "email", "type": "regex", "pattern": r".+@.+\..+"},
        {"field": "age", "type": "range", "min": 18, "max": 120},
    ],
    fail_on_error=False,
)
```

</details>

## Success Criteria

- [ ] Custom operator inherits from BaseOperator correctly
- [ ] Constructor accepts all required parameters
- [ ] template_fields enables Jinja templating for file_path
- [ ] execute() returns validation results dictionary
- [ ] Validation logic handles multiple rule types
- [ ] Logging shows validation progress and failures
- [ ] Operator can be used in a DAG without errors
