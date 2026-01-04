"""
Solution 3.2: Custom Operator
=============================

Complete solution demonstrating:
- Custom operator inheriting from BaseOperator
- template_fields for Jinja templating
- Multiple validation rule types
- Proper error handling and logging
- XCom result return
"""

from datetime import datetime
import json
import re
import logging
from typing import Any

from airflow.sdk import DAG, task
from airflow.models import BaseOperator


class DataValidationOperator(BaseOperator):
    """
    Validates data against specified rules.

    This custom operator demonstrates:
    - Proper BaseOperator inheritance
    - Template field support for dynamic paths
    - Multiple validation rule types
    - Comprehensive result reporting

    :param file_path: Path to the data file (templatable with Jinja)
    :param rules: List of validation rule dictionaries
    :param fail_on_error: Whether to raise exception on validation failure

    Example rules:
        [
            {"field": "email", "type": "regex", "pattern": r".+@.+\..+"},
            {"field": "age", "type": "range", "min": 18, "max": 120},
            {"field": "status", "type": "allowed_values", "values": ["active", "inactive"]},
            {"field": "name", "type": "required"},
        ]
    """

    # Enable Jinja templating for file_path
    # This allows using {{ ds }}, {{ logical_date }}, etc.
    template_fields = ("file_path",)

    def __init__(
        self,
        file_path: str,
        rules: list[dict],
        fail_on_error: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.rules = rules
        self.fail_on_error = fail_on_error
        self.log = logging.getLogger(self.__class__.__name__)

    def execute(self, context) -> dict[str, Any]:
        """
        Execute the validation logic.

        :param context: Airflow context dictionary
        :return: Dictionary with validation results (pushed to XCom)
        """
        self.log.info("=" * 60)
        self.log.info("DATA VALIDATION OPERATOR")
        self.log.info("=" * 60)
        self.log.info(f"File path: {self.file_path}")
        self.log.info(f"Number of rules: {len(self.rules)}")
        self.log.info(f"Fail on error: {self.fail_on_error}")

        # Load or simulate data
        data = self._load_data()

        # Validate each rule
        results = []
        passed_count = 0
        failed_count = 0

        for rule in self.rules:
            result = self._validate_rule(data, rule)
            results.append(result)

            if result["status"] == "passed":
                passed_count += 1
                self.log.info(f"✅ Rule '{rule['field']}' ({rule['type']}): PASSED")
            else:
                failed_count += 1
                self.log.warning(
                    f"❌ Rule '{rule['field']}' ({rule['type']}): FAILED - {result.get('reason', 'Unknown')}"
                )

        # Build summary
        summary = {
            "file_path": self.file_path,
            "total_rules": len(self.rules),
            "passed": passed_count,
            "failed": failed_count,
            "success_rate": round(passed_count / len(self.rules) * 100, 2) if self.rules else 0,
            "details": results,
            "validated_at": datetime.now().isoformat(),
        }

        self.log.info("=" * 60)
        self.log.info(f"VALIDATION SUMMARY: {passed_count}/{len(self.rules)} rules passed")
        self.log.info("=" * 60)

        # Handle fail_on_error
        if self.fail_on_error and failed_count > 0:
            raise ValueError(
                f"Validation failed: {failed_count} rule(s) did not pass. "
                f"Details: {[r for r in results if r['status'] == 'failed']}"
            )

        return summary

    def _load_data(self) -> dict:
        """
        Load data from file or simulate it.

        In production, this would read from the actual file.
        For the exercise, we simulate sample data.
        """
        # Simulate data (in production: read from self.file_path)
        simulated_data = {
            "name": "John Doe",
            "email": "john.doe@example.com",
            "age": 30,
            "status": "active",
            "score": 85.5,
        }

        self.log.info(f"Loaded data: {simulated_data}")
        return simulated_data

    def _validate_rule(self, data: dict, rule: dict) -> dict:
        """
        Validate a single rule against data.

        Supported rule types:
        - regex: Check if field matches regular expression pattern
        - range: Check if field value is within min/max bounds
        - allowed_values: Check if field is in list of allowed values
        - required: Check if field exists and is not None/empty

        :param data: Data dictionary to validate
        :param rule: Rule specification dictionary
        :return: Result dictionary with status and details
        """
        field = rule.get("field")
        rule_type = rule.get("type")
        value = data.get(field)

        result = {
            "field": field,
            "rule_type": rule_type,
            "value": value,
            "status": "passed",
        }

        try:
            if rule_type == "regex":
                pattern = rule.get("pattern", "")
                if value is None or not re.match(pattern, str(value)):
                    result["status"] = "failed"
                    result["reason"] = f"Value '{value}' does not match pattern '{pattern}'"

            elif rule_type == "range":
                min_val = rule.get("min", float("-inf"))
                max_val = rule.get("max", float("inf"))

                if value is None:
                    result["status"] = "failed"
                    result["reason"] = "Value is None"
                elif not (min_val <= value <= max_val):
                    result["status"] = "failed"
                    result["reason"] = f"Value {value} not in range [{min_val}, {max_val}]"

            elif rule_type == "allowed_values":
                allowed = rule.get("values", [])
                if value not in allowed:
                    result["status"] = "failed"
                    result["reason"] = f"Value '{value}' not in allowed values {allowed}"

            elif rule_type == "required":
                if value is None or (isinstance(value, str) and value.strip() == ""):
                    result["status"] = "failed"
                    result["reason"] = "Field is required but missing or empty"

            else:
                result["status"] = "failed"
                result["reason"] = f"Unknown rule type: {rule_type}"

        except Exception as e:
            result["status"] = "failed"
            result["reason"] = f"Validation error: {str(e)}"

        return result


# =========================================================================
# DAG DEFINITION - Demonstrate the custom operator
# =========================================================================

with DAG(
    dag_id="solution_3_2_custom_operator",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-03", "custom-operator"],
    description="Demonstrates custom DataValidationOperator",
) as dag:

    @task
    def prepare_test_data() -> str:
        """Create test data for validation (simulated)."""
        test_data = {
            "name": "Jane Smith",
            "email": "jane.smith@example.com",
            "age": 28,
            "status": "active",
        }

        # In production: write to file
        # For exercise: just log and return path
        file_path = "/tmp/test_data.json"
        print(f"Test data prepared: {test_data}")
        print(f"Would write to: {file_path}")

        return file_path

    # Use the custom operator
    validate_user_data = DataValidationOperator(
        task_id="validate_user_data",
        file_path="/tmp/test_data.json",
        rules=[
            {"field": "email", "type": "regex", "pattern": r".+@.+\..+"},
            {"field": "age", "type": "range", "min": 0, "max": 150},
            {"field": "status", "type": "allowed_values", "values": ["active", "inactive", "pending"]},
            {"field": "name", "type": "required"},
        ],
        fail_on_error=False,
    )

    @task
    def process_validation_results(results: dict) -> dict:
        """Process and report on validation results."""
        print("=" * 60)
        print("VALIDATION RESULTS PROCESSING")
        print("=" * 60)
        print(f"File validated: {results['file_path']}")
        print(f"Success rate: {results['success_rate']}%")
        print(f"Passed: {results['passed']}/{results['total_rules']}")

        if results["failed"] > 0:
            print("\nFailed validations:")
            for detail in results["details"]:
                if detail["status"] == "failed":
                    print(f"  - {detail['field']}: {detail.get('reason', 'Unknown')}")

        return {"processed": True, "validation_summary": results}

    # Wire up the tasks
    prep = prepare_test_data()
    prep >> validate_user_data
    process_validation_results(validate_user_data.output)
