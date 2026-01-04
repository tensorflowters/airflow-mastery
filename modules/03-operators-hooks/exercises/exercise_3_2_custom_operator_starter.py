"""
Exercise 3.2: Custom Operator
=============================

Create a reusable DataValidationOperator.

You'll practice:
- Inheriting from BaseOperator
- Implementing the execute() method
- Using template_fields for Jinja support
- Returning results via XCom
- Proper logging patterns
"""

from datetime import datetime
import json
import re

# TODO: Import DAG from airflow.sdk
# from airflow.sdk import DAG

# TODO: Import BaseOperator
# from airflow.models import BaseOperator


# =========================================================================
# CUSTOM OPERATOR DEFINITION
# =========================================================================

# TODO: Create the DataValidationOperator class
# class DataValidationOperator(BaseOperator):
#     """
#     Validates data against specified rules.
#
#     :param file_path: Path to the data file (templatable with Jinja)
#     :param rules: List of validation rule dictionaries
#     :param fail_on_error: Whether to raise exception on validation failure
#     """
#
#     # Enable Jinja templating for these fields
#     template_fields = ("file_path",)
#
#     def __init__(
#         self,
#         file_path: str,
#         rules: list,
#         fail_on_error: bool = False,
#         **kwargs
#     ):
#         super().__init__(**kwargs)
#         # TODO: Store the parameters as instance attributes
#         # self.file_path = file_path
#         # self.rules = rules
#         # self.fail_on_error = fail_on_error
#         pass
#
#     def execute(self, context):
#         """
#         Execute validation logic.
#
#         :param context: Airflow context dictionary
#         :return: Dictionary with validation results
#         """
#         # TODO: Implement validation logic
#         # 1. Log start of validation
#         # 2. Load/simulate data from file_path
#         # 3. Validate each rule
#         # 4. Collect results
#         # 5. Handle fail_on_error if needed
#         # 6. Return results dictionary
#
#         # Example result structure:
#         # {
#         #     "file_path": self.file_path,
#         #     "total_rules": len(self.rules),
#         #     "passed": 5,
#         #     "failed": 1,
#         #     "details": [
#         #         {"rule": "email", "status": "passed"},
#         #         {"rule": "age", "status": "failed", "reason": "out of range"}
#         #     ]
#         # }
#         pass
#
#     def _validate_rule(self, data: dict, rule: dict) -> dict:
#         """
#         Validate a single rule against data.
#
#         :param data: Data dictionary to validate
#         :param rule: Rule specification dictionary
#         :return: Result dictionary with status and details
#         """
#         # TODO: Implement rule validation
#         # Handle these rule types:
#         # - "regex": Check if field matches pattern
#         # - "range": Check if field is within min/max
#         # - "allowed_values": Check if field is in list
#         # - "required": Check if field exists and is not None
#         pass


# =========================================================================
# DAG DEFINITION - Test the custom operator
# =========================================================================

# TODO: Create a test DAG
# with DAG(
#     dag_id="exercise_3_2_custom_operator",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-03", "custom-operator"],
# ) as dag:

    # TODO: Create sample data for testing
    # You can use a TaskFlow task to create test data

    # TODO: Use your DataValidationOperator
    # validate_data = DataValidationOperator(
    #     task_id="validate_data",
    #     file_path="/tmp/test_data.json",
    #     rules=[
    #         {"field": "email", "type": "regex", "pattern": r".+@.+\..+"},
    #         {"field": "age", "type": "range", "min": 0, "max": 150},
    #         {"field": "status", "type": "allowed_values", "values": ["active", "inactive"]},
    #         {"field": "name", "type": "required"},
    #     ],
    #     fail_on_error=False,
    # )

    # TODO: Create a task to process validation results

    pass  # Remove when implementing
