# Exercise 3.1: Operator Exploration

## Objective

Create a DAG that demonstrates multiple operator types working together, including conditional branching based on data content.

## Requirements

Your DAG should:
1. Be named `exercise_3_1_operator_exploration`
2. Have a start date of January 1, 2024
3. Not be scheduled (manual trigger only)
4. Include tags: `["exercise", "module-03", "operators"]`

### Task Requirements

1. **BashOperator** (`generate_data`):
   - Creates a temp file
   - Writes random data (use `$RANDOM` or similar)
   - Include a "status" line that's either "VALID" or "INVALID" randomly
   - Echo the file path for downstream tasks

2. **PythonOperator** (`analyze_data`):
   - Reads the file path from XCom
   - Reads file content (simulated or actual)
   - Determines if status is VALID or INVALID
   - Returns the status string via XCom

3. **BranchPythonOperator** (`decide_branch`):
   - Pulls the status from `analyze_data`
   - Returns task_id "process_valid" if VALID
   - Returns task_id "handle_invalid" if INVALID

4. **EmptyOperator** (`process_valid`):
   - Endpoint for valid data path

5. **EmptyOperator** (`handle_invalid`):
   - Endpoint for invalid data path

6. **EmptyOperator** (`final_task`):
   - Runs after either branch (use trigger_rule)

## Starter Code

Create a file `dags/playground/exercise_3_1_operator_exploration.py`:

```python
"""
Exercise 3.1: Operator Exploration
==================================
Explore different operator types and branching patterns.
"""

from datetime import datetime
# TODO: Import necessary operators
# from airflow.sdk import DAG
# from airflow.providers.standard.operators.bash import BashOperator
# from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
# from airflow.providers.standard.operators.empty import EmptyOperator


# TODO: Define Python callables for PythonOperator and BranchPythonOperator
# def analyze_data_func(**context):
#     pass

# def decide_branch_func(**context):
#     pass


# TODO: Create the DAG
# with DAG(...) as dag:
    # Create tasks and set dependencies
```

## Hints

<details>
<summary>Hint 1: Bash random value</summary>

```bash
# Generate random status in bash
if [ $((RANDOM % 2)) -eq 0 ]; then
    STATUS="VALID"
else
    STATUS="INVALID"
fi
echo "STATUS=$STATUS" > $TEMP_FILE
```

</details>

<details>
<summary>Hint 2: BranchPythonOperator return</summary>

```python
def decide_branch_func(**context):
    ti = context['ti']
    status = ti.xcom_pull(task_ids='analyze_data')

    if status == "VALID":
        return "process_valid"  # Return task_id as string
    else:
        return "handle_invalid"
```

</details>

<details>
<summary>Hint 3: Final task trigger rule</summary>

```python
# Use trigger_rule to run after ANY branch completes
final_task = EmptyOperator(
    task_id="final_task",
    trigger_rule="none_failed_min_one_success",
)
```

</details>

## Success Criteria

- [ ] BashOperator creates file with random status
- [ ] PythonOperator correctly reads and analyzes data
- [ ] Branch correctly routes based on status
- [ ] Only one branch executes per run
- [ ] Final task runs regardless of which branch was taken
