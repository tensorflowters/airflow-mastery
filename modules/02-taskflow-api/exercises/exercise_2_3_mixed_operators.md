# Exercise 2.3: Mixed Operators and TaskFlow

## Objective

Create a DAG that demonstrates how to seamlessly mix traditional Airflow operators with TaskFlow tasks, using XCom to pass data between them.

## Requirements

Your DAG should:
1. Be named `exercise_2_3_mixed_operators`
2. Have a start date of January 1, 2024
3. Not be scheduled (manual trigger only)
4. Include tags: `["exercise", "module-02", "mixed"]`

### Task Requirements

1. **BashOperator Task** (`create_temp_file`):
   - Creates a temporary file with some content
   - Echoes the file path (this becomes the XCom return value)
   - Use `do_xcom_push=True` (default for BashOperator with echo)

2. **TaskFlow Task** (`read_file_info`):
   - Receives the file path from the BashOperator via XCom
   - Uses `.output` to get the XCom reference
   - Returns file metadata dictionary

3. **TaskFlow Task** (`process_file`):
   - Receives the file metadata
   - Simulates processing the file
   - Returns processing results

4. **PythonOperator Task** (`cleanup`):
   - Uses traditional PythonOperator (not TaskFlow)
   - Shows how to access XCom from previous tasks
   - Simulates cleanup operations

## Starter Code

Create a file `dags/playground/exercise_2_3_mixed_operators.py`:

```python
"""
Exercise 2.3: Mixed Operators and TaskFlow
==========================================
Learn to combine traditional operators with TaskFlow tasks.
"""

from datetime import datetime
# TODO: Import necessary components
# from airflow.sdk import DAG, task
# from airflow.providers.standard.operators.bash import BashOperator
# from airflow.providers.standard.operators.python import PythonOperator


# TODO: Define a function for the PythonOperator (cleanup)
# def cleanup_function(**context):
#     # Access XCom from previous task
#     ti = context['ti']
#     ...


# TODO: Define the DAG using context manager
# with DAG(...) as dag:

    # TODO: Create BashOperator to create temp file
    # Bash command should:
    # 1. Create a temp file: /tmp/airflow_exercise_XXXXX
    # 2. Write some data to it
    # 3. Echo the file path (last echo is the XCom value)

    # TODO: Create TaskFlow task to read file info
    # Use the .output attribute to get XCom from BashOperator

    # TODO: Create TaskFlow task to process file

    # TODO: Create PythonOperator for cleanup

    # TODO: Set up dependencies
```

## Verification

1. Run the DAG and check each task's logs
2. Verify the file path passes from Bash → TaskFlow correctly
3. Check that the PythonOperator can access upstream XCom
4. Confirm cleanup task runs last

## Hints

<details>
<summary>Hint 1: BashOperator XCom</summary>

```python
# BashOperator returns the last line of stdout as XCom
create_file = BashOperator(
    task_id="create_temp_file",
    bash_command="""
        TEMP_FILE=$(mktemp /tmp/airflow_exercise_XXXXXX)
        echo "Sample data: $(date)" > $TEMP_FILE
        echo $TEMP_FILE
    """,
)

# The file path is now in create_file.output
```

</details>

<details>
<summary>Hint 2: TaskFlow consuming operator output</summary>

```python
@task
def read_file_info(file_path: str) -> dict:
    print(f"Received file path: {file_path}")
    return {"path": file_path, "exists": True}

# Use .output to get the XCom reference
file_info = read_file_info(create_file.output)
```

</details>

<details>
<summary>Hint 3: PythonOperator accessing XCom</summary>

```python
def cleanup_function(**context):
    ti = context['ti']
    # Pull XCom from specific task
    file_path = ti.xcom_pull(task_ids='create_temp_file')
    print(f"Cleaning up: {file_path}")

cleanup = PythonOperator(
    task_id="cleanup",
    python_callable=cleanup_function,
)
```

</details>

<details>
<summary>Hint 4: Setting dependencies</summary>

```python
# With TaskFlow, dependencies are set by data flow
# But you can also use >> for explicit ordering

# TaskFlow creates deps automatically:
file_info = read_file_info(create_file.output)  # create_file >> read_file_info
result = process_file(file_info)                 # read_file_info >> process_file

# For PythonOperator, use explicit dep:
result >> cleanup  # or: cleanup.set_upstream(result)
```

</details>

## Success Criteria

- [ ] BashOperator creates a temp file and outputs its path
- [ ] TaskFlow task receives the file path via XCom
- [ ] Data flows correctly through all tasks
- [ ] PythonOperator successfully accesses XCom
- [ ] All tasks show green (success) status

## Key Concepts

| Concept | Description |
|---------|-------------|
| `.output` | Property on operators that provides XCom reference |
| `ti.xcom_pull()` | Method to explicitly pull XCom in traditional operators |
| Mixed deps | TaskFlow creates deps from function calls; operators use `>>` |
| XCom bridge | `.output` bridges traditional operators to TaskFlow |
