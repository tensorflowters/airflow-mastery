# Exercise 7.3: Debug a Broken DAG

## Objective

Practice debugging skills by identifying and fixing multiple issues in a deliberately broken DAG.

## Background

Debugging DAGs requires:
- Understanding error messages and stack traces
- Checking logs at various levels
- Using Airflow's testing tools
- Systematic problem isolation

### Common DAG Issues

| Issue Type | Symptoms |
|------------|----------|
| Import errors | DAG doesn't appear in UI |
| Syntax errors | DAG fails to parse |
| Runtime errors | Tasks fail during execution |
| Logic errors | Tasks complete but produce wrong results |
| Dependency issues | Tasks run in wrong order |

## The Broken DAG

A broken DAG is provided in `broken_dag.py`. It contains multiple intentional bugs:

1. **Import error**: Missing or incorrect import
2. **Syntax error**: Python syntax issue
3. **Configuration error**: Invalid DAG parameter
4. **Runtime error**: Task fails during execution
5. **Logic error**: Incorrect business logic
6. **Dependency error**: Wrong task order

## Requirements

1. **Identify Issues**
   - List all bugs found
   - Explain what each bug causes

2. **Fix Each Bug**
   - Correct all issues
   - Explain your fix

3. **Add Regression Tests**
   - Write tests that would catch these bugs
   - Prevent future regressions

## Debugging Techniques

### Check for Import Errors

```bash
# Quick import check
python -c "from airflow.models import DagBag; db=DagBag('dags/'); print(db.import_errors)"

# Or use airflow CLI
airflow dags list-import-errors
```

### Test DAG Parsing

```bash
# Test specific DAG
airflow dags test broken_dag 2024-01-15
```

### Check Task Logs

```bash
# In production
airflow tasks logs broken_dag task_id 2024-01-15

# In UI: Click on task → View Log
```

### Debug Interactively

```python
# In Python shell
from airflow.models import DagBag

dag_bag = DagBag()
dag = dag_bag.dags.get("broken_dag")

# Inspect DAG
print(dag.schedule_interval)
print(dag.tasks)

# Test task
task = dag.get_task("task_name")
task.execute(context={})
```

## Starter Code

See `broken_dag.py` for the broken code.

## Hints

<details>
<summary>Hint 1: Finding import errors</summary>

Run this to see import errors:
```bash
python -c "from dags.exercises.broken_dag import *"
```

If you see a traceback, that's an import error.

</details>

<details>
<summary>Hint 2: Common syntax issues</summary>

- Missing colons after function definitions
- Incorrect indentation
- Unclosed parentheses or brackets
- Wrong decorator syntax

</details>

<details>
<summary>Hint 3: Configuration validation</summary>

Check these DAG parameters:
- `schedule` format
- `start_date` type (must be datetime)
- `dag_id` uniqueness
- `default_args` structure

</details>

<details>
<summary>Hint 4: Runtime debugging</summary>

If a task fails:
1. Check the task logs
2. Look for the exception type
3. Find the line number
4. Test the function in isolation

</details>

## Success Criteria

- [ ] All import errors identified and fixed
- [ ] All syntax errors identified and fixed
- [ ] All configuration errors identified and fixed
- [ ] All runtime errors identified and fixed
- [ ] All logic errors identified and fixed
- [ ] DAG runs successfully end-to-end
- [ ] Tests added to prevent regression

## Debugging Checklist

```
□ DAG appears in Airflow UI
□ No import errors shown
□ DAG can be triggered
□ All tasks complete successfully
□ Output data is correct
□ Dependencies execute in correct order
□ Tests pass
```
