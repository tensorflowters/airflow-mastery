"""
Exercise 1.2: Build a Parallel DAG

Your task: Create a DAG where:
- Task A and Task B run in parallel (no dependency on each other)
- Task C depends on BOTH A and B completing
- Task D depends on C

Use the TaskFlow API with @task decorators.

Requirements:
- dag_id = "ex_1_2_parallel_dag"
- Each task should print what it's doing
- Use schedule=None (manual trigger)
- Add appropriate tags

Hints:
- For parallel tasks, just don't create a dependency between them
- For C to wait on both A and B, you'll need to pass results from both

When done, copy to dags/playground/ and test with:
    airflow dags test ex_1_2_parallel_dag 2024-01-01
"""

from airflow.sdk import DAG, task
from datetime import datetime

# YOUR CODE HERE

with DAG(
    dag_id="ex_1_2_parallel_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "module-01"],
):
    # Define your tasks here using @task decorator
    
    @task
    def task_a():
        # TODO: Implement
        pass
    
    @task
    def task_b():
        # TODO: Implement
        pass
    
    @task
    def task_c():
        # TODO: Implement - this should receive data from A and B
        pass
    
    @task
    def task_d():
        # TODO: Implement - this should receive data from C
        pass
    
    # TODO: Wire up the dependencies
    # Hint: a_result = task_a()
