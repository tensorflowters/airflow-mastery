"""
Solution for Exercise 1.2: Parallel DAG

This solution demonstrates:
- Parallel task execution (A and B run simultaneously)
- Fan-in pattern (C waits for both A and B)
- Linear dependency (D waits for C)
- Data passing between tasks via XCom (automatic with TaskFlow)
"""

from airflow.sdk import DAG, task
from datetime import datetime
import time

with DAG(
    dag_id="ex_1_2_parallel_dag",
    description="Exercise 1.2 - Parallel task execution pattern",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "module-01", "solution"],
):
    
    @task
    def task_a():
        """Simulates data extraction from source A"""
        print("Task A: Starting extraction from source A...")
        time.sleep(2)  # Simulate work
        result = {"source": "A", "records": 100}
        print(f"Task A: Completed. Extracted {result['records']} records.")
        return result
    
    @task
    def task_b():
        """Simulates data extraction from source B"""
        print("Task B: Starting extraction from source B...")
        time.sleep(3)  # Simulate work (takes longer than A)
        result = {"source": "B", "records": 250}
        print(f"Task B: Completed. Extracted {result['records']} records.")
        return result
    
    @task
    def task_c(data_a: dict, data_b: dict):
        """Combines data from both sources - waits for A and B"""
        print(f"Task C: Received data from A: {data_a}")
        print(f"Task C: Received data from B: {data_b}")
        
        total_records = data_a["records"] + data_b["records"]
        combined = {
            "sources": [data_a["source"], data_b["source"]],
            "total_records": total_records,
            "status": "combined"
        }
        
        print(f"Task C: Combined {total_records} total records.")
        return combined
    
    @task
    def task_d(combined_data: dict):
        """Final task - loads combined data"""
        print(f"Task D: Received combined data: {combined_data}")
        print(f"Task D: Loading {combined_data['total_records']} records to destination...")
        print("Task D: Pipeline complete!")
        return {"status": "success", "records_loaded": combined_data["total_records"]}
    
    # Wire up the DAG
    # A and B run in parallel (no dependency between them)
    result_a = task_a()
    result_b = task_b()
    
    # C waits for BOTH A and B by receiving their results
    combined = task_c(result_a, result_b)
    
    # D depends on C
    task_d(combined)

"""
Execution flow:

Time  Task A    Task B    Task C    Task D
─────────────────────────────────────────────
t=0   Running   Running   Waiting   Waiting
t=2   Done      Running   Waiting   Waiting
t=3   Done      Done      Running   Waiting
t=4   Done      Done      Done      Running
t=5   Done      Done      Done      Done

Key insight: 
- A and B start at the same time (parallel)
- C only starts when BOTH A and B are complete (fan-in)
- D only starts when C is complete (linear)
"""
