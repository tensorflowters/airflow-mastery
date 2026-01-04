"""
Exercise 3.3: Hook Usage
========================

Learn to use Hooks for external system interaction.

You'll practice:
- Fetching data from external APIs
- Using HttpHook or requests library
- Processing API responses
- Simulating file storage operations
"""

from datetime import datetime

# TODO: Import dag and task from airflow.sdk
# from airflow.sdk import dag, task

# For API calls, choose one:
# Option A: Use requests (simpler, no connection setup)
# import requests

# Option B: Use HttpHook (production pattern, requires connection)
# from airflow.providers.http.hooks.http import HttpHook


# =========================================================================
# DAG DEFINITION
# =========================================================================

# TODO: Add @dag decorator
# @dag(
#     dag_id="exercise_3_3_hook_usage",
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["exercise", "module-03", "hooks"],
# )
def hook_usage_dag():
    """Demonstrate Hook usage for external system interaction."""

    # =====================================================================
    # TASK 1: Fetch Data from API
    # =====================================================================

    # TODO: Create fetch_data task
    # Option A: Using requests library (simpler)
    # @task
    # def fetch_data() -> list:
    #     """Fetch posts from JSONPlaceholder API."""
    #     import requests
    #
    #     url = "https://jsonplaceholder.typicode.com/posts"
    #     print(f"Fetching data from: {url}")
    #
    #     response = requests.get(url, timeout=30)
    #     response.raise_for_status()  # Raise exception for HTTP errors
    #
    #     data = response.json()
    #     print(f"Fetched {len(data)} records")
    #
    #     return data

    # Option B: Using HttpHook (requires connection setup)
    # @task
    # def fetch_data() -> list:
    #     """Fetch posts using HttpHook."""
    #     from airflow.providers.http.hooks.http import HttpHook
    #
    #     # Connection must exist: jsonplaceholder_api
    #     hook = HttpHook(method="GET", http_conn_id="jsonplaceholder_api")
    #     response = hook.run(endpoint="/posts")
    #
    #     return response.json()

    # =====================================================================
    # TASK 2: Process Data
    # =====================================================================

    # TODO: Create process_data task
    # @task
    # def process_data(posts: list) -> dict:
    #     """Process and summarize the fetched posts."""
    #     print(f"Processing {len(posts)} posts...")
    #
    #     # Calculate statistics
    #     # total_posts = len(posts)
    #     # unique_users = len(set(p["userId"] for p in posts))
    #     # avg_title_length = sum(len(p["title"]) for p in posts) / len(posts)
    #
    #     # summary = {
    #     #     "total_posts": total_posts,
    #     #     "unique_users": unique_users,
    #     #     "avg_title_length": round(avg_title_length, 2),
    #     #     "sample_titles": [p["title"] for p in posts[:3]],
    #     # }
    #
    #     # print(f"Summary: {summary}")
    #     # return summary
    #     pass

    # =====================================================================
    # TASK 3: Save Results
    # =====================================================================

    # TODO: Create save_results task
    # @task
    # def save_results(summary: dict) -> dict:
    #     """Simulate saving results to a file."""
    #     import json
    #
    #     # Simulate file path
    #     # file_path = f"/data/output/posts_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    #
    #     # In production, you would write to actual storage:
    #     # with open(file_path, 'w') as f:
    #     #     json.dump(summary, f, indent=2)
    #
    #     # For exercise, just print
    #     # print(f"Would save to: {file_path}")
    #     # print(f"Content: {json.dumps(summary, indent=2)}")
    #
    #     # return {
    #     #     "file_path": file_path,
    #     #     "bytes_written": len(json.dumps(summary)),
    #     #     "saved_at": datetime.now().isoformat(),
    #     # }
    #     pass

    # =====================================================================
    # WIRE UP THE TASKS
    # =====================================================================

    # TODO: Chain the tasks
    # posts = fetch_data()
    # summary = process_data(posts)
    # save_results(summary)

    pass  # Remove when implementing


# TODO: Instantiate the DAG
# hook_usage_dag()
