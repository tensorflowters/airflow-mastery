# Exercise 3.3: Hook Usage

## Objective

Create a TaskFlow DAG that demonstrates using Hooks to interact with external systems, specifically fetching data from a public API and processing the results.

## Requirements

Your DAG should:
1. Be named `exercise_3_3_hook_usage`
2. Have a start date of January 1, 2024
3. Not be scheduled (manual trigger only)
4. Include tags: `["exercise", "module-03", "hooks"]`

### Task Requirements

1. **fetch_data** task:
   - Uses HttpHook (or requests library) to fetch data from a public API
   - Suggested API: JSONPlaceholder (https://jsonplaceholder.typicode.com)
   - Fetch posts or users data
   - Return the JSON response

2. **process_data** task:
   - Receives the API data
   - Performs some transformation (count, filter, summarize)
   - Returns processed results

3. **save_results** task:
   - Takes processed results
   - Simulates saving to a file (print the path and summary)
   - Returns metadata about the save operation

## About Hooks

Hooks are Airflow's interface to external systems. They provide:
- Connection management (credentials, endpoints)
- Reusable interaction patterns
- Consistent error handling

Common hooks:
- `HttpHook`: REST API calls
- `PostgresHook`: Database operations
- `S3Hook`: AWS S3 operations
- `GCSHook`: Google Cloud Storage

## Starter Code

Create a file `dags/playground/exercise_3_3_hook_usage.py`:

```python
"""
Exercise 3.3: Hook Usage
========================
Learn to use Hooks for external system interaction.
"""

from datetime import datetime
# TODO: Import necessary modules
# from airflow.sdk import dag, task
# from airflow.providers.http.hooks.http import HttpHook
# Or use requests directly for simplicity


# TODO: Create the DAG with @dag decorator

    # TODO: Create fetch_data task
    # Option A: Use HttpHook with a configured connection
    # Option B: Use requests library directly (simpler for exercise)

    # TODO: Create process_data task
    # Summarize the fetched data

    # TODO: Create save_results task
    # Simulate saving to file

    # TODO: Wire up the tasks
```

## Hints

<details>
<summary>Hint 1: Using requests directly (simpler)</summary>

```python
import requests

@task
def fetch_data() -> list:
    """Fetch data from public API."""
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    response.raise_for_status()
    return response.json()
```

</details>

<details>
<summary>Hint 2: Using HttpHook (production pattern)</summary>

```python
from airflow.providers.http.hooks.http import HttpHook

@task
def fetch_data() -> list:
    """Fetch data using HttpHook."""
    # Requires connection 'http_default' or custom conn_id
    hook = HttpHook(method="GET", http_conn_id="http_default")

    # Override the endpoint if connection has base URL
    response = hook.run(endpoint="/posts")
    return response.json()
```

</details>

<details>
<summary>Hint 3: Processing data</summary>

```python
@task
def process_data(posts: list) -> dict:
    """Process and summarize the posts data."""
    return {
        "total_posts": len(posts),
        "unique_users": len(set(p["userId"] for p in posts)),
        "avg_title_length": sum(len(p["title"]) for p in posts) / len(posts),
        "sample_titles": [p["title"] for p in posts[:3]],
    }
```

</details>

<details>
<summary>Hint 4: Creating a connection (for HttpHook)</summary>

If using HttpHook, create a connection in Airflow UI:
1. Admin → Connections → Add
2. Conn ID: `jsonplaceholder_api`
3. Conn Type: HTTP
4. Host: `https://jsonplaceholder.typicode.com`

Or use environment variable:
```bash
AIRFLOW_CONN_JSONPLACEHOLDER_API='http://jsonplaceholder.typicode.com'
```

</details>

## Success Criteria

- [ ] DAG successfully fetches data from external API
- [ ] Data is properly passed between tasks via XCom
- [ ] Processing task summarizes the data correctly
- [ ] Save task simulates file output with metadata
- [ ] Error handling for API failures (optional but recommended)

## API Reference

JSONPlaceholder endpoints:
- `/posts` - 100 posts
- `/users` - 10 users
- `/comments` - 500 comments
- `/todos` - 200 todos

Each returns JSON arrays with predictable structure.
