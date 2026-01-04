"""
Solution 3.3: Hook Usage
========================

Complete solution demonstrating:
- API data fetching with requests library
- Alternative HttpHook pattern (commented)
- Data processing and summarization
- Simulated file storage with metadata
"""

from datetime import datetime
import json
import requests
from airflow.sdk import dag, task


@dag(
    dag_id="solution_3_3_hook_usage",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "module-03", "hooks", "api"],
    description="Demonstrates Hook/API usage patterns",
)
def hook_usage_dag():
    """
    Hook Usage Pipeline.

    Demonstrates fetching data from external APIs,
    processing the response, and storing results.

    Uses JSONPlaceholder as a free, public test API.
    """

    @task(retries=2)
    def fetch_data() -> list:
        """
        Fetch posts from JSONPlaceholder API.

        This uses the requests library directly for simplicity.
        For production, consider using HttpHook with a managed connection.

        Returns:
            List of post dictionaries from the API
        """
        url = "https://jsonplaceholder.typicode.com/posts"

        print("=" * 60)
        print("FETCHING DATA FROM API")
        print("=" * 60)
        print(f"URL: {url}")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()
            print(f"Status Code: {response.status_code}")
            print(f"Records fetched: {len(data)}")
            print(f"Sample record: {data[0] if data else 'No data'}")
            print("=" * 60)

            return data

        except requests.RequestException as e:
            print(f"API Error: {e}")
            raise

    # Alternative implementation using HttpHook (requires connection setup)
    # Uncomment to use instead of the above
    """
    @task(retries=2)
    def fetch_data_with_hook() -> list:
        '''
        Fetch posts using HttpHook.

        Requires a connection to be configured:
        - Conn ID: jsonplaceholder_api
        - Conn Type: HTTP
        - Host: https://jsonplaceholder.typicode.com

        Or via environment:
        AIRFLOW_CONN_JSONPLACEHOLDER_API='http://jsonplaceholder.typicode.com'
        '''
        from airflow.providers.http.hooks.http import HttpHook

        hook = HttpHook(method="GET", http_conn_id="jsonplaceholder_api")

        print("Fetching data using HttpHook...")
        response = hook.run(endpoint="/posts")

        if response.status_code != 200:
            raise ValueError(f"API returned status {response.status_code}")

        data = response.json()
        print(f"Fetched {len(data)} records via HttpHook")

        return data
    """

    @task
    def process_data(posts: list) -> dict:
        """
        Process and summarize the fetched posts data.

        Calculates various statistics and extracts insights
        from the raw API response.

        Args:
            posts: List of post dictionaries from the API

        Returns:
            Dictionary with processed summary statistics
        """
        print("=" * 60)
        print("PROCESSING DATA")
        print("=" * 60)
        print(f"Processing {len(posts)} posts...")

        if not posts:
            return {
                "total_posts": 0,
                "error": "No data to process",
            }

        # Calculate statistics
        total_posts = len(posts)
        unique_users = len(set(p["userId"] for p in posts))
        avg_title_length = sum(len(p["title"]) for p in posts) / len(posts)
        avg_body_length = sum(len(p["body"]) for p in posts) / len(posts)

        # Group posts by user
        posts_per_user = {}
        for post in posts:
            user_id = post["userId"]
            posts_per_user[user_id] = posts_per_user.get(user_id, 0) + 1

        # Find most active user
        most_active_user = max(posts_per_user.items(), key=lambda x: x[1])

        summary = {
            "total_posts": total_posts,
            "unique_users": unique_users,
            "avg_title_length": round(avg_title_length, 2),
            "avg_body_length": round(avg_body_length, 2),
            "posts_per_user": posts_per_user,
            "most_active_user": {
                "user_id": most_active_user[0],
                "post_count": most_active_user[1],
            },
            "sample_titles": [p["title"][:50] + "..." for p in posts[:3]],
            "processed_at": datetime.now().isoformat(),
        }

        print(f"Total posts: {total_posts}")
        print(f"Unique users: {unique_users}")
        print(f"Average title length: {avg_title_length:.2f} chars")
        print(f"Most active user: {most_active_user[0]} ({most_active_user[1]} posts)")
        print("=" * 60)

        return summary

    @task
    def save_results(summary: dict) -> dict:
        """
        Simulate saving results to a file.

        In production, this might:
        - Write to S3 using S3Hook
        - Insert into database using PostgresHook
        - Push to message queue

        Args:
            summary: Processed data summary

        Returns:
            Metadata about the save operation
        """
        print("=" * 60)
        print("SAVING RESULTS")
        print("=" * 60)

        # Generate file path with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"/data/output/posts_summary_{timestamp}.json"

        # Prepare content
        content = json.dumps(summary, indent=2, default=str)
        bytes_count = len(content.encode("utf-8"))

        # Simulate writing (in production: actual file/S3/database write)
        print(f"Simulating save to: {file_path}")
        print(f"Content size: {bytes_count} bytes")
        print("\nContent preview:")
        print("-" * 40)
        # Print first 500 chars
        print(content[:500] + ("..." if len(content) > 500 else ""))
        print("-" * 40)

        # In production with S3Hook:
        # from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        # hook = S3Hook(aws_conn_id='aws_default')
        # hook.load_string(content, key=file_path, bucket_name='my-bucket')

        metadata = {
            "file_path": file_path,
            "bytes_written": bytes_count,
            "records_summarized": summary.get("total_posts", 0),
            "saved_at": datetime.now().isoformat(),
            "status": "simulated",
        }

        print(f"\nSave metadata: {metadata}")
        print("=" * 60)

        return metadata

    # Wire up the pipeline
    posts = fetch_data()
    summary = process_data(posts)
    save_results(summary)


# Instantiate the DAG
hook_usage_dag()
