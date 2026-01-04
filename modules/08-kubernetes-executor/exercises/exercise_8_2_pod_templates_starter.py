"""
Exercise 8.2: Custom Pod Templates (Starter)
=============================================

Create a DAG demonstrating different Kubernetes pod configurations
for the KubernetesExecutor.

Learning Objectives:
1. Configure per-task resource requirements
2. Use node selectors and tolerations
3. Inject secrets as environment variables
4. Add init containers for setup tasks

Prerequisites:
- Kubernetes cluster with Airflow deployed
- kubernetes Python package installed (pip install kubernetes)
"""

from airflow.sdk import dag, task
from datetime import datetime

# TODO: Uncomment when implementing
# from kubernetes.client import models as k8s


# =============================================================================
# POD CONFIGURATIONS
# =============================================================================

# TODO: Define high_memory_pod
# Create a V1Pod with:
# - Container resources: 2 CPU requests, 4Gi memory requests
# - Limits: 4 CPU, 8Gi memory
#
# high_memory_pod = k8s.V1Pod(
#     spec=k8s.V1PodSpec(
#         containers=[
#             k8s.V1Container(
#                 name="base",
#                 resources=k8s.V1ResourceRequirements(
#                     requests={"cpu": "???", "memory": "???"},
#                     limits={"cpu": "???", "memory": "???"}
#                 )
#             )
#         ]
#     )
# )


# TODO: Define gpu_pod
# Create a V1Pod with:
# - Node selector: {"accelerator": "nvidia-gpu"}
# - Toleration for GPU taints
# - Resource limit: nvidia.com/gpu: 1
#
# gpu_pod = k8s.V1Pod(
#     spec=k8s.V1PodSpec(
#         containers=[...],
#         node_selector={...},
#         tolerations=[...]
#     )
# )


# TODO: Define env_vars_pod
# Create a V1Pod with:
# - Environment variable from secret: API_KEY from secret "api-credentials"
# - Static environment variable: ENVIRONMENT = "production"
#
# env_vars_pod = k8s.V1Pod(
#     spec=k8s.V1PodSpec(
#         containers=[
#             k8s.V1Container(
#                 name="base",
#                 env=[
#                     k8s.V1EnvVar(...),
#                     k8s.V1EnvVar(...)
#                 ]
#             )
#         ]
#     )
# )


# TODO: Define init_container_pod
# Create a V1Pod with:
# - Init container that runs before main task
# - Shared volume between init and main container
#
# init_container_pod = k8s.V1Pod(
#     spec=k8s.V1PodSpec(
#         init_containers=[...],
#         containers=[...],
#         volumes=[...]
#     )
# )


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="exercise_8_2_custom_pods",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["exercise", "kubernetes"],
    description="Demonstrates custom Kubernetes pod configurations",
)
def custom_pods_dag():
    """
    DAG demonstrating custom pod configurations for KubernetesExecutor.

    Each task uses different pod settings to show customization options:
    1. Standard task - uses default pod template
    2. High memory task - increased resource allocation
    3. GPU task - node selection for accelerators
    4. Secrets task - environment variable injection
    5. Init container task - preparation before execution
    """

    @task
    def standard_task():
        """
        Task 1: Standard task using default pod configuration.

        This uses whatever pod template is configured globally.
        """
        import os
        import platform

        print("=" * 50)
        print("STANDARD TASK")
        print("=" * 50)
        print(f"Hostname: {os.environ.get('HOSTNAME', 'unknown')}")
        print(f"Platform: {platform.platform()}")
        print(f"Python: {platform.python_version()}")

        return {"type": "standard", "status": "completed"}

    # TODO: Implement high_memory_task
    # Use @task decorator with executor_config parameter
    # @task(executor_config={"pod_override": high_memory_pod})
    # def high_memory_task():
    #     """Task 2: High memory task with 4Gi allocation."""
    #     import resource
    #     # Get memory limits
    #     soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    #     print(f"Memory limits - Soft: {soft}, Hard: {hard}")
    #     return {"memory": "4Gi", "status": "completed"}

    # TODO: Implement gpu_task
    # @task(executor_config={"pod_override": gpu_pod})
    # def gpu_task():
    #     """Task 3: GPU-enabled task."""
    #     import subprocess
    #     try:
    #         result = subprocess.run(
    #             ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
    #             capture_output=True, text=True, check=True
    #         )
    #         gpu_name = result.stdout.strip()
    #         print(f"GPU available: {gpu_name}")
    #         return {"gpu": gpu_name, "status": "completed"}
    #     except Exception as e:
    #         print(f"No GPU available: {e}")
    #         return {"gpu": None, "status": "no_gpu"}

    # TODO: Implement secure_task
    # @task(executor_config={"pod_override": env_vars_pod})
    # def secure_task():
    #     """Task 4: Task with secrets from Kubernetes."""
    #     import os
    #     api_key = os.environ.get("API_KEY")
    #     environment = os.environ.get("ENVIRONMENT")
    #     print(f"Environment: {environment}")
    #     print(f"API Key present: {bool(api_key)}")
    #     # Never log actual secrets!
    #     return {
    #         "api_key_present": bool(api_key),
    #         "environment": environment
    #     }

    # TODO: Implement prepared_task
    # @task(executor_config={"pod_override": init_container_pod})
    # def prepared_task():
    #     """Task 5: Task with init container preparation."""
    #     import os
    #     # Check for files prepared by init container
    #     data_file = "/data/prepared.txt"
    #     if os.path.exists(data_file):
    #         with open(data_file) as f:
    #             prepared_data = f.read()
    #         print(f"Init container prepared: {prepared_data}")
    #         return {"prepared": True, "data": prepared_data}
    #     else:
    #         print("No prepared data found")
    #         return {"prepared": False}

    # Task execution
    result1 = standard_task()

    # TODO: Uncomment when tasks are implemented
    # result2 = high_memory_task()
    # result3 = gpu_task()
    # result4 = secure_task()
    # result5 = prepared_task()

    # TODO: Set up dependencies
    # result1 >> result2 >> result3 >> result4 >> result5


# Instantiate the DAG
custom_pods_dag()


# =============================================================================
# HELPER FUNCTIONS FOR TESTING
# =============================================================================


def print_pod_config(pod_config):
    """Helper to print pod configuration for debugging."""
    import json
    from kubernetes.client import ApiClient

    api_client = ApiClient()
    pod_dict = api_client.sanitize_for_serialization(pod_config)
    print(json.dumps(pod_dict, indent=2))


if __name__ == "__main__":
    # Test pod configurations locally
    print("Testing pod configurations...")

    # TODO: Uncomment to test your configurations
    # print("\n=== High Memory Pod ===")
    # print_pod_config(high_memory_pod)
    #
    # print("\n=== GPU Pod ===")
    # print_pod_config(gpu_pod)
    #
    # print("\n=== Env Vars Pod ===")
    # print_pod_config(env_vars_pod)
    #
    # print("\n=== Init Container Pod ===")
    # print_pod_config(init_container_pod)
