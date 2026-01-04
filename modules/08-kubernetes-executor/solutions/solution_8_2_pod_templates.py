"""
Solution 8.2: Custom Pod Templates
===================================

Complete implementation demonstrating Kubernetes pod customizations
for the KubernetesExecutor.

Key Concepts:
1. V1Pod spec overrides via executor_config
2. Resource requests and limits for task isolation
3. Node selection and tolerations for specialized workloads
4. Secret injection through Kubernetes-native mechanisms
5. Init containers for task preparation

Each task uses `executor_config={"pod_override": pod_spec}` to customize
how the Kubernetes scheduler runs the task pod.
"""

from airflow.sdk import dag, task
from datetime import datetime
from kubernetes.client import models as k8s


# =============================================================================
# POD CONFIGURATIONS
# =============================================================================

# Configuration 1: High Memory Pod
# Use case: Data processing, ML training, batch analytics
high_memory_pod = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(
        labels={"workload-type": "high-memory"},
        annotations={
            "cluster-autoscaler.kubernetes.io/safe-to-evict": "false"
        }
    ),
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={
                        "cpu": "2",
                        "memory": "4Gi"
                    },
                    limits={
                        "cpu": "4",
                        "memory": "8Gi"
                    }
                )
            )
        ]
    )
)


# Configuration 2: GPU Pod
# Use case: ML inference, video processing, CUDA workloads
gpu_pod = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(
        labels={"workload-type": "gpu"}
    ),
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={
                        "cpu": "2",
                        "memory": "8Gi"
                    },
                    limits={
                        "cpu": "4",
                        "memory": "16Gi",
                        "nvidia.com/gpu": "1"  # Request 1 GPU
                    }
                )
            )
        ],
        # Target nodes with GPU
        node_selector={
            "accelerator": "nvidia-gpu"
        },
        # Tolerate GPU taint
        tolerations=[
            k8s.V1Toleration(
                key="nvidia.com/gpu",
                operator="Equal",
                value="present",
                effect="NoSchedule"
            )
        ]
    )
)


# Configuration 3: Environment Variables Pod
# Use case: Tasks requiring secrets, configuration injection
env_vars_pod = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(
        labels={"workload-type": "secure"}
    ),
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                env=[
                    # Secret from Kubernetes secret
                    k8s.V1EnvVar(
                        name="API_KEY",
                        value_from=k8s.V1EnvVarSource(
                            secret_key_ref=k8s.V1SecretKeySelector(
                                name="api-credentials",
                                key="api-key"
                            )
                        )
                    ),
                    # Secret from Kubernetes secret
                    k8s.V1EnvVar(
                        name="DATABASE_PASSWORD",
                        value_from=k8s.V1EnvVarSource(
                            secret_key_ref=k8s.V1SecretKeySelector(
                                name="database-credentials",
                                key="password"
                            )
                        )
                    ),
                    # Value from ConfigMap
                    k8s.V1EnvVar(
                        name="LOG_LEVEL",
                        value_from=k8s.V1EnvVarSource(
                            config_map_key_ref=k8s.V1ConfigMapKeySelector(
                                name="app-config",
                                key="log-level"
                            )
                        )
                    ),
                    # Static environment variable
                    k8s.V1EnvVar(
                        name="ENVIRONMENT",
                        value="production"
                    ),
                    # Downward API - expose pod metadata
                    k8s.V1EnvVar(
                        name="POD_NAME",
                        value_from=k8s.V1EnvVarSource(
                            field_ref=k8s.V1ObjectFieldSelector(
                                field_path="metadata.name"
                            )
                        )
                    ),
                    k8s.V1EnvVar(
                        name="POD_NAMESPACE",
                        value_from=k8s.V1EnvVarSource(
                            field_ref=k8s.V1ObjectFieldSelector(
                                field_path="metadata.namespace"
                            )
                        )
                    )
                ]
            )
        ]
    )
)


# Configuration 4: Init Container Pod
# Use case: Downloading dependencies, waiting for services, data preparation
init_container_pod = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(
        labels={"workload-type": "prepared"}
    ),
    spec=k8s.V1PodSpec(
        # Init containers run before the main container
        init_containers=[
            k8s.V1Container(
                name="download-data",
                image="alpine/curl:latest",
                command=[
                    "sh", "-c",
                    """
                    echo "Downloading sample data..."
                    curl -s -o /data/sample.json \
                        https://jsonplaceholder.typicode.com/posts/1
                    echo "Data downloaded successfully"
                    cat /data/sample.json
                    """
                ],
                volume_mounts=[
                    k8s.V1VolumeMount(
                        name="shared-data",
                        mount_path="/data"
                    )
                ]
            ),
            k8s.V1Container(
                name="setup-environment",
                image="busybox:latest",
                command=[
                    "sh", "-c",
                    """
                    echo "Setting up environment..."
                    echo "$(date): Environment initialized" > /data/init-log.txt
                    echo "Ready for processing"
                    """
                ],
                volume_mounts=[
                    k8s.V1VolumeMount(
                        name="shared-data",
                        mount_path="/data"
                    )
                ]
            )
        ],
        containers=[
            k8s.V1Container(
                name="base",
                volume_mounts=[
                    k8s.V1VolumeMount(
                        name="shared-data",
                        mount_path="/data"
                    )
                ]
            )
        ],
        volumes=[
            k8s.V1Volume(
                name="shared-data",
                empty_dir=k8s.V1EmptyDirVolumeSource()
            )
        ]
    )
)


# Configuration 5: Sidecar Container Pod
# Use case: Logging, monitoring, service mesh proxy
sidecar_pod = k8s.V1Pod(
    metadata=k8s.V1ObjectMeta(
        labels={"workload-type": "sidecar"}
    ),
    spec=k8s.V1PodSpec(
        containers=[
            # Main Airflow task container
            k8s.V1Container(
                name="base",
                volume_mounts=[
                    k8s.V1VolumeMount(
                        name="shared-logs",
                        mount_path="/shared/logs"
                    )
                ]
            ),
            # Sidecar for log collection
            k8s.V1Container(
                name="log-collector",
                image="fluent/fluent-bit:latest",
                args=[
                    "-c", "/fluent-bit/etc/fluent-bit.conf"
                ],
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "100m", "memory": "128Mi"},
                    limits={"cpu": "200m", "memory": "256Mi"}
                ),
                volume_mounts=[
                    k8s.V1VolumeMount(
                        name="shared-logs",
                        mount_path="/shared/logs",
                        read_only=True
                    )
                ]
            )
        ],
        volumes=[
            k8s.V1Volume(
                name="shared-logs",
                empty_dir=k8s.V1EmptyDirVolumeSource()
            )
        ]
    )
)


# =============================================================================
# DAG DEFINITION
# =============================================================================


@dag(
    dag_id="solution_8_2_custom_pods",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["solution", "kubernetes"],
    description="Demonstrates custom Kubernetes pod configurations",
)
def custom_pods_dag():
    """
    DAG demonstrating custom pod configurations for KubernetesExecutor.

    Each task showcases different Kubernetes customization patterns:
    1. Standard - default pod template
    2. High Memory - resource allocation
    3. GPU - node selection
    4. Secrets - environment injection
    5. Init Container - task preparation
    """

    @task
    def standard_task():
        """Task 1: Standard task using default pod configuration."""
        import os
        import platform

        print("=" * 60)
        print("TASK 1: STANDARD POD")
        print("=" * 60)
        print(f"Hostname: {os.environ.get('HOSTNAME', 'unknown')}")
        print(f"Platform: {platform.platform()}")
        print(f"Python: {platform.python_version()}")

        return {
            "task": "standard",
            "hostname": os.environ.get("HOSTNAME", "unknown"),
            "status": "completed"
        }

    @task(executor_config={"pod_override": high_memory_pod})
    def high_memory_task():
        """Task 2: High memory task with 4Gi allocation."""
        import os
        import resource
        import psutil

        print("=" * 60)
        print("TASK 2: HIGH MEMORY POD")
        print("=" * 60)

        # Check memory limits from cgroup (Kubernetes enforces via cgroups)
        try:
            with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
                cgroup_limit = int(f.read().strip())
                print(f"Cgroup memory limit: {cgroup_limit / (1024**3):.2f} GB")
        except FileNotFoundError:
            print("Cgroup v1 not available, trying v2...")
            try:
                with open("/sys/fs/cgroup/memory.max") as f:
                    content = f.read().strip()
                    if content == "max":
                        print("Memory limit: unlimited")
                    else:
                        cgroup_limit = int(content)
                        print(f"Cgroup memory limit: {cgroup_limit / (1024**3):.2f} GB")
            except FileNotFoundError:
                print("Memory cgroup info not available")

        # Resource limits from Python perspective
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        print(f"Python RLIMIT_AS - Soft: {soft}, Hard: {hard}")

        # Available system memory
        memory_info = psutil.virtual_memory()
        print(f"System memory total: {memory_info.total / (1024**3):.2f} GB")
        print(f"System memory available: {memory_info.available / (1024**3):.2f} GB")

        return {
            "task": "high_memory",
            "memory_requested": "4Gi",
            "memory_limit": "8Gi",
            "status": "completed"
        }

    @task(executor_config={"pod_override": gpu_pod})
    def gpu_task():
        """Task 3: GPU-enabled task."""
        import subprocess
        import os

        print("=" * 60)
        print("TASK 3: GPU POD")
        print("=" * 60)

        # Check CUDA environment
        cuda_visible = os.environ.get("CUDA_VISIBLE_DEVICES", "not set")
        print(f"CUDA_VISIBLE_DEVICES: {cuda_visible}")

        try:
            # Query GPU info
            result = subprocess.run(
                ["nvidia-smi", "--query-gpu=name,memory.total,driver_version",
                 "--format=csv,noheader"],
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )
            gpu_info = result.stdout.strip()
            print(f"GPU Info: {gpu_info}")
            return {
                "task": "gpu",
                "gpu_info": gpu_info,
                "status": "completed"
            }
        except FileNotFoundError:
            print("nvidia-smi not found - no GPU available")
            return {"task": "gpu", "gpu_info": None, "status": "no_gpu"}
        except subprocess.CalledProcessError as e:
            print(f"GPU query failed: {e}")
            return {"task": "gpu", "gpu_info": None, "status": "error"}
        except subprocess.TimeoutExpired:
            print("GPU query timed out")
            return {"task": "gpu", "gpu_info": None, "status": "timeout"}

    @task(executor_config={"pod_override": env_vars_pod})
    def secure_task():
        """Task 4: Task with secrets from Kubernetes."""
        import os

        print("=" * 60)
        print("TASK 4: SECURE POD WITH SECRETS")
        print("=" * 60)

        # Check for injected environment variables
        # NEVER log actual secret values!
        api_key = os.environ.get("API_KEY")
        db_password = os.environ.get("DATABASE_PASSWORD")
        log_level = os.environ.get("LOG_LEVEL", "not set")
        environment = os.environ.get("ENVIRONMENT", "not set")
        pod_name = os.environ.get("POD_NAME", "not set")
        pod_namespace = os.environ.get("POD_NAMESPACE", "not set")

        print(f"API_KEY present: {bool(api_key)}")
        print(f"DATABASE_PASSWORD present: {bool(db_password)}")
        print(f"LOG_LEVEL: {log_level}")
        print(f"ENVIRONMENT: {environment}")
        print(f"POD_NAME: {pod_name}")
        print(f"POD_NAMESPACE: {pod_namespace}")

        return {
            "task": "secure",
            "api_key_present": bool(api_key),
            "db_password_present": bool(db_password),
            "log_level": log_level,
            "environment": environment,
            "pod_name": pod_name,
            "status": "completed"
        }

    @task(executor_config={"pod_override": init_container_pod})
    def prepared_task():
        """Task 5: Task with init container preparation."""
        import os
        import json

        print("=" * 60)
        print("TASK 5: PREPARED POD WITH INIT CONTAINER")
        print("=" * 60)

        data_dir = "/data"

        # Check for files prepared by init containers
        sample_file = os.path.join(data_dir, "sample.json")
        init_log_file = os.path.join(data_dir, "init-log.txt")

        result = {
            "task": "prepared",
            "files_found": [],
            "status": "completed"
        }

        if os.path.exists(sample_file):
            print(f"Found sample data at {sample_file}")
            with open(sample_file) as f:
                sample_data = json.load(f)
                print(f"Sample data title: {sample_data.get('title', 'N/A')}")
                result["sample_data"] = sample_data
                result["files_found"].append("sample.json")
        else:
            print(f"Sample file not found at {sample_file}")

        if os.path.exists(init_log_file):
            print(f"Found init log at {init_log_file}")
            with open(init_log_file) as f:
                init_log = f.read()
                print(f"Init log: {init_log}")
                result["init_log"] = init_log
                result["files_found"].append("init-log.txt")
        else:
            print(f"Init log not found at {init_log_file}")

        # List all files in data directory
        if os.path.exists(data_dir):
            all_files = os.listdir(data_dir)
            print(f"All files in {data_dir}: {all_files}")
            result["all_files"] = all_files

        return result

    @task
    def summarize_results(results: list):
        """Summarize all task results."""
        print("=" * 60)
        print("SUMMARY OF ALL TASKS")
        print("=" * 60)

        for result in results:
            task_name = result.get("task", "unknown")
            status = result.get("status", "unknown")
            print(f"  {task_name}: {status}")

        return {"total_tasks": len(results), "all_completed": True}

    # Task execution flow
    result1 = standard_task()
    result2 = high_memory_task()
    result3 = gpu_task()
    result4 = secure_task()
    result5 = prepared_task()

    # Collect results and summarize
    summary = summarize_results([result1, result2, result3, result4, result5])


# Instantiate the DAG
custom_pods_dag()


# =============================================================================
# UTILITY: Generate and print pod YAML for debugging
# =============================================================================

if __name__ == "__main__":
    import json
    from kubernetes.client import ApiClient

    def print_pod_yaml(name, pod_config):
        """Print pod configuration as YAML-like JSON."""
        api_client = ApiClient()
        pod_dict = api_client.sanitize_for_serialization(pod_config)
        print(f"\n{'='*60}")
        print(f"POD CONFIGURATION: {name}")
        print("=" * 60)
        print(json.dumps(pod_dict, indent=2))

    # Print all configurations
    print_pod_yaml("High Memory Pod", high_memory_pod)
    print_pod_yaml("GPU Pod", gpu_pod)
    print_pod_yaml("Env Vars Pod", env_vars_pod)
    print_pod_yaml("Init Container Pod", init_container_pod)
    print_pod_yaml("Sidecar Pod", sidecar_pod)
