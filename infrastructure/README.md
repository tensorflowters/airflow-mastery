# Infrastructure Configuration

This directory contains infrastructure configurations for Apache Airflow 3.x deployment.

## Directory Structure

```
infrastructure/
├── docker-compose/     # Local development environment
│   ├── docker-compose.yml
│   └── .env.example    # Environment variable template
├── helm/               # Kubernetes deployment
│   ├── values.yaml           # Development values (INSECURE)
│   └── values-production.yaml # Production values (SECURE)
├── kubernetes/         # Additional K8s resources
│   └── pod-templates/  # Custom pod templates for KubernetesExecutor
└── scripts/            # Deployment automation scripts
```

## Quick Start - Local Development

### Using Docker Compose

```bash
# Navigate to docker-compose directory
cd infrastructure/docker-compose

# Copy environment template
cp .env.example .env

# (Optional) Generate secure keys - recommended even for development
python3 -c "from cryptography.fernet import Fernet; print(f'AIRFLOW_FERNET_KEY={Fernet.generate_key().decode()}')" >> .env
python3 -c "import secrets; print(f'AIRFLOW_WEBSERVER_SECRET_KEY={secrets.token_hex(32)}')" >> .env

# Start Airflow
docker-compose up -d

# Access UI
open http://localhost:8080
# Default login: admin / admin
```

### Using Helm (Development)

```bash
# Add Airflow Helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

# Create namespace
kubectl create namespace airflow

# Deploy with development values
helm upgrade --install airflow apache-airflow/airflow \
  -f infrastructure/helm/values.yaml \
  -n airflow

# Port forward to access UI
kubectl port-forward svc/airflow-webserver 8080:8080 -n airflow
```

## Production Deployment

### Prerequisites

Before deploying to production, ensure you have:

1. **External PostgreSQL** - Managed database service (RDS, Cloud SQL, Azure Database)
2. **TLS Certificates** - Valid certificates for HTTPS
3. **Ingress Controller** - nginx-ingress or similar installed
4. **Secrets Management** - Secure storage for credentials

### Generate Security Keys

```bash
# Fernet key (for encrypting connections and variables)
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Webserver secret key (for session management)
python3 -c "import secrets; print(secrets.token_hex(32))"

# Strong database password
python3 -c "import secrets; print(secrets.token_urlsafe(32))"
```

### Create Kubernetes Secrets

```bash
NAMESPACE=airflow

# 1. Database connection
kubectl create secret generic airflow-database-secret \
  --from-literal=connection='postgresql+psycopg2://user:password@host:5432/airflow' \
  -n $NAMESPACE

# 2. Fernet key
FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
kubectl create secret generic airflow-fernet-key \
  --from-literal=fernet-key="$FERNET_KEY" \
  -n $NAMESPACE

# 3. Webserver secret
SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
kubectl create secret generic airflow-webserver-secret \
  --from-literal=webserver-secret-key="$SECRET_KEY" \
  -n $NAMESPACE

# 4. Git SSH key (for DAG sync)
kubectl create secret generic airflow-git-ssh-secret \
  --from-file=gitSshKey=/path/to/your/ssh/key \
  -n $NAMESPACE
```

### Deploy Production Configuration

```bash
# Deploy with production values
helm upgrade --install airflow apache-airflow/airflow \
  -f infrastructure/helm/values-production.yaml \
  -n airflow \
  --set ingress.hosts[0].name=airflow.your-domain.com

# Verify deployment
kubectl get pods -n airflow
kubectl get ingress -n airflow
```

## Security Checklist

### Development Environment

- [x] Credentials use environment variables (docker-compose)
- [x] Database isolated in Docker network
- [ ] Change default admin password after first login

### Production Environment

- [ ] External managed database (not embedded PostgreSQL)
- [ ] Fernet key generated and stored in Kubernetes secret
- [ ] Webserver secret generated and stored in Kubernetes secret
- [ ] Strong database password (32+ characters)
- [ ] TLS/HTTPS enabled via Ingress
- [ ] RBAC configured for Kubernetes resources
- [ ] Service accounts with minimal permissions
- [ ] Network policies restricting pod communication
- [ ] Pod security policies/standards enforced
- [ ] Audit logging enabled
- [ ] Monitoring and alerting configured
- [ ] Regular secret rotation procedure documented
- [ ] Backup and recovery tested

## Pod Templates

Custom pod templates for the KubernetesExecutor are located in `kubernetes/pod-templates/`:

| Template | Use Case | Resources |
|----------|----------|-----------|
| `default-pod.yaml` | Standard tasks | 0.25-1 CPU, 256Mi-1Gi RAM |
| `high-memory-pod.yaml` | Data processing, ML | 2-4 CPU, 4-8Gi RAM |

### Using Custom Pod Templates

```python
from airflow.sdk import dag, task

@dag(dag_id="custom_resources")
def my_dag():
    @task(executor_config={
        "pod_template_file": "/path/to/high-memory-pod.yaml"
    })
    def memory_intensive_task():
        # Processing large datasets
        pass

    memory_intensive_task()

my_dag()
```

## Secret Rotation

Regular rotation of secrets is a security best practice. This section documents how to safely rotate Airflow secrets.

### Fernet Key Rotation

The Fernet key encrypts connections and variables in the database. Airflow supports multi-key rotation:

```bash
# 1. Generate a new Fernet key
NEW_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

# 2. Get current key from secret
OLD_KEY=$(kubectl get secret airflow-fernet-key -n airflow -o jsonpath='{.data.fernet-key}' | base64 -d)

# 3. Update secret with BOTH keys (new key first, old key second)
# Airflow will encrypt with new key, decrypt with either
kubectl create secret generic airflow-fernet-key \
  --from-literal=fernet-key="${NEW_KEY},${OLD_KEY}" \
  -n airflow \
  --dry-run=client -o yaml | kubectl apply -f -

# 4. Restart scheduler and webserver to pick up new key
kubectl rollout restart deployment/airflow-scheduler -n airflow
kubectl rollout restart deployment/airflow-webserver -n airflow

# 5. Re-encrypt all connections and variables with new key
# This can be done via Airflow CLI or UI

# 6. After verification (24-48 hours), remove old key
kubectl create secret generic airflow-fernet-key \
  --from-literal=fernet-key="${NEW_KEY}" \
  -n airflow \
  --dry-run=client -o yaml | kubectl apply -f -
```

**Important:** Keep both keys during transition to avoid decryption failures.

### Webserver Secret Key Rotation

The webserver secret key manages user sessions:

```bash
# 1. Generate new secret
NEW_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")

# 2. Update Kubernetes secret
kubectl create secret generic airflow-webserver-secret \
  --from-literal=webserver-secret-key="${NEW_SECRET}" \
  -n airflow \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Restart webserver
kubectl rollout restart deployment/airflow-webserver -n airflow
```

**Note:** This will invalidate all existing user sessions. Users will need to log in again.

### Database Password Rotation

```bash
# 1. Update password in your database service (RDS, Cloud SQL, etc.)

# 2. Update the Kubernetes secret with new connection string
kubectl create secret generic airflow-database-secret \
  --from-literal=connection='postgresql+psycopg2://user:NEW_PASSWORD@host:5432/airflow' \
  -n airflow \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Restart all Airflow components
kubectl rollout restart deployment/airflow-scheduler -n airflow
kubectl rollout restart deployment/airflow-webserver -n airflow
kubectl rollout restart deployment/airflow-triggerer -n airflow
# Workers will pick up new config on next task
```

### Rotation Schedule Recommendations

| Secret | Rotation Frequency | Impact |
|--------|-------------------|--------|
| Fernet Key | Annually | Low (with multi-key rotation) |
| Webserver Secret | Quarterly | Medium (session invalidation) |
| Database Password | Quarterly | Low (with proper procedure) |
| Git SSH Key | Annually | Low (update in Git provider) |

### Rotation Automation

Consider using:
- **HashiCorp Vault** with Kubernetes auth for dynamic secrets
- **AWS Secrets Manager** with automatic rotation
- **External Secrets Operator** for Kubernetes secret sync

Example with External Secrets:
```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: airflow-fernet-key
  namespace: airflow
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: airflow-fernet-key
  data:
    - secretKey: fernet-key
      remoteRef:
        key: airflow/fernet-key
```

## Troubleshooting

### Common Issues

**Scheduler not processing DAGs:**
```bash
# Check scheduler logs
kubectl logs -f deployment/airflow-scheduler -n airflow

# Verify DAG folder mounted
kubectl exec -it deployment/airflow-scheduler -n airflow -- ls /opt/airflow/dags
```

**Database connection errors:**
```bash
# Verify secret exists
kubectl get secret airflow-database-secret -n airflow -o yaml

# Test connection from pod
kubectl exec -it deployment/airflow-webserver -n airflow -- \
  python -c "from airflow.configuration import conf; print(conf.get('database', 'sql_alchemy_conn'))"
```

**Worker pods failing:**
```bash
# Check pod events
kubectl describe pod <worker-pod-name> -n airflow

# Check worker logs
kubectl logs <worker-pod-name> -n airflow
```

## Version Information

| Component | Version |
|-----------|---------|
| Apache Airflow | 3.1.5 |
| Helm Chart | 1.18.0+ |
| PostgreSQL | 15 |
| Python | 3.9+ |

## Related Documentation

- [Airflow 3 Migration Guide](../docs/airflow3-k8s-guide.md)
- [Module 08: Kubernetes Executor](../modules/08-kubernetes-executor/README.md)
- [Module 09: Production Patterns](../modules/09-production-patterns/README.md)
- [Official Airflow Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html)
