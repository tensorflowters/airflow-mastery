# Exercise 8.3: Production Deployment Checklist

## Objective

Prepare a production-ready Airflow deployment on Kubernetes by configuring external databases, git-sync DAG deployment, remote logging, connection pooling, and comprehensive secrets management.

## Background

Production Airflow deployments require careful attention to:

| Area | Development | Production |
|------|-------------|------------|
| Database | Built-in PostgreSQL | External managed DB |
| DAG Sync | Manual copy | Git-sync with CI/CD |
| Logging | Local files | Remote storage (S3/GCS) |
| Secrets | Plain text | Kubernetes Secrets + External vaults |
| Scaling | Single replica | HA with PgBouncer |
| Monitoring | None | StatsD + Prometheus |

## Requirements

Complete each section of this production checklist, documenting your configuration choices.

---

### Part 1: External PostgreSQL Configuration

**Why**: Built-in PostgreSQL lacks persistence, backups, and HA.

**Task**: Configure Airflow to use an external PostgreSQL database.

1. **Create database secret**:
```bash
# Create the secret with your connection string
kubectl create secret generic airflow-database-secret \
  --namespace airflow \
  --from-literal=connection='postgresql://airflow:YOUR_PASSWORD@postgres-host.example.com:5432/airflow?sslmode=require'
```

2. **Update values.yaml**:
```yaml
# Disable built-in PostgreSQL
postgresql:
  enabled: false

# Reference external database
data:
  metadataSecretName: airflow-database-secret
  # For Airflow 3.x, ensure connection format is correct
  # postgresql+psycopg2://... or postgresql://...
```

3. **Verify connection**:
```bash
# Test from scheduler pod
kubectl exec -n airflow deployment/airflow-scheduler -- \
  airflow db check
```

**Documentation**: Record your database configuration choices:
- [ ] Database provider (AWS RDS, GCP Cloud SQL, Azure, self-hosted)
- [ ] SSL/TLS enabled
- [ ] Connection pooling at DB level
- [ ] Backup strategy

---

### Part 2: Git-Sync DAG Deployment

**Why**: Enables GitOps workflow, version control, and CI/CD integration.

**Task**: Configure git-sync to automatically pull DAGs from a repository.

1. **Generate SSH key for git access**:
```bash
# Generate deploy key
ssh-keygen -t ed25519 -C "airflow-deploy" -f airflow_deploy_key

# Create Kubernetes secret
kubectl create secret generic airflow-git-ssh-secret \
  --namespace airflow \
  --from-file=gitSshKey=airflow_deploy_key
```

2. **Add deploy key to your repository** (read-only access)

3. **Update values.yaml**:
```yaml
dags:
  persistence:
    enabled: false  # Using git-sync instead

  gitSync:
    enabled: true
    repo: "git@github.com:your-org/airflow-dags.git"
    branch: main
    rev: HEAD
    subPath: "dags"  # Path to DAGs in repo

    # Authentication
    sshKeySecret: airflow-git-ssh-secret

    # Sync configuration
    wait: 60  # Sync interval in seconds
    containerName: git-sync

    # For HTTPS repos (alternative to SSH)
    # credentialsSecret: airflow-git-credentials
    # knownHosts: |
    #   github.com ssh-rsa AAAA...
```

4. **Verify sync**:
```bash
# Check git-sync container logs
kubectl logs -n airflow deployment/airflow-scheduler -c git-sync -f

# Verify DAGs are loaded
kubectl exec -n airflow deployment/airflow-scheduler -- \
  airflow dags list
```

**Documentation**:
- [ ] Repository URL
- [ ] Branch strategy (main, staging, prod branches?)
- [ ] Sync interval choice rationale
- [ ] CI/CD pipeline for DAG validation

---

### Part 3: Remote Logging (S3/GCS)

**Why**: Task pods are ephemeral; logs must persist externally.

**Task**: Configure remote logging to cloud storage.

**For AWS S3**:
```yaml
# values.yaml
config:
  logging:
    remote_logging: "True"
    remote_log_conn_id: "aws_default"
    remote_base_log_folder: "s3://my-airflow-logs-bucket/logs"
    # Optional: encrypt logs
    encrypt_s3_logs: "True"

# Create AWS credentials secret
# Option 1: Access keys (not recommended for production)
kubectl create secret generic aws-credentials \
  --namespace airflow \
  --from-literal=aws-access-key-id=AKIAXXXXXXXX \
  --from-literal=aws-secret-access-key=XXXXXXXXXX

# Option 2: Use IRSA (IAM Roles for Service Accounts) - RECOMMENDED
# Configure serviceAccount with IAM role annotation
```

**For GCP GCS**:
```yaml
config:
  logging:
    remote_logging: "True"
    remote_log_conn_id: "google_cloud_default"
    remote_base_log_folder: "gs://my-airflow-logs-bucket/logs"

# Create GCP credentials
kubectl create secret generic gcp-credentials \
  --namespace airflow \
  --from-file=key.json=service-account-key.json
```

**Verify logging**:
```bash
# Trigger a task and check logs appear in cloud storage
# Check for errors in scheduler logs
kubectl logs -n airflow deployment/airflow-scheduler -c scheduler | grep -i logging
```

**Documentation**:
- [ ] Storage provider and bucket name
- [ ] Log retention policy
- [ ] Access control (IAM roles, service accounts)
- [ ] Log encryption configuration

---

### Part 4: Enable PgBouncer (Connection Pooling)

**Why**: Prevents database connection exhaustion under load.

**Task**: Enable and configure PgBouncer.

```yaml
# values.yaml
pgbouncer:
  enabled: true

  # Pool sizing
  maxClientConn: 100
  metadataPoolSize: 10
  resultBackendPoolSize: 5

  # Authentication
  auth_type: scram-sha-256

  # Resources
  resources:
    requests:
      cpu: 100m
      memory: 128Mi
    limits:
      cpu: 500m
      memory: 256Mi
```

**Verify PgBouncer**:
```bash
# Check PgBouncer is running
kubectl get pods -n airflow | grep pgbouncer

# View PgBouncer stats
kubectl exec -n airflow deployment/airflow-pgbouncer -- \
  psql -h localhost -p 6543 -U pgbouncer pgbouncer -c "SHOW POOLS"
```

**Documentation**:
- [ ] Pool size calculations
- [ ] Expected concurrent connections
- [ ] Monitoring metrics to watch

---

### Part 5: Security & Secrets Management

**Task**: Create and document all required secrets.

**Required Secrets Checklist**:

```bash
# 1. Webserver secret key (session security)
kubectl create secret generic airflow-webserver-secret \
  --namespace airflow \
  --from-literal=webserver-secret-key=$(openssl rand -hex 32)

# 2. Fernet key (connection encryption)
FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
kubectl create secret generic airflow-fernet-secret \
  --namespace airflow \
  --from-literal=fernet-key=$FERNET_KEY

# 3. Database connection (from Part 1)
# Already created: airflow-database-secret

# 4. Git SSH key (from Part 2)
# Already created: airflow-git-ssh-secret

# 5. Cloud provider credentials (from Part 3)
# AWS: aws-credentials or IRSA
# GCP: gcp-credentials or Workload Identity
```

**Reference secrets in values.yaml**:
```yaml
webserverSecretKeySecretName: airflow-webserver-secret
fernetKeySecretName: airflow-fernet-secret
```

**Documentation**:
- [ ] List all secrets created
- [ ] Rotation policy for each
- [ ] Access control (RBAC)
- [ ] Consider external secrets (HashiCorp Vault, AWS Secrets Manager)

---

### Part 6: High Availability Configuration

**Task**: Configure scheduler and webserver for HA.

```yaml
# values.yaml
scheduler:
  replicas: 2  # HA - active-active in Airflow 3.x

  # Leader election for deduplication
  extraEnv:
    - name: AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK
      value: "True"

  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: 2
      memory: 4Gi

  # Pod disruption budget
  podDisruptionBudget:
    enabled: true
    minAvailable: 1

webserver:
  replicas: 2

  resources:
    requests:
      cpu: 250m
      memory: 512Mi
    limits:
      cpu: 1
      memory: 2Gi

  podDisruptionBudget:
    enabled: true
    minAvailable: 1
```

---

### Part 7: Monitoring Setup

**Task**: Configure metrics export.

```yaml
# values.yaml

# StatsD metrics
statsd:
  enabled: true

config:
  metrics:
    statsd_on: "True"
    statsd_host: "localhost"
    statsd_port: "8125"
    statsd_prefix: "airflow"

# Or OpenTelemetry (Airflow 3.x)
extraEnv:
  - name: AIRFLOW__METRICS__OTEL_ON
    value: "True"
  - name: AIRFLOW__METRICS__OTEL_HOST
    value: "otel-collector.monitoring"
  - name: AIRFLOW__METRICS__OTEL_PORT
    value: "4318"
```

---

## Deliverables

Create a production deployment package:

```
production-deployment/
├── values-production.yaml      # Complete Helm values
├── secrets/
│   └── secrets-manifest.yaml   # Secret creation commands (sanitized)
├── RUNBOOK.md                  # Operational runbook
└── CHECKLIST.md                # Completed checklist with notes
```

### CHECKLIST.md Template

```markdown
# Production Deployment Checklist

## Pre-Deployment
- [ ] Database provisioned and tested
- [ ] Storage bucket created with correct permissions
- [ ] Git repository with deploy key configured
- [ ] All secrets created in Kubernetes

## Database
- [ ] Provider: _______________
- [ ] SSL enabled: Yes / No
- [ ] Backup frequency: _______________
- [ ] Connection string verified: Yes / No

## DAG Synchronization
- [ ] Repository: _______________
- [ ] Branch: _______________
- [ ] Sync interval: ___ seconds
- [ ] CI/CD pipeline configured: Yes / No

## Logging
- [ ] Storage provider: _______________
- [ ] Bucket: _______________
- [ ] Retention: ___ days
- [ ] Encryption: Yes / No

## Connection Pooling
- [ ] PgBouncer enabled: Yes / No
- [ ] Max connections: ___
- [ ] Pool size: ___

## Secrets
| Secret Name | Purpose | Rotation Policy |
|-------------|---------|-----------------|
| airflow-database-secret | DB connection | Quarterly |
| airflow-webserver-secret | Session security | Annual |
| airflow-fernet-secret | Encryption | Never (break existing) |
| airflow-git-ssh-secret | DAG sync | Annual |

## High Availability
- [ ] Scheduler replicas: ___
- [ ] Webserver replicas: ___
- [ ] PDB configured: Yes / No

## Monitoring
- [ ] Metrics enabled: Yes / No
- [ ] Exporter: StatsD / OTEL
- [ ] Dashboards created: Yes / No

## Post-Deployment Verification
- [ ] All pods healthy
- [ ] DAGs synced successfully
- [ ] Test task executed
- [ ] Logs appearing in remote storage
- [ ] Metrics visible in monitoring
```

---

## Success Criteria

- [ ] Complete values-production.yaml with all configurations
- [ ] All secrets created and referenced correctly
- [ ] Git-sync working with your DAG repository
- [ ] Remote logging verified in cloud storage
- [ ] PgBouncer operational
- [ ] HA configuration applied
- [ ] Monitoring metrics flowing
- [ ] Comprehensive documentation in CHECKLIST.md

---

## Hints

<details>
<summary>Hint 1: Testing database connection</summary>

```bash
# Test from local machine first
psql "postgresql://user:pass@host:5432/airflow?sslmode=require"

# Or use Kubernetes job
kubectl run pg-test --rm -it --restart=Never \
  --image=postgres:15 -- \
  psql "postgresql://user:pass@host:5432/airflow"
```

</details>

<details>
<summary>Hint 2: Debugging git-sync</summary>

```bash
# Check git-sync logs
kubectl logs -n airflow deployment/airflow-scheduler -c git-sync

# Common issues:
# - SSH key permissions: must be 0600
# - Known hosts not configured
# - Wrong branch name
```

</details>

<details>
<summary>Hint 3: Verifying remote logging</summary>

```bash
# Trigger a simple DAG
kubectl exec -n airflow deployment/airflow-scheduler -- \
  airflow dags trigger example_bash_operator

# Check cloud storage for logs
aws s3 ls s3://my-bucket/logs/
# OR
gsutil ls gs://my-bucket/logs/
```

</details>

---

## Additional Challenge

Implement external secrets management using one of:

1. **HashiCorp Vault**: External Secrets Operator with Vault backend
2. **AWS Secrets Manager**: ESO with AWS provider
3. **Azure Key Vault**: ESO with Azure provider

```yaml
# Example: External Secrets Operator
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: airflow-database-secret
  namespace: airflow
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: airflow-database-secret
  data:
    - secretKey: connection
      remoteRef:
        key: airflow/database
        property: connection_string
```

---

## References

- [Apache Airflow Helm Chart Documentation](https://airflow.apache.org/docs/helm-chart/stable/index.html)
- [Production Deployment Guide](https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html)
- [Kubernetes Executor Configuration](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/executor/kubernetes.html)
