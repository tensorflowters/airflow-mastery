# Pod Security Standards for Airflow

This guide covers Kubernetes Pod Security Standards (PSS) configuration for Apache Airflow deployments.

## Overview

Kubernetes Pod Security Standards define three policy levels:

| Level | Description | Airflow Compatibility |
|-------|-------------|----------------------|
| **Privileged** | Unrestricted | ✅ Full compatibility |
| **Baseline** | Minimally restrictive | ✅ Compatible with configuration |
| **Restricted** | Heavily restricted | ⚠️ Requires modifications |

## Recommended Configuration

For production Airflow deployments, we recommend the **Baseline** level with additional hardening.

### Namespace-Level Enforcement

```yaml
# Apply to the airflow namespace
apiVersion: v1
kind: Namespace
metadata:
  name: airflow
  labels:
    # Pod Security Standards labels
    pod-security.kubernetes.io/enforce: baseline
    pod-security.kubernetes.io/enforce-version: latest
    pod-security.kubernetes.io/audit: restricted
    pod-security.kubernetes.io/audit-version: latest
    pod-security.kubernetes.io/warn: restricted
    pod-security.kubernetes.io/warn-version: latest
```

This configuration:
- **Enforces** baseline (blocks non-compliant pods)
- **Audits** restricted (logs violations)
- **Warns** restricted (shows warnings)

## Airflow Security Context Configuration

### Default Pod Template Security Context

```yaml
# In values.yaml or pod template
securityContext:
  runAsUser: 50000           # Airflow user
  runAsGroup: 0              # Root group (for volume permissions)
  runAsNonRoot: true         # Enforce non-root
  fsGroup: 0                 # File system group

containerSecurityContext:
  runAsUser: 50000
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: false  # Airflow needs writable filesystem
  capabilities:
    drop:
      - ALL
```

### Restricted-Compatible Configuration

For environments requiring **Restricted** level:

```yaml
securityContext:
  runAsUser: 50000
  runAsGroup: 50000          # Change from 0
  runAsNonRoot: true
  fsGroup: 50000             # Change from 0
  seccompProfile:
    type: RuntimeDefault

containerSecurityContext:
  runAsUser: 50000
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  readOnlyRootFilesystem: false
  capabilities:
    drop:
      - ALL
  seccompProfile:
    type: RuntimeDefault
```

**Note:** Restricted level requires:
- `seccompProfile` set explicitly
- `runAsGroup` and `fsGroup` as non-root
- This may require volume permission adjustments

## Component-Specific Considerations

### Webserver / API Server

```yaml
# Baseline compatible
securityContext:
  runAsUser: 50000
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: [ALL]
```

### Scheduler

```yaml
# Needs access to create pods for KubernetesExecutor
securityContext:
  runAsUser: 50000
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: [ALL]
```

### Workers

```yaml
# May need additional capabilities depending on tasks
securityContext:
  runAsUser: 50000
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: [ALL]
    # Add back if needed for specific tasks:
    # add: [NET_BIND_SERVICE]
```

### Triggerer

```yaml
# Minimal requirements
securityContext:
  runAsUser: 50000
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: [ALL]
```

### Init Containers (wait-for-airflow-migrations)

```yaml
# Same as main container
securityContext:
  runAsUser: 50000
  runAsNonRoot: true
  allowPrivilegeEscalation: false
```

## Common Issues and Solutions

### Issue: Permission Denied on Volumes

**Symptom:** Pods fail to start with permission errors on mounted volumes.

**Solution:** Ensure `fsGroup` matches the expected group:

```yaml
securityContext:
  fsGroup: 0  # Or 50000 for restricted mode
```

For hostPath volumes (development only):
```bash
# On the node
sudo chown -R 50000:0 /path/to/volume
```

### Issue: Git-Sync Container Fails

**Symptom:** Git-sync sidecar cannot clone repositories.

**Solution:** Git-sync runs as user 65533 by default:

```yaml
# In git-sync container spec
securityContext:
  runAsUser: 65533
  runAsNonRoot: true
```

### Issue: Logs Directory Not Writable

**Symptom:** Workers cannot write logs.

**Solution:** Ensure logs volume has correct permissions:

```yaml
volumes:
  - name: logs
    emptyDir: {}  # Or PVC with correct permissions
```

## Helm Chart Configuration

### values.yaml Security Settings

```yaml
# Security configurations for Airflow Helm chart
airflowSecurityContext:
  runAsUser: 50000
  runAsNonRoot: true
  fsGroup: 0

webserver:
  securityContext:
    runAsUser: 50000
    runAsNonRoot: true
    allowPrivilegeEscalation: false

scheduler:
  securityContext:
    runAsUser: 50000
    runAsNonRoot: true
    allowPrivilegeEscalation: false

workers:
  securityContext:
    runAsUser: 50000
    runAsNonRoot: true
    allowPrivilegeEscalation: false

triggerer:
  securityContext:
    runAsUser: 50000
    runAsNonRoot: true
    allowPrivilegeEscalation: false
```

## Validation Commands

### Check Pod Security Violations

```bash
# Check for pods that would violate restricted policy
kubectl get pods -n airflow -o yaml | \
  kubectl apply --dry-run=server -f - 2>&1 | grep -i "forbidden"

# View audit logs for PSS violations
kubectl logs -n kube-system -l component=kube-apiserver | \
  grep -i "pod-security"
```

### Verify Security Context

```bash
# Check actual security context of running pods
kubectl get pods -n airflow -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.securityContext}{"\n"}{end}'

# Detailed security info for a specific pod
kubectl get pod <pod-name> -n airflow -o yaml | grep -A 20 "securityContext"
```

### Test Baseline Compliance

```bash
# Dry-run deployment with baseline enforcement
kubectl label namespace airflow \
  pod-security.kubernetes.io/enforce=baseline \
  --dry-run=client -o yaml | kubectl apply -f -

# Then check if pods can still be scheduled
kubectl get events -n airflow | grep -i "forbidden"
```

## Migration Path

### From Privileged to Baseline

1. **Audit current state:**
   ```bash
   kubectl label namespace airflow \
     pod-security.kubernetes.io/audit=baseline
   ```

2. **Review audit logs for violations**

3. **Update Helm values with security contexts**

4. **Test in staging environment**

5. **Enable enforcement:**
   ```bash
   kubectl label namespace airflow \
     pod-security.kubernetes.io/enforce=baseline
   ```

### From Baseline to Restricted

1. Add `seccompProfile: RuntimeDefault` to all containers
2. Change `fsGroup` and `runAsGroup` from 0 to 50000
3. Update volume permissions accordingly
4. Test thoroughly in staging

## Best Practices

1. **Start with Baseline, audit Restricted**
   - Enforces reasonable defaults
   - Shows what needs work for Restricted

2. **Use read-only root filesystem where possible**
   - Airflow needs writable for some operations
   - Consider mounting specific paths read-write

3. **Drop all capabilities by default**
   - Add back only what's needed
   - Document why each capability is required

4. **Set resource limits**
   - Prevents resource exhaustion attacks
   - Enables fair scheduling

5. **Use network policies alongside PSS**
   - PSS controls pod configuration
   - NetworkPolicies control pod communication

## References

- [Kubernetes Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/)
- [Airflow Security Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/security/index.html)
- [Airflow Helm Chart Security](https://airflow.apache.org/docs/helm-chart/stable/parameters-ref.html#security)
