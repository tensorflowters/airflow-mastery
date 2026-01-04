#!/bin/bash
# Airflow Helm Deployment Script
# Usage: ./deploy.sh [namespace] [release-name]

set -e

NAMESPACE="${1:-airflow}"
RELEASE="${2:-airflow}"
VALUES_FILE="values.yaml"
CHART="apache-airflow/airflow"

echo "================================================"
echo "Deploying Apache Airflow 3 to Kubernetes"
echo "================================================"
echo "Namespace: $NAMESPACE"
echo "Release:   $RELEASE"
echo ""

# Check prerequisites
echo "Checking prerequisites..."
command -v kubectl >/dev/null 2>&1 || { echo "kubectl required but not found"; exit 1; }
command -v helm >/dev/null 2>&1 || { echo "helm required but not found"; exit 1; }

# Check cluster connectivity
kubectl cluster-info >/dev/null 2>&1 || { echo "Cannot connect to Kubernetes cluster"; exit 1; }
echo "✓ Kubernetes cluster accessible"

# Add/update Helm repo
echo ""
echo "Updating Helm repository..."
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo update

# Create namespace if not exists
echo ""
echo "Ensuring namespace exists..."
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

# Generate secrets if they don't exist
echo ""
echo "Checking secrets..."

# Check/create Fernet key secret (for connection/variable encryption)
if ! kubectl get secret airflow-fernet-key -n "$NAMESPACE" >/dev/null 2>&1; then
    echo "Creating Fernet key secret..."
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
    kubectl create secret generic airflow-fernet-key \
        --from-literal=fernet-key="$FERNET_KEY" \
        -n "$NAMESPACE"
    echo "✓ Fernet key secret created"
else
    echo "✓ Fernet key secret exists"
fi

# Check/create webserver secret (for session management)
if ! kubectl get secret airflow-webserver-secret -n "$NAMESPACE" >/dev/null 2>&1; then
    echo "Creating webserver secret..."
    # Use 256-bit entropy (32 hex bytes) for production security
    WEBSERVER_SECRET=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    kubectl create secret generic airflow-webserver-secret \
        --from-literal=webserver-secret-key="$WEBSERVER_SECRET" \
        -n "$NAMESPACE"
    echo "✓ Webserver secret created"
else
    echo "✓ Webserver secret exists"
fi

# Check for database secret (required for production)
if ! kubectl get secret airflow-database-secret -n "$NAMESPACE" >/dev/null 2>&1; then
    echo ""
    echo "⚠️  WARNING: Database secret 'airflow-database-secret' not found!"
    echo "   For production deployments, create it with:"
    echo ""
    echo "   kubectl create secret generic airflow-database-secret \\"
    echo "       --from-literal=connection='postgresql://user:password@host:5432/airflow' \\"
    echo "       -n $NAMESPACE"
    echo ""
    echo "   Continuing with embedded PostgreSQL (development only)..."
else
    echo "✓ Database secret exists"
fi

# Deploy with Helm
echo ""
echo "Deploying Airflow..."
helm upgrade --install "$RELEASE" "$CHART" \
    --namespace "$NAMESPACE" \
    -f "$VALUES_FILE" \
    --timeout 10m \
    --wait

# Show status
echo ""
echo "================================================"
echo "Deployment Complete!"
echo "================================================"
kubectl get pods -n "$NAMESPACE"

echo ""
echo "To access the UI, run:"
echo "  kubectl port-forward svc/airflow-api-server 8080:8080 -n $NAMESPACE"
echo ""
echo "Default credentials: admin / admin"
echo "(or check the values.yaml for custom credentials)"
