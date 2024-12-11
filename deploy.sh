#!/bin/bash

set -e

# Configuration
DOCKER_IMAGE="marianbasti/buenarda-worker:latest"
WORKERS_PER_INDEX=3
STORAGE_SIZE="100Gi"
NAMESPACE="default"

# Check prerequisites
check_command() {
    if ! command -v $1 &> /dev/null; then
        echo "Error: $1 is required but not installed."
        exit 1
    fi
}

check_prerequisites() {
    echo "Checking prerequisites..."
    check_command kubectl
    check_command docker
    
    if ! kubectl cluster-info &> /dev/null; then
        echo "Error: Cannot connect to Kubernetes cluster"
        exit 1
    fi
}

# Create persistent volume and claim
create_storage() {
    echo "Creating storage resources..."
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: crawler-data-pvc
  namespace: ${NAMESPACE}
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: ${STORAGE_SIZE}
  storageClassName: local-path
EOF
    
    echo "Waiting for PVC to be bound..."
    kubectl wait --for=condition=bound pvc/crawler-data-pvc -n ${NAMESPACE} --timeout=30s || {
        echo "Error: PVC failed to bind within timeout"
        kubectl get pvc crawler-data-pvc -n ${NAMESPACE} -o yaml
        exit 1
    }
    
    echo "PVC created and bound successfully"
}

# Deploy crawler jobs
deploy_jobs() {
    echo "Deploying crawler jobs..."
    # Add PYTHONPATH to include project root
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    export PYTHONPATH="${SCRIPT_DIR}:${PYTHONPATH}"
    
    # Run with module notation
    python3 -m scripts.buenarda_job_controller --workers ${WORKERS_PER_INDEX}
}

# Main deployment flow
main() {
    echo "Starting deployment..."
    
    check_prerequisites
    
    # Create namespace if it doesn't exist
    kubectl create namespace ${NAMESPACE} --dry-run=client -o yaml | kubectl apply -f -
    
    create_storage
    deploy_jobs
    
    echo "Deployment completed successfully!"
    echo "Monitor jobs with: kubectl get jobs -n ${NAMESPACE}"
    echo "View logs with: kubectl logs -f job/buenarda-crawler-* -n ${NAMESPACE}"
}

main "$@"