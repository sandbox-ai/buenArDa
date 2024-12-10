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
    
    # Create a test pod to validate PVC
    echo "Creating test pod to validate PVC..."
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: pvc-test
  namespace: ${NAMESPACE}
spec:
  containers:
  - name: pvc-test
    image: busybox
    command: ['sh', '-c', 'echo "Testing PVC" > /test/test.txt && sleep 5']
    volumeMounts:
    - name: crawler-data
      mountPath: /test
  volumes:
  - name: crawler-data
    persistentVolumeClaim:
      claimName: crawler-data-pvc
  restartPolicy: Never
EOF

    echo "Waiting for test pod to complete..."
    kubectl wait --for=condition=ready pod/pvc-test --timeout=30s || true
    kubectl wait --for=condition=complete pod/pvc-test --timeout=30s || true
    kubectl delete pod pvc-test --ignore-not-found
}

# Deploy crawler jobs
deploy_jobs() {
    echo "Deploying crawler jobs..."
    python3 scripts/buenarda_job_controller.py --workers ${WORKERS_PER_INDEX}
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