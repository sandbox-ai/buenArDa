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

get_node_name() {
    NODE_NAME=$(kubectl get nodes --no-headers -o custom-columns=":metadata.name" | head -n 1)
    if [ -z "$NODE_NAME" ]; then
        echo "Error: Could not get node name"
        exit 1
    fi
    echo $NODE_NAME
}

create_persistent_volume() {
    echo "Creating persistent volume..."
    NODE_NAME=$(get_node_name)
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: crawler-data-pv
spec:
  capacity:
    storage: ${STORAGE_SIZE}
  accessModes:
    - ReadWriteMany
  persistentVolumeReclaimPolicy: Retain
  storageClassName: local-path
  local:
    path: ./data
  nodeAffinity:
    required:
      nodeSelectorTerms:
      - matchExpressions:
        - key: kubernetes.io/hostname
          operator: In
          values:
          - ${NODE_NAME}
EOF

    mkdir -p ./data
}

# Create persistent volume and claim
create_storage_with_pod() {
    # Create test pod first
    cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: volume-test
  namespace: ${NAMESPACE}
spec:
  containers:
  - name: volume-test
    image: busybox
    command: ["sleep", "infinity"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: crawler-data-pvc
EOF

    # Now create PVC
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

    # Wait for binding
    kubectl wait --for=condition=bound pvc/crawler-data-pvc -n ${NAMESPACE} --timeout=30s
    
    # Clean up test pod
    kubectl delete pod volume-test -n ${NAMESPACE}
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
    create_persistent_volume
    create_storage_with_pod
    deploy_jobs
    
    echo "Deployment completed successfully!"
    echo "Monitor jobs with: kubectl get jobs -n ${NAMESPACE}"
    echo "View logs with: kubectl logs -f job/buenarda-crawler-* -n ${NAMESPACE}"
}

main "$@"