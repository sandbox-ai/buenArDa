#!/bin/bash

set -e

# Configuration
DOCKER_IMAGE="marianbasti/buenarda-worker:latest"
WORKERS_PER_INDEX=3
STORAGE_SIZE="100Gi"
NAMESPACE="default"
NFS_PATH="/mnt/buenarda"

while getopts "i:" opt; do
  case $opt in
    i) NFS_SERVER="$OPTARG";;
    *) echo "Usage: $0 -i <nfs_server_ip>" >&2; exit 1;;
  esac
done

if [ -z "$NFS_SERVER" ]; then
    echo "Error: NFS server IP required. Usage: $0 -i <nfs_server_ip>" >&2
    exit 1
fi

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
  storageClassName: nfs-storage
  nfs:
    server: ${NFS_SERVER}
    path: /mnt/buenarda
EOF
}

setup_nfs_storage_class() {
    echo "Creating NFS StorageClass..."
    cat <<EOF | kubectl apply -f -
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: nfs-storage
provisioner: kubernetes.io/nfs
parameters:
  server: ${NFS_SERVER}
  path: ${NFS_PATH}
EOF
}

create_persistent_volume() {
    echo "Creating persistent volume..."
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
  storageClassName: nfs-storage
  nfs:
    server: ${NFS_SERVER}
    path: ${NFS_PATH}
EOF
}

create_storage() {
    echo "Creating PVC..."
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
  storageClassName: nfs-storage
EOF

    kubectl wait --for=condition=bound pvc/crawler-data-pvc -n ${NAMESPACE} --timeout=30s
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
    create_storage
    deploy_jobs
    
    echo "Deployment completed successfully!"
    echo "Monitor jobs with: kubectl get jobs -n ${NAMESPACE}"
    echo "View logs with: kubectl logs -f job/buenarda-crawler-* -n ${NAMESPACE}"
}

main "$@"