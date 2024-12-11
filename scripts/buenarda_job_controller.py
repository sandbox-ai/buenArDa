import logging
from typing import Dict, List
import time
from kubernetes.client.rest import ApiException
from kubernetes import client, config
from scripts.search_commoncrawl_index import get_commoncrawl_indexes

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_job_template(index_id, output_path, worker_id, total_workers):
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"buenarda-crawler-{index_id.replace('/', '-').lower()}-{worker_id}",
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": "crawler",
                        "image": "marianbasti/buenarda-worker:latest",
                        "args": [
                            "--index", index_id.lower(),
                            "--output", output_path,
                            "--worker-id", str(worker_id),
                            "--total-workers", str(total_workers)
                        ],
                        "volumeMounts": [{
                            "name": "data",
                            "mountPath": "/data"
                        }]
                    }],
                    "volumes": [{
                        "name": "data",
                        "persistentVolumeClaim": {
                            "claimName": "crawler-data-pvc"
                        }
                    }],
                    "restartPolicy": "Never"
                }
            },
            "backoffLimit": 4
        }
    }

def validate_k8s_resources():
    """Check if required K8s resources exist"""
    try:
        v1 = client.CoreV1Api()
        v1.read_namespaced_persistent_volume_claim(
            name="crawler-data-pvc",
            namespace="default"
        )
    except ApiException as e:
        raise RuntimeError(f"Required K8s resources not found: {e}")

def monitor_jobs(batch_v1, jobs: Dict[str, dict]):
    """Monitor job status and log progress"""
    while jobs:
        for job_name, job_info in list(jobs.items()):
            try:
                status = batch_v1.read_namespaced_job_status(
                    name=job_name,
                    namespace="default"
                )
                if status.status.succeeded:
                    logger.info(f"Job {job_name} completed successfully")
                    jobs.pop(job_name)
                elif status.status.failed:
                    logger.error(f"Job {job_name} failed")
                    jobs.pop(job_name)
            except ApiException as e:
                logger.error(f"Error monitoring job {job_name}: {e}")
        time.sleep(30)

def main(workers_per_index=1):
    if workers_per_index < 1:
        raise ValueError("workers_per_index must be at least 1")

    config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    
    try:
        validate_k8s_resources()
        indexes = get_commoncrawl_indexes()
        if not indexes:
            raise RuntimeError("No CommonCrawl indexes found")
            
        active_jobs = {}
        
        for index in indexes:
            for worker_id in range(workers_per_index):
                job = create_job_template(
                    index,
                    f"/data/crawl_data_{index.replace('/', '-')}_{worker_id}.jsonl",
                    worker_id,
                    workers_per_index
                )
                try:
                    response = batch_v1.create_namespaced_job(
                        body=job,
                        namespace="default"
                    )
                    active_jobs[response.metadata.name] = {
                        'index': index,
                        'worker_id': worker_id
                    }
                    logger.info(f"Created job for index {index} worker {worker_id}")
                except ApiException as e:
                    logger.error(f"Error creating job for {index} worker {worker_id}: {e}")
        
        # Monitor jobs until completion
        monitor_jobs(batch_v1, active_jobs)
        
    except Exception as e:
        logger.error(f"Critical error in job controller: {e}")
        raise

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', type=int, default=1,
                      help='Number of workers per index')
    args = parser.parse_args()
    main(args.workers)