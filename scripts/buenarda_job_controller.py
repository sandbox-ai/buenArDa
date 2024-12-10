
from kubernetes import client, config
import json
import os
from base64 import b64encode
from scripts.buenArDa import get_commoncrawl_indexes

def create_job_template(index_id, output_path):
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": f"buenarda-crawler-{index_id.replace('/', '-')}",
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [{
                        "name": "crawler",
                        "image": "buenarda-crawler:latest",
                        "args": ["--index", index_id, "--output", output_path],
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

def main():
    config.load_kube_config()
    batch_v1 = client.BatchV1Api()
    
    # Get indexes
    indexes = get_commoncrawl_indexes()
    
    # Create a job for each index
    for index in indexes:
        job = create_job_template(index, "/data/crawl_data.jsonl")
        try:
            batch_v1.create_namespaced_job(
                body=job,
                namespace="default"
            )
            print(f"Created job for index {index}")
        except Exception as e:
            print(f"Error creating job for {index}: {e}")

if __name__ == "__main__":
    main()