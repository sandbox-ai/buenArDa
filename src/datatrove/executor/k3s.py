from __future__ import annotations

import json
import os
import signal
import sys
from copy import deepcopy
from typing import Callable

import dill
from dill import CONTENTS_FMODE
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from datatrove.executor.base import PipelineExecutor
from datatrove.io import DataFolderLike
from datatrove.pipeline.base import PipelineStep
from datatrove.utils.logging import get_random_str, get_timestamp, logger


def cleanup_handler(signum, _frame):
    signame = signal.Signals(signum).name
    logger.warning(f"Received signal {signum} ({signame}). Cleaning up and exiting...")
    sys.exit(15)

def launch_pickled_pipeline(pickle_path):
    """Load and run a pickled pipeline executor
    
    Args:
        pickle_path: Path to the pickled executor file
    """
    with open(pickle_path, 'rb') as f:
        executor = dill.load(f)
    executor.run()

class K3sPipelineExecutor(PipelineExecutor):
    """Execute a pipeline on a k3s cluster
    Creates and manages k3s jobs for pipeline execution.

    Args:
        pipeline: a list of PipelineStep and/or custom functions
        tasks: total number of tasks to run the pipeline on
        cpu_request: CPU cores to request per pod
        memory_request: Memory to request per pod (e.g., "2Gi")
        workers: max number of concurrent pods (-1 for unlimited)
        job_name: k3s job name
        namespace: k3s namespace
        image: container image to use
        env_vars: dictionary of environment variables
        depends: another K3sPipelineExecutor that should run before this one
        depends_job_id: alternatively, you can pass the job id of a dependency
        logging_dir: where to save logs, stats, etc.
        skip_completed: whether to skip completed tasks
        tasks_per_job: number of pipeline tasks per k3s job
    """

    def __init__(
        self,
        pipeline: list[PipelineStep | Callable],
        tasks: int,
        cpu_request: str = "1",
        memory_request: str = "2Gi",
        workers: int = -1,
        job_name: str = "data-processing",
        namespace: str = "default",
        image: str = "marianbasti/buenarda:latest",
        env_vars: dict = None,
        depends: K3sPipelineExecutor | None = None,
        depends_job_id: str | None = None,
        logging_dir: DataFolderLike = None,
        skip_completed: bool = True,
        tasks_per_job: int = 1,
    ):
        super().__init__(pipeline, logging_dir, skip_completed)
        self.tasks = tasks
        self.cpu_request = cpu_request
        self.memory_request = memory_request
        self.workers = workers
        self.tasks_per_job = tasks_per_job
        self.job_name = job_name
        self.namespace = namespace
        self.image = image
        self.env_vars = env_vars or {}
        self.depends = depends
        self.depends_job_id = depends_job_id
        self.job_id = None
        
        # Remove client initialization from __init__
        self.k8s_batch_api = None
        self.k8s_core_api = None

    def _init_k8s_clients(self):
        """Initialize Kubernetes clients if not already initialized"""
        if self.k8s_batch_api is None or self.k8s_core_api is None:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()
            self.k8s_batch_api = client.BatchV1Api()
            self.k8s_core_api = client.CoreV1Api()

    def run(self):
        """Run the pipeline either inside a k3s pod or launch k3s jobs"""
        if "K3S_TASK_ID" in os.environ:
            # We're inside a k3s pod, handle task distribution
            task_id = int(os.environ["K3S_TASK_ID"])
            
            # Load the ranks to run
            with self.logging_dir.open("ranks_to_run.json", "r") as ranks_file:
                all_ranks = json.load(ranks_file)

            # Calculate rank range for this task
            ranks_range = (task_id * self.tasks_per_job, (task_id + 1) * self.tasks_per_job)
            
            if ranks_range[0] >= len(all_ranks):
                return

            # Handle cleanup signals
            for sig in (signal.SIGTERM, signal.SIGINT):
                signal.signal(sig, cleanup_handler)

            # Run assigned ranks
            for rank_idx in range(*ranks_range):
                if rank_idx >= len(all_ranks):
                    break
                rank = all_ranks[rank_idx]
                self._run_for_rank(rank)

        else:
            # We're outside k3s, launch the job
            self.launch_job()

    def launch_job(self):
        # Get incomplete ranks
        ranks_to_run = self.get_incomplete_ranks()
        if not ranks_to_run:
            logger.info(f"All {self.tasks} tasks completed, skipping {self.job_name}")
            self.job_id = -1
            return
    
        # Save ranks to run
        with self.logging_dir.open("ranks_to_run.json", "w") as f:
            json.dump(ranks_to_run, f)
    
        # Calculate number of k3s jobs needed
        num_jobs = -(-len(ranks_to_run) // self.tasks_per_job)  # Ceiling division
        
        # Initialize k8s client
        self._init_k8s_clients()
        
        # Create job with proper parallelism
        parallelism = min(self.workers, num_jobs) if self.workers > 0 else num_jobs
        job = self.create_k3s_job_object(parallelism, num_jobs)
        
        try:
            api_response = self.k8s_batch_api.create_namespaced_job(
                body=job,
                namespace=self.namespace
            )
            self.job_id = api_response.metadata.name
            logger.info(f"Created k3s job {self.job_id} with {num_jobs} tasks")
        except ApiException as e:
            logger.error(f"Failed to create k3s job: {e}")
            raise

    def create_k3s_job_object(self, parallelism: int, completions: int):
        container = client.V1Container(
            name=self.job_name,
            image=self.image,
            command=["python", "-m"],
            args=[
                "datatrove.executor.k3s", 
                "launch_pickled_pipeline",
                f"{self.logging_dir.resolve_paths('executor.pik')}"
            ],
            resources=client.V1ResourceRequirements(
                requests={
                    "cpu": self.cpu_request,
                    "memory": self.memory_request
                }
            ),
            env=[
                client.V1EnvVar(name=k, value=v)
                for k, v in {
                    **self.env_vars,
                    "K3S_TASK_ID": "$(JOB_COMPLETION_INDEX)"  # Add task ID env var
                }.items()
            ]
        )

        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"app": self.job_name}),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container]
            )
        )

        spec = client.V1JobSpec(
            parallelism=parallelism,
            completions=completions,
            template=template,
            backoff_limit=4
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=self.job_name),
            spec=spec
        )

        return job

    @property
    def world_size(self) -> int:
        return self.tasks
