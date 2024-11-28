"""
This file contains the code used to process and create the
FineWeb dataset for Argentina (https://huggingface.co/datasets/HuggingFaceFW/fineweb)
"""

from datatrove.executor.k3s import K3sPipelineExecutor  # Change import
from datatrove.pipeline.dedup import MinhashDedupCluster, MinhashDedupFilter, MinhashDedupSignature
from datatrove.pipeline.dedup.minhash import MinhashConfig, MinhashDedupBuckets
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    C4QualityFilter,
    FineWebQualityFilter,
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.pipeline.formatters import PIIFormatter
from datatrove.pipeline.readers import JsonlReader, WarcReader
from datatrove.pipeline.tokens import TokensCounter
from datatrove.pipeline.writers.jsonl import JsonlWriter
from datatrove.utils.hashing import HashConfig


"""
    we first ran the following pipeline for each dump
"""
DUMP_TO_PROCESS = "CC-MAIN-2023-50"  # example

MAIN_OUTPUT_PATH = "./data/argentina"
FILTERING_OUTPUT_PATH = f"{MAIN_OUTPUT_PATH}/base_processing"

main_processing_executor = K3sPipelineExecutor(
    job_name=f"cc-{DUMP_TO_PROCESS.lower()}-argentina",  # Modified to be k8s compliant (lowercase)
    pipeline=[
        WarcReader(
            f"s3://commoncrawl/crawl-data/{DUMP_TO_PROCESS}/segments/",
            glob_pattern="*/warc/*",  # we want the warc files
            default_metadata={"dump": DUMP_TO_PROCESS},
        ),
        URLFilter(exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/1_url/{DUMP_TO_PROCESS}")),
        Trafilatura(favour_precision=True),
        LanguageFilter(
            exclusion_writer=JsonlWriter(
                f"{FILTERING_OUTPUT_PATH}/2_non_spanish_argentina/",
                output_filename="${language}/" + DUMP_TO_PROCESS + "/${rank}.jsonl.gz",
                # folder structure: language/dump/file
            ),
            languages=["es-AR"],  # filter for Spanish (Argentina)
        ),
        GopherRepetitionFilter(
            exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/3_gopher_rep/{DUMP_TO_PROCESS}")
        ),
        GopherQualityFilter(
            exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/4_gopher_qual/{DUMP_TO_PROCESS}")
        ),
        C4QualityFilter(
            filter_no_terminal_punct=False,
            exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/5_c4/{DUMP_TO_PROCESS}"),
        ),
        FineWebQualityFilter(
            exclusion_writer=JsonlWriter(f"{FILTERING_OUTPUT_PATH}/removed/6_fineweb_qual/{DUMP_TO_PROCESS}")
        ),
        JsonlWriter(f"{FILTERING_OUTPUT_PATH}/output/{DUMP_TO_PROCESS}"),
    ],
    tasks=400,
    cpu_request="1",  # Replace time with CPU request
    memory_request="1Gi",  # Replace mem_per_cpu_gb with memory request
    workers=10,  # Control parallel jobs
    namespace="default",
    logging_dir=f"{MAIN_OUTPUT_PATH}/logs/base_processing/{DUMP_TO_PROCESS}",
)
main_processing_executor.run()

"""
    we then applied minhash deduplication to each individual dump,
"""

# you can also change ngrams or the number of buckets and their size here
minhash_config = MinhashConfig(
    hash_config=HashConfig(
        hash_fc="sha1",  # better precision -> fewer false positives (collisions)
        precision=64,
    ),
    num_buckets=2,
    hashes_per_bucket=8,
    n_grams=5,
)

MINHASH_BASE_PATH = f"{MAIN_OUTPUT_PATH}/minhash"

LOGS_FOLDER = f"{MAIN_OUTPUT_PATH}/logs/minhash"
LOCAL_LOGS_FOLDER = "./logs/minhash"

TOTAL_TASKS = 1000

# this is the original data that we want to deduplicate
INPUT_READER = JsonlReader(
    f"{FILTERING_OUTPUT_PATH}/output/{DUMP_TO_PROCESS}"
)  # this is the output from the first part

# stage 1 computes minhash signatures for each task (each task gets a set of files)
stage1 = K3sPipelineExecutor(
    job_name=f"mh1-{DUMP_TO_PROCESS.lower()}-argentina",
    pipeline=[
        INPUT_READER,
        MinhashDedupSignature(
            output_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/signatures", config=minhash_config
        ),
    ],
    tasks=TOTAL_TASKS,
    cpu_request="1",
    memory_request="1Gi",
    workers=10,
    namespace="default",
    logging_dir=f"{LOGS_FOLDER}/signatures",
    depends=main_processing_executor,
)

stage2 = K3sPipelineExecutor(
    job_name=f"mh2-{DUMP_TO_PROCESS.lower()}-argentina",
    pipeline=[
        MinhashDedupBuckets(
            input_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/signatures",
            output_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/buckets",
            config=MinhashConfig(hash_config=minhash_config.hash_config),
        ),
    ],
    tasks=minhash_config.num_buckets * 50,  # the code supports parallelizing each bucket. here we run 50
    # workers per bucket
    logging_dir=f"{LOGS_FOLDER}/buckets",
    cpu_request="1",
    memory_request="1Gi",
    workers=10,
    namespace="default",
    depends=stage1,
)


stage3 = K3sPipelineExecutor(
    job_name=f"mh3-{DUMP_TO_PROCESS.lower()}-argentina",
    pipeline=[
        MinhashDedupCluster(
            input_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/buckets",
            output_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/remove_ids",
            config=minhash_config,
        ),
    ],
    tasks=1,  # this step runs on a single task
    logging_dir=f"{LOGS_FOLDER}/clustering",
    cpu_request="1",
    memory_request="1Gi",
    workers=10,
    namespace="default",
    depends=stage2,
)


stage4 = K3sPipelineExecutor(
    job_name=f"mh4-{DUMP_TO_PROCESS.lower()}-argentina",
    pipeline=[
        INPUT_READER,
        TokensCounter(),  # you can remove this one, it's just a nice way to know how many tokens we have
        # before and after dedup
        MinhashDedupFilter(input_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/remove_ids"),
        # run the PII removal
        PIIFormatter(),
        JsonlWriter(f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/deduped_output"),
    ],
    tasks=TOTAL_TASKS,
    logging_dir=f"{LOGS_FOLDER}/filtering",
    cpu_request="1",
    memory_request="1Gi",
    workers=10,
    namespace="default",
    depends=stage3,
)

# launch dedup pipelines
stage4.run()
