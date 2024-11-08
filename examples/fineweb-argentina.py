"""
This file contains the code used to process and create the
FineWeb dataset for Argentina (https://huggingface.co/datasets/HuggingFaceFW/fineweb)
"""

from datatrove.executor.slurm import SlurmPipelineExecutor
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

main_processing_executor = SlurmPipelineExecutor(
    job_name=f"cc_{DUMP_TO_PROCESS}_argentina",
    pipeline=[
        WarcReader(
            f"./data/commoncrawl/{DUMP_TO_PROCESS}/segments/",
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
    time="10:00:00",
    logging_dir=f"{MAIN_OUTPUT_PATH}/logs/base_processing/{DUMP_TO_PROCESS}",
    slurm_logs_folder=f"./logs/base_processing/{DUMP_TO_PROCESS}/slurm_logs",  # must be local
    randomize_start_duration=180,  # don't hit the bucket all at once with the list requests
    mem_per_cpu_gb=1,
    partition="fineweb",
    #srun_args = {'display':'fineweb'}
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
stage1 = SlurmPipelineExecutor(
    job_name=f"mh1_{DUMP_TO_PROCESS}_argentina",
    pipeline=[
        INPUT_READER,
        MinhashDedupSignature(
            output_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/signatures", config=minhash_config
        ),
    ],
    tasks=TOTAL_TASKS,
    time="5:00:00",
    partition="fineweb",
    logging_dir=f"{LOGS_FOLDER}/signatures",
    slurm_logs_folder=f"{LOCAL_LOGS_FOLDER}/signatures/slurm_logs",
    randomize_start_duration=180,
    depends=main_processing_executor,  # only start after the first one completes
    #srun_args={'display':'fineweb'}
)

stage2 = SlurmPipelineExecutor(
    job_name=f"mh2_{DUMP_TO_PROCESS}_argentina",
    pipeline=[
        MinhashDedupBuckets(
            input_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/signatures",
            output_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/buckets",
            config=MinhashConfig(hash_config=minhash_config.hash_config),
        ),
    ],
    tasks=minhash_config.num_buckets * 50,  # the code supports parallelizing each bucket. here we run 50
    # workers per bucket
    randomize_start_duration=180,
    logging_dir=f"{LOGS_FOLDER}/buckets",
    partition="fineweb",
    time="02:00:00",
    mem_per_cpu_gb=1,
    cpus_per_task=1,  # you can add run more (smaller) tasks if you do not have a lot of memory
    depends=stage1,
    #srun_args={'display':'fineweb'}
)


stage3 = SlurmPipelineExecutor(
    job_name=f"mh3_{DUMP_TO_PROCESS}_argentina",
    pipeline=[
        MinhashDedupCluster(
            input_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/buckets",
            output_folder=f"{MINHASH_BASE_PATH}/{DUMP_TO_PROCESS}/remove_ids",
            config=minhash_config,
        ),
    ],
    tasks=1,  # this step runs on a single task
    logging_dir=f"{LOGS_FOLDER}/clustering",
    partition="fineweb",
    time="30:00:00",  # and can also be quite slow. Usually not this slow though
    mem_per_cpu_gb=1,
    cpus_per_task=1,  # if you dedup a full dump, you do need a lot of memory for this one
    depends=stage2,
    #srun_args={'display':'fineweb'}
)


stage4 = SlurmPipelineExecutor(
    job_name=f"mh4_{DUMP_TO_PROCESS}_argentina",
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
    partition="fineweb",
    time="5:00:00",
    mem_per_cpu_gb=1,
    depends=stage3,
    #srun_args={'display':'fineweb'}
)

# launch dedup pipelines
stage4.run()
