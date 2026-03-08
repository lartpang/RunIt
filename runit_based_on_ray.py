# -*- coding: utf-8 -*-
# @Time    : 2024/8/14
# @Author  : Lart Pang & AI Assistant
# @GitHub  : https://github.com/lartpang

import argparse
import logging
import os
import subprocess

import yaml

try:
    import ray
except ImportError:
    raise ImportError("Please install ray using `pip install ray` to use this script.")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter("[%(name)s %(levelname)s] %(message)s"))
logger.addHandler(stream_handler)


@ray.remote
def worker(job_id: int, job_info: dict):
    job_identifier = f"[Job-{job_id}:{job_info['name']}]"
    job_cmd = job_info["command"]

    # Ray handles the CUDA_VISIBLE_DEVICES assignment automatically when num_gpus > 0
    # ray.get_gpu_ids() returns a list of assigned GPU IDs
    assigned_gpus = ray.get_gpu_ids()

    env = os.environ.copy()
    if assigned_gpus:
        env["CUDA_VISIBLE_DEVICES"] = ",".join([str(g) for g in assigned_gpus])

    logger.info(f"{job_identifier} Executing `{job_cmd}` on Ray Allocated GPU(s): {assigned_gpus}")

    with subprocess.Popen(job_cmd, shell=True, env=env) as sub_proc:
        sub_proc.wait()
        if sub_proc.returncode == 0:
            logger.info(f"{job_identifier} Command `{job_cmd}` completed successfully.")
            return True
        else:
            logger.error(f"{job_identifier} Command `{job_cmd}` failed with return code {sub_proc.returncode}.")
            raise RuntimeError(f"Command `{job_cmd}` failed with return code {sub_proc.returncode}.")


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=True, help="The path of the yaml containing all information of gpus and cmds.")
    parser.add_argument("--max-retries", type=int, default=0, help="The max number of retries for failed jobs, powered by Ray.")
    return parser.parse_args()


def main():
    args = get_args()
    logger.info("[YOUR CONFIG]\n" + str(args))

    with open(args.config, mode="r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    gpu_infos: list = config["gpu"]
    job_infos: list = config["job"]

    logger.info(f"Loaded {len(gpu_infos)} GPUs and {len(job_infos)} Jobs.")

    # 1. Start Ray context
    # Use ray's built-in GPU management to track GPUs automatically.
    # For a local scheduler, we initialize Ray and tell it exactly how many GPUs we want to manage.
    ray.init(num_gpus=len(gpu_infos), ignore_reinit_error=True)

    # 2. Compute "Bin-packing" standard VRAM
    # To let Ray dynamically package VRAM limits as "Fractional GPUs" (e.g. 4000MB represents 0.166 of a GPU),
    # we use the maximum GPU memory available in the config as the denominator.
    # So if you have tasks needing exactly <x> MB, it will claim <x>/<max_card_mb> fraction of a GPU natively.
    standard_vram = max([gpu["memory"] for gpu in gpu_infos]) if gpu_infos else 24000

    futures = []

    for job_id, job_info in enumerate(job_infos):
        required_vram = job_info.get("memory", 0)
        req_gpus = job_info.get("num_gpus", 1)

        # Calculate Ray fractional GPU equivalent
        gpu_fraction = 1.0
        if required_vram > 0:
            gpu_fraction = min(1.0, required_vram / standard_vram)

        # At minimum assign a tiny fraction so Ray still binds it to exactly one GPU ID and sets env vars
        if gpu_fraction <= 0:
            gpu_fraction = 0.001

        fractional_gpus_to_request = gpu_fraction * req_gpus

        # Configure retry limits and custom fractional resources dynamically
        future = worker.options(
            num_gpus=fractional_gpus_to_request,
            max_retries=args.max_retries
        ).remote(job_id, job_info)

        futures.append(future)

    logger.info(f"Submitted {len(futures)} jobs to Ray cluster. Auto bin-packing active...")

    # 3. Wait for all jobs to complete
    try:
        results = ray.get(futures)
        success_count = sum(1 for r in results if r)
        logger.info(f"All jobs completed. {success_count}/{len(results)} succeeded.")
    except Exception as e:
        logger.error(f"Some jobs failed and exceeded max retries: {e}")

    ray.shutdown()


if __name__ == "__main__":
    main()
