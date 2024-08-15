# -*- coding: utf-8 -*-
# @Time    : 2021/3/6
# @Author  : Lart Pang
# @GitHub  : https://github.com/lartpang

import argparse
import logging
import os
import signal
import subprocess
import time
from enum import Enum
from multiprocessing import Lock, Manager, Pool, freeze_support

import pynvml
import yaml

lock = Lock()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter("[%(name)s %(levelname)s] %(message)s"))
logger.addHandler(stream_handler)


class STATUS(Enum):
    WAITING = 0
    RUNNING = 1
    DONE = 2
    FAILED = 3


class GPUMonitor:
    def __init__(self, available_gpu_ids) -> None:
        pynvml.nvmlInit()

        self.available_gpu_ids = available_gpu_ids
        self.driver_version = pynvml.nvmlSystemGetDriverVersion()
        self.cuda_version = pynvml.nvmlSystemGetCudaDriverVersion()

        max_num_gpus = pynvml.nvmlDeviceGetCount()
        if len(self.available_gpu_ids) > max_num_gpus:
            raise ValueError("The number of gpus in config is larger than the number of available gpus.")
        self.gpu_handlers = {idx: pynvml.nvmlDeviceGetHandleByIndex(idx) for idx in self.available_gpu_ids}

    def shutdown(self):
        pynvml.nvmlShutdown()

    def __repr__(self) -> str:
        base_info = f"GPU Information: Driver: {self.driver_version}, CUDA:{self.cuda_version}\n\t"
        gpu_infos = []
        for idx in self.available_gpu_ids:
            mem_info = pynvml.nvmlDeviceGetMemoryInfo(self.gpu_handlers[idx])
            total_mem = int(mem_info.total / 1024 / 1024)
            used_mem = int(mem_info.used / 1024 / 1024)
            gpu_infos.append({"GPU ID": idx, "Total Mem(MB)": total_mem, "Used Mem(MB)": used_mem})
        return base_info + "\n\t".join([str(x) for x in gpu_infos])

    def get_total_mem_by_id(self, idx):
        mem_info = pynvml.nvmlDeviceGetMemoryInfo(self.gpu_handlers[idx])
        return int(mem_info.total / 1024 / 1024)

    def get_used_mem_by_id(self, idx):
        mem_info = pynvml.nvmlDeviceGetMemoryInfo(self.gpu_handlers[idx])
        return int(mem_info.used / 1024 / 1024)

    def get_available_mem_by_id(self, idx):
        mem_info = pynvml.nvmlDeviceGetMemoryInfo(self.gpu_handlers[idx])
        total_mem = int(mem_info.total / 1024 / 1024)
        used_mem = int(mem_info.used / 1024 / 1024)
        return total_mem - used_mem


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def worker(job_id: int, job_info: dict, available_gpu_ids: list, done_jobs: dict, total_gpu_info: dict):
    gpu_ids = ",".join(available_gpu_ids)
    job_identifier = f"[GPU-{gpu_ids}:Job-{job_info['name']}]"

    # 设置子程序环境变量
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"] = gpu_ids

    job_cmd = job_info["command"]
    with subprocess.Popen(job_cmd, shell=True, env=env) as sub_proc:
        try:
            logger.info(f"{job_identifier} Executing `{job_cmd}`...")
            sub_proc.wait()
            done_jobs[job_id] = STATUS.DONE
        except Exception as e:
            logger.error(f"{job_identifier} Command `{job_cmd}` failed: {e}")
            sub_proc.terminate()
            done_jobs[job_id] = STATUS.FAILED

    with lock:
        logger.info(f"Release {job_info}!")
        logger.debug(f"From {total_gpu_info}")
        for gpu_id in available_gpu_ids:
            total_gpu_info[gpu_id] += job_info["memory"]
        logger.debug(f"To {total_gpu_info}")


def get_available_gpu_ids(job_info: dict, total_gpu_info: dict):
    # TODO: Better Assignment Strategy
    with lock:
        available_gpu_ids = []
        for gpu_id, available_mem in total_gpu_info.items():
            if available_mem >= job_info["memory"]:
                available_gpu_ids.append(gpu_id)

        if len(available_gpu_ids) <= job_info["num_gpus"]:
            return None
        return available_gpu_ids[: job_info["num_gpus"]]


def get_args():
    # fmt: off
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, required=True, help="The path of the yaml containing all information of gpus and cmds.")
    parser.add_argument("--max-workers", type=int, help="The max number of the workers.")
    parser.add_argument("--interval-for-waiting-gpu", type=int, default=3, help="In seconds, the interval for waiting for a GPU to be available.")
    parser.add_argument("--interval-for-loop", type=int, default=1, help="In seconds, the interval for looping.")
    # fmt: on
    return parser.parse_args()


def main():
    args = get_args()
    logger.info("[YOUR CONFIG]\n" + str(args))

    with open(args.config, mode="r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    gpu_infos: list = config["gpu"]
    job_infos: list = config["job"]
    assert isinstance(gpu_infos, (tuple, list)), gpu_infos
    assert isinstance(job_infos, (tuple, list)), job_infos
    logger.info("[YOUR GPUS]\n -" + "\n -".join([str(x) for x in gpu_infos]))
    logger.info("[YOUR CMDS]\n -" + "\n -".join([str(x) for x in job_infos]))

    if args.max_workers is None:
        args.max_workers = len(gpu_infos)

    gpu_monitor = GPUMonitor(available_gpu_ids=[x["id"] for x in gpu_infos])
    logger.info(gpu_monitor)

    manager = Manager()
    # 创建一个跨进程共享的dict来跟踪空余的GPU显存
    total_gpu_info = manager.dict()
    for gpu_info in gpu_infos:
        total_gpu_info[str(gpu_info["id"])] = gpu_monitor.get_available_mem_by_id(gpu_info["id"])  # 剩余显存比例
    # 创建一个跨进程共享的dict来跟踪已完成的命令
    done_jobs = manager.dict()
    for job_id, job_info in enumerate(job_infos):
        if job_info["num_gpus"] > len(gpu_infos):
            raise ValueError(f"The number of gpus in job {job_id} is larger than the number of available gpus.")
        if job_info.get("memory", 0) <= 0:
            job_info["memory"] = 0  # 默认所需显存为0
            logger.warning(f"The memory of job {job_id} is not set, set it to 0 by default.")
        done_jobs[job_id] = STATUS.WAITING

    # 在创建进程池之前注册信号处理器，以便在接收到中断信号时执行清理操作
    original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
    pool = Pool(processes=args.max_workers, initializer=init_worker)
    # 将原始的信号处理器恢复
    signal.signal(signal.SIGINT, original_sigint_handler)

    try:
        # 循环处理指令，直到所有指令都被处理
        while not all([status is STATUS.DONE for status in done_jobs.values()]):
            for job_id, job_info in enumerate(job_infos):
                if done_jobs[job_id] in [STATUS.DONE, STATUS.RUNNING]:
                    continue

                available_gpu_ids = get_available_gpu_ids(job_info, total_gpu_info)
                if available_gpu_ids:
                    # 如果当前有足够的GPU资源，执行指令
                    with lock:
                        done_jobs[job_id] = STATUS.RUNNING

                        # 更新GPU的全局状态
                        # 将这个状态更新放到worker中会导致get_available_gpu_ids内部的GPU状态无法即时更新，所以放到外部
                        logger.info(f"Perform {job_info}!")
                        logger.debug(f"From {total_gpu_info}")
                        for gpu_id in available_gpu_ids:
                            total_gpu_info[gpu_id] -= job_info["memory"]
                        logger.debug(f"To {total_gpu_info}")

                    # 执行分配的指令
                    pool.apply_async(worker, args=(job_id, job_info, available_gpu_ids, done_jobs, total_gpu_info))
                else:
                    # 如果GPU资源不足，跳过当前指令，稍后重试
                    logger.warning(f"Skipping {job_info}, not enough GPUs available ({total_gpu_info}).")
                    # 等待一段时间再次检查
                    time.sleep(args.interval_for_waiting_gpu)

            # 等待一段时间再次检查
            time.sleep(args.interval_for_loop)

        # 关闭进程池并等待所有任务完成
        pool.close()
    except KeyboardInterrupt:
        logger.error("[CAUGHT KEYBOARDINTERRUPT, TERMINATING WORKERS!]")
        pool.terminate()
    finally:
        pool.join()
        manager.shutdown()

    gpu_monitor.shutdown()
    logger.info("[ALL COMMANDS HAVE BEEN COMPLETED!]")


if __name__ == "__main__":
    freeze_support()
    main()
