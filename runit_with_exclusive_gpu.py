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
from multiprocessing import Manager, Pool, freeze_support
from queue import Queue

import yaml

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


def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def worker(cmd: str, gpu_ids: str, queue: Queue, job_id: int, done_jobs: dict):
    job_identifier = f"[Job-{job_id}:GPU-{gpu_ids}]"

    # 设置子程序环境变量
    env = os.environ.copy()
    env["CUDA_VISIBLE_DEVICES"] = gpu_ids

    with subprocess.Popen(cmd, shell=True, env=env) as sub_proc:
        try:
            logger.info(f"{job_identifier} Executing `{cmd}`...")
            sub_proc.wait()
            done_jobs[job_id] = STATUS.DONE
        except Exception as e:
            logger.error(f"{job_identifier} Command `{cmd}` failed: {e}")
            sub_proc.terminate()
            done_jobs[job_id] = STATUS.FAILED

    # 释放GPU资源回队列
    for gpu in gpu_ids.split(","):
        queue.put(gpu)
    logger.info(f"{job_identifier} Release GPU {gpu_ids}...")


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

    manager = Manager()

    # 创建一个跨进程共享的队列来统计空余的GPU资源
    available_gpus = manager.Queue()
    for gpu_info in gpu_infos:
        available_gpus.put(str(gpu_info["id"]))
    # 创建一个跨进程共享的dict来跟踪已完成的命令
    done_jobs = manager.dict()
    for job_id, job_info in enumerate(job_infos):
        if job_info["num_gpus"] > len(gpu_infos):
            raise ValueError(f"The number of gpus in job {job_id} is larger than the number of available gpus.")
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
                # else: STATUS.WAITING, STATUS.FAILED

                # job_name = job_info["name"]
                command = job_info["command"]
                num_gpus = job_info["num_gpus"]

                num_avaliable_gpus = available_gpus.qsize()
                # 如果当前有足够的GPU资源，执行指令
                if num_gpus <= num_avaliable_gpus:
                    done_jobs[job_id] = STATUS.RUNNING
                    # 从队列中获取可用的GPU资源
                    gpu_ids = ",".join([available_gpus.get() for _ in range(num_gpus)])
                    # 执行给定的指令，并提供回调函数来更新完成的命令列表
                    pool.apply_async(worker, args=(command, gpu_ids, available_gpus, job_id, done_jobs))
                else:
                    # 如果GPU资源不足，跳过当前指令，稍后重试
                    logger.warning(
                        f"Skipping `{command}`, not enough GPUs available ({num_gpus} > {num_avaliable_gpus})."
                    )
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
    logger.info("[ALL COMMANDS HAVE BEEN COMPLETED!]")


if __name__ == "__main__":
    freeze_support()
    main()
