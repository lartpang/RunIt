# -*- coding: utf-8 -*-
# @Time    : 2021/3/6
# @Author  : Lart Pang
# @GitHub  : https://github.com/lartpang

import argparse
import os
import subprocess
import time
from enum import Enum
from multiprocessing import Manager, Pool, freeze_support
from queue import Queue

import yaml


class STATUS(Enum):
    WAITING = 0
    RUNNING = 1
    DONE = 2
    FAILED = 3


def worker(cmd: str, gpu_ids: str, queue: Queue, job_id: int, done_jobs: dict):
    job_identifier = f"[Job-{job_id}:GPU-{gpu_ids}]"

    try:
        print(f"{job_identifier} Executing {cmd}...")

        # 设置子程序环境变量
        env = os.environ.copy()
        env["CUDA_VISIBLE_DEVICES"] = gpu_ids
        subprocess.run(cmd, shell=True, check=True, env=env)
        done_jobs[job_id] = STATUS.DONE
    except subprocess.CalledProcessError as e:
        print(f"{job_identifier} Command '{cmd}' failed: {e}")

        done_jobs[job_id] = STATUS.FAILED

    # 释放GPU资源回队列
    for gpu in gpu_ids.split(","):
        queue.put(gpu)
    print(f"{job_identifier} Release GPU {gpu_ids}...")


def get_args():
    # fmt: off
    parser = argparse.ArgumentParser()
    parser.add_argument("--gpu-pool", nargs="+", type=int, default=[0], help="The pool containing all ids of your gpu devices.")
    parser.add_argument("--max-workers", type=int, help="The max number of the workers.")
    parser.add_argument("--cmd-pool",type=str, required=True, help="The path of the yaml containing all cmds.")
    parser.add_argument("--interval-for-waiting-gpu",type=int, default=3, help="In seconds, the interval for waiting for a GPU to be available.")
    parser.add_argument("--interval-for-loop",type=int, default=1, help="In seconds, the interval for looping.")
    # fmt: on

    args = parser.parse_args()
    if args.max_workers is None:
        args.max_workers = len(args.gpu_pool)
    return args


def main():
    args = get_args()
    print("[YOUR CONFIG]\n" + str(args))

    with open(args.cmd_pool, mode="r", encoding="utf-8") as f:
        jobs = yaml.safe_load(f)
    assert isinstance(jobs, (tuple, list)), jobs
    print("[YOUR CMDS]\n" + "\n\t".join([str(job) for job in jobs]))

    manager = Manager()
    # 创建一个跨进程共享的队列来统计空余的GPU资源
    available_gpus = manager.Queue()
    for i in args.gpu_pool:
        available_gpus.put(str(i))
    # 创建一个跨进程共享的dict来跟踪已完成的命令
    done_jobs = manager.dict()
    for job_id, job_info in enumerate(jobs):
        if job_info["num_gpus"] > len(args.gpu_pool):
            raise ValueError(f"The number of gpus in job {job_id} is larger than the number of available gpus.")
        done_jobs[job_id] = STATUS.WAITING

    # 创建进程池
    pool = Pool(processes=args.max_workers)
    # 循环处理指令，直到所有指令都被处理
    while not all([status is STATUS.DONE for status in done_jobs.values()]):
        for job_id, job_info in enumerate(jobs):
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
                print(f"Skipping '{command}', not enough GPUs available ({num_gpus} > {num_avaliable_gpus}).")
                # 等待一段时间再次检查
                time.sleep(args.interval_for_waiting_gpu)

        # 等待一段时间再次检查
        time.sleep(args.interval_for_loop)

    # 关闭进程池并等待所有任务完成
    pool.close()
    pool.join()
    manager.shutdown()

    print("[ALL COMMANDS HAVE BEEN COMPLETED!]")


if __name__ == "__main__":
    freeze_support()
    main()
