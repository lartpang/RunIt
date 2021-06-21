# -*- coding: utf-8 -*-
# @Time    : 2021/3/6
# @Author  : Lart Pang
# @GitHub  : https://github.com/lartpang

import argparse
import subprocess
import time
from multiprocessing import Process


class MyProcess:
    def __init__(self, interpreter_path, gpu_id, verbose=True, stdin=None, stdout=None, stderr=None):
        super().__init__()
        print(f"Create the process object on GPU: {gpu_id}")
        self.interpreter_path = interpreter_path
        self.gpu_id = gpu_id
        self.verbose = verbose
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.sub_proc = None
        self.proc = None

    def _create_sub_proc(self, cmd=""):
        self.sub_proc = subprocess.Popen(
            args=" ".join([f"CUDA_VISIBLE_DEVICES={self.gpu_id}", self.interpreter_path, "-u", cmd]),
            stdin=self.stdin,
            stdout=self.stdout,
            stderr=self.stderr,
            shell=True,
            executable="bash",
            env=None,
            close_fds=True,
            bufsize=1,
            text=True,
            encoding="utf-8",
        )
        print(f"\n[NEW TASK PID: {self.sub_proc.pid}] '{self.sub_proc.args}' has been created.")

        if self.verbose:
            if self.sub_proc is not None and self.sub_proc.stdout is not None:
                for l in self.sub_proc.stdout:
                    print(f"GPU: {self.gpu_id} {l}", end="")

    def create_and_start_proc(self, cmd=None):
        if not cmd:
            return
        print("Create and start subprocess.")
        self.proc = Process(target=self._create_sub_proc, kwargs=dict(cmd=cmd))
        self.proc.start()


def read_cmds_from_txt(path):
    with open(path, encoding="utf-8", mode="r") as f:
        cmds = []
        for line in f:
            line = line.strip()
            if line:
                cmds.append(line)
    return cmds


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--interpreter", type=str, required=True, help="The path of your interpreter you want to use.")
    parser.add_argument("--verbose", action="store_true", help="Whether to print the output of the subprocess.")
    parser.add_argument(
        "--gpu-pool", nargs="+", type=int, required=True, help="The pool containing all ids of your gpu devices."
    )
    parser.add_argument("--max-workers", type=int, required=True, help="The max number of the workers.")
    parser.add_argument(
        "--cmd-pool",
        type=str,
        required=True,
        help="The text file containing all your commands. It will be combined with `interpreter`",
    )
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    print("[YOUR CONFIG]\n" + str(args))
    cmds = read_cmds_from_txt(path=args.cmd_pool)
    print("[YOUR CMDS]\n" + "\n".join(cmds))

    num_gpus = len(args.gpu_pool)
    cmd_pool = iter(cmds)
    num_cmds = len(cmds)

    print("create the initial process objects")
    # TODO: There needs to be a compatible relationship between the two.
    proc_slots = []
    for i in range(min(args.max_workers, num_cmds)):
        proc_slots.append(
            MyProcess(
                interpreter_path=args.interpreter,
                gpu_id=args.gpu_pool[i % num_gpus],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
        )

    print("execute process with multi-processing")
    for i, p in enumerate(proc_slots):
        cmd = next(cmd_pool, False)
        p.create_and_start_proc(cmd=cmd)

    while proc_slots:
        # the pool of the processes is not empty
        for p_idx, p in enumerate(proc_slots):  # polling
            if not p.proc.is_alive():
                cmd = next(cmd_pool, False)
                if cmd:
                    # cmd_pool is not empty
                    p.create_and_start_proc(cmd=cmd)
                else:
                    # cmd_pool is empty and we will delete the corresponding slot
                    del proc_slots[p_idx]
                    break
        time.sleep(1)


if __name__ == "__main__":
    main()
