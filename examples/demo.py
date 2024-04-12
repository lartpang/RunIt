import argparse
import os
import time

parser = argparse.ArgumentParser()
parser.add_argument("--value", type=int, default=0)
args = parser.parse_args()


GPU_IDS = os.environ["CUDA_VISIBLE_DEVICES"]
print(f"[GPUs: {GPU_IDS}] Start {args.value}")
time.sleep(args.value * 2)
print(f"[GPUs: {GPU_IDS}] End {args.value}")
