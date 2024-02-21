# RunIt

> [!NOTE]
> This tool still has some limitations.
> If you encounter any problems in use, please feel free to ask.

A simple program scheduler for your code on different devices.

Let the machine move!

Putting the machine into sleep is a disrespect for time.

## Usage

```shell
$ python run_it.py --help
usage: run_it.py [-h] [--verbose] [--gpu-pool GPU_POOL [GPU_POOL ...]] [--max-workers MAX_WORKERS] --cmd-pool CMD_POOL
                 [--max-used-ratio MAX_USED_RATIO]

optional arguments:
  -h, --help            show this help message and exit
  --verbose             Whether to print the output of the subprocess.
  --gpu-pool GPU_POOL [GPU_POOL ...]
                        The pool containing all ids of your gpu devices.
  --max-workers MAX_WORKERS
                        The max number of the workers.
  --cmd-pool CMD_POOL   The text file containing all your commands. It need to contain the launcher.
  --max-used-ratio MAX_USED_RATIO
                        The max used ratio of the gpu.
```

## demo

```shell
$ python run_it.py --verbose --cmd-pool ./examples/config.txt # with the default `gpu-pool` and `max-workers`
$ python run_it.py --verbose --gpu-pool 0 1 --max-workers 2 --cmd-pool ./examples/config.txt
```

<details>
<summary>
./examples/config.txt
</summary>

```shell
$ cat ./examples/config.txt
python -m pip list
python --version
python ./examples/demo.py
python ./examples/demo.py
python ./examples/demo.py
```

 </details>

## Thanks

- https://www.jb51.net/article/142787.htm
- https://docs.python.org/zh-cn/3/library/subprocess.html
- https://stackoverflow.com/a/23616229
- https://stackoverflow.com/a/14533902
