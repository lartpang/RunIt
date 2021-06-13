# RunIt

A simple program scheduler for your code on different devices.

Let the machine move!

Putting the machine into sleep is a disrespect for time.

## Usage

```shell
python run_it.py --interpreter your_python_path --verbose --gpu-pool 0 1 --max-workers 2 --cmd-pool ./cmd_pools.txt
```

##  demo

<details>
<summary> cmd_pools.txt </summary>
  
```shell
$ cat ./cmd_pools.txt

/home/user_name/path_1/main.py --config=your_config_1.py
/home/user_name/path_2/main.py --config=your_config_2.py
```
 </details>
 
 ## Thanks

- https://www.jb51.net/article/142787.htm
- https://docs.python.org/zh-cn/3/library/subprocess.html
- https://stackoverflow.com/a/23616229
- https://stackoverflow.com/a/14533902
