import multitasking
import time
import random
import signal
from retry import retry

# kill all tasks on ctrl-c
signal.signal(signal.SIGINT, multitasking.killall)


# or, wait for task to finish on ctrl-c:
# signal.signal(signal.SIGINT, multitasking.wait_for_tasks)

@multitasking.task  # 通过装饰器方法进行函数并行加速
@retry(tries=3, delay=1)
def hello(count):
    try:
        sleep = random.randint(1, 10) / 2
        print("Hello %s (sleeping for %ss)" % (count, sleep))
        # time.sleep(sleep)
        2/0
        print("Goodbye %s (after for %ss)" % (count, sleep))
    except Exception as e:
        raise Exception("23")


if __name__ == "__main__":
    for i in range(0, 2):
        hello(i + 1)
