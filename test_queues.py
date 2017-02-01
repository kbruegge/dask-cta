from queue import Queue
import time
from distributed import Client
from threading import Thread

client = Client('127.0.0.1:8786')


def load_data(q):
    i = 0
    while True:
        q.put(i)
        time.sleep(0.1)
        i += 1


def increment(x, inc=[0, 0, 0]):
    time.sleep(0.5)
    for i in inc:
        x += i
    return x


input_q = Queue(maxsize=100)
data = [1, 2, 3, 4, 5, 6, 7, 8]

remote_data = client.scatter(data)
remote_q = client.scatter(input_q)


inc_q = client.map(increment, remote_q, **{'inc': remote_data})
result_q = client.gather(inc_q)


Thread(target=load_data, args=(input_q,), daemon=True).start()

while True:
    print('latest result: {}'.format(result_q.get()))
