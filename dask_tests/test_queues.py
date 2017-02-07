from queue import Queue
import time
from distributed import Client
from threading import Thread

client = Client('127.0.0.1:8786')


def load_data(q, batch_size=5):
    while True:
        q.put([k for k in range(batch_size)])
        # time.sleep(0.1)


def increment(x, inc=[0, 0, 0]):
    time.sleep(0.005)
    x += sum(inc)
    return x

input_q = Queue(maxsize=100)
data = [1, 2, 3, 4, 5, 6, 7, 8, 100]

remote_data = client.scatter(data)
remote_q = client.scatter(input_q)

inc_q = client.map(lambda xs, **kw: [increment(x, **kw) for x in xs], remote_q, **{'inc': remote_data})
result_q = client.gather(inc_q)


Thread(target=load_data, args=(input_q, 10), daemon=True).start()

s = time.time()
while True:
    results = [result_q.get() for i in range(result_q.qsize())]

    if results:
        print(results)
        r = len(results)
        l = len(results[0])
        dt = time.time() - s
        s = time.time()
        print('Latest results: {} elements with length {}, thats {:.2f} elements/second'
              .format(r, l, r*l/dt))
    else:
        print('no results')
    time.sleep(5)
