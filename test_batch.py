import time
from distributed import Client

client = Client('127.0.0.1:8786')


def increment(x, inc=[0, 0, 0]):
    time.sleep(0.5)
    for i in inc:
        x += i
    return x


input_data = list(range(10))
data = [1, 2, 3, 4, 5, 6, 7, 8]

remote_data = client.scatter(data)


futures = client.map(increment, input_data, **{'inc': remote_data})
results = client.gather(futures)

print('results: {}'.format(results))
