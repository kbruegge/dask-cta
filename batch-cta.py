from queue import Queue
import time
from ctapipe.io.containers import InstrumentContainer
from daskcta import load_cam_geoms, load_instrument, load_event_generator
from daskcta import analysis
from threading import Thread
import click

import distributed.joblib
from joblib import Parallel, parallel_backend, delayed


instrument = load_instrument().as_dict()
geoms = load_cam_geoms()


def batch_analysis(events):
    return [analysis.reco(analysis.hillas(e, geoms), instrument) for e in events]


def load_data(q, events, batch_size=100, sleep=2):
    while True:
        q.put([next(events) for k in range(batch_size)])
        time.sleep(sleep)


def monitor_q(q, name='result queue'):
    while True:
        print('Items currently in {} : {}'.format(name, q.qsize()))
        time.sleep(5)


def get_results(q):
    s = time.time()
    while True:
        results = [q.get() for i in range(q.qsize())]

        if results:
            # print(results)
            r = len(results)
            b = len(results[0])
            e = len(results[0][0])
            dt = time.time() - s
            s = time.time()
            print('Got {} batches containing {} events in {:.1f} seconds \n'
                  'thats {:.2f} elements per second'.format(r, b, dt,  (r*b*e)/dt))
        time.sleep(5)


@click.command()
@click.option('--batch_size', '-b', default=1, help='Number of events in one batch.')
@click.option('--input_q_size', '-qs', default=10, help='Number of events to hold in the input queue.')
@click.option('--sleep', '-s', default=0.01, help='Delay until new batch is pushed into queue.')
@click.option('--jobs', '-j', default=2, help='Number of jobs to use.')
@click.option('--distributed', 'local_execution', flag_value=False, help='execute on dask workers')
@click.option('--local', 'local_execution', flag_value=True, default=True, help='execute localy')
def main(batch_size, input_q_size, sleep, jobs, local_execution):
    generator = load_event_generator()
    # instrument = load_instrument().as_dict()
    input_q = Queue(maxsize=input_q_size)

    output_q = Queue(maxsize=100)

    Thread(target=load_data, args=(input_q, generator, batch_size, sleep), daemon=True).start()

    # Thread(target=monitor_q, args=(input_q, 'input queue'), daemon=True).start()

    Thread(target=get_results, args=(output_q,), daemon=True).start()

    if local_execution:
        print('Starting local execution on {} cores'.format(jobs))
        with Parallel(n_jobs=jobs) as parallel:
            execute(input_q, output_q, parallel)

    else:
        with parallel_backend('dask.distributed', scheduler_host='127.0.0.1:8786'):
            parallel = Parallel()
            execute(input_q, output_q, parallel)


def execute(input_q, output_q, parallel):
    n_iter = 0
    while n_iter < 1000:
        batch = input_q.get()
        results = parallel(delayed(batch_analysis)([event]) for event in batch)
        output_q.put(results)
        # print('number of results:{}'.format(len(results)))
        n_iter += 1

if __name__ == '__main__':
    main()
