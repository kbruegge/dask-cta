from queue import Queue
import time
import logging
import click
from distributed import Client
from daskcta import load_cam_geoms, load_instrument, load_event_generator
import daskcta.analysis as analysis

client = Client('127.0.0.1:8786')


def batch_reco(dicts, instrument_dict):
    return [analysis.reco(e, instrument_dict) for e in dicts]


def batch_hillas(events, geoms):
    return [analysis.hillas(e, geoms) for e in events]


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
            r = len(results)
            b = len(results[0])
            e = len(results[0][0])
            dt = time.time() - s
            s = time.time()
            print('Got {} results of {} batches containing {} events in {:.1f} seconds \n'
                  'thats {:.2f} elements per second'.format(r, b, e, dt,  (r*b*e)/dt))
        time.sleep(5)


@click.command()
@click.option('--batch_size', '-b', default=1, help='Number of events in one batch.')
@click.option('--input_q_size', '-qs', default=10, help='Number of events to hold in the input queue.')
@click.option('--sleep', '-s', default=1, help='Delay until new batch is pushed into queue.')
def main(batch_size, input_q_size, sleep):

    generator = load_event_generator()
    instrument = load_instrument().as_dict()
    geoms = load_cam_geoms()

    logging.info('loaded instrument and geometries, distributing to workers')
    instrument_remote = client.scatter(instrument, broadcast=True)
    geometries_remote = client.scatter(geoms, broadcast=True)

    # hillas = partial(hillas, geoms=geometries_remote)
    # reco = partial(reco, instrument=instrument_remote)

    input_q = Queue(maxsize=input_q_size)
    remote_queue = client.scatter(input_q)

    hillas_q = client.map(batch_hillas, remote_queue, **{'geoms': geometries_remote})
    reco_q = client.map(batch_reco,  hillas_q, **{'instrument_dict': instrument_remote})

    result_q = client.gather(reco_q)

    from threading import Thread
    Thread(target=load_data, args=(input_q, generator, batch_size, sleep), daemon=True).start()

    # Thread(target=load_data, args=(input_q, generator, 90), daemon=True).start()

    # Thread(target=load_data, args=(input_q, generator, 60), daemon=True).start()
    Thread(target=monitor_q, args=(input_q, 'input queue'), daemon=True).start()

    get_results(result_q)

if __name__ == '__main__':
    main()
