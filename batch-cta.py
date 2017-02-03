from ctapipe.image.cleaning import tailcuts_clean
from ctapipe.image.hillas import hillas_parameters
from ctapipe.reco.FitGammaHillas import FitGammaHillas
import numpy as np
from queue import Queue
from astropy import units as u
import time
import logging
from ctapipe.io.containers import InstrumentContainer
from cta import load_cam_geoms, load_instrument, load_event_generator
from joblib import Parallel, delayed
from threading import Thread
import click


def reco(hillas_dict, instrument_dict):
    instrument = InstrumentContainer()
    instrument.__dict__ = instrument_dict

    logging.info('starting reco for dict: {}'.format(hillas_dict))
    tel_phi = {tel_id: 0*u.deg for tel_id in hillas_dict.keys()}
    tel_theta = {tel_id: 20*u.deg for tel_id in hillas_dict.keys()}
    fit_result = FitGammaHillas().predict(
                            hillas_dict,
                            instrument,
                            tel_phi,
                            tel_theta)

    logging.info('Finished reco')
    return fit_result


def batch_analysis(events, geoms, instrument):
    return [reco(hillas(e, geoms), instrument) for e in events]


def hillas(event, geoms):
    hillas_dict = {}
    # print(event)
    for tel_id in event['data']:
        pmt_signal = np.array(event['data'][tel_id]['adc_sums'])
        cam_geom = geoms[tel_id]
        mask = tailcuts_clean(cam_geom, pmt_signal, 1,
                              picture_thresh=10., boundary_thresh=5.)
        pmt_signal[mask == 0] = 0

        moments = hillas_parameters(cam_geom.pix_x,
                                    cam_geom.pix_y,
                                    pmt_signal)

        hillas_dict[tel_id] = moments

    return hillas_dict


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
            l = len(results[0])
            dt = time.time() - s
            s = time.time()
            print('Latest results: {} elements of length {} in {:.1f} seconds \n'
                  'thats {:.2f} elements per second'.format(r, l, dt,  r*l/dt))

        else:
            print('no results')
        time.sleep(5)


@click.command()
@click.option('--batch_size', '-b', default=1, help='Number of events in one batch.')
@click.option('--input_q_size', '-qs', default=10, help='Number of events to hold in the input queue.')
@click.option('--sleep', '-s', default=0.01, help='Delay until new batch is pushed into queue.')
@click.option('--jobs', '-j', default=2, help='Number of jobs to use.')
def main(batch_size, input_q_size, sleep, jobs):
    generator = load_event_generator()
    # instrument = load_instrument().as_dict()
    geoms = load_cam_geoms()

    input_q = Queue(maxsize=input_q_size)

    Thread(target=load_data, args=(input_q, generator, batch_size, sleep), daemon=True).start()

    Thread(target=monitor_q, args=(input_q, 'input queue'), daemon=True).start()

    Thread(target=get_results, args=(input_q,), daemon=True).start()

    with Parallel(n_jobs=jobs) as parallel:
        n_iter = 0
        while n_iter < 1000:
            batches = [input_q.get() for _ in range(input_q.qsize())]
            # print('number of batches {}'.format(len(batches)))
            results = parallel(delayed(batch_hillas)(events, geoms) for events in batches)

            # print('number of results:{}'.format(len(results)))
            n_iter += 1

if __name__ == '__main__':
    main()
