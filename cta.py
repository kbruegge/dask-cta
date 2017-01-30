from ctapipe.image.cleaning import tailcuts_clean
from ctapipe.io import CameraGeometry
from ctapipe.image.hillas import hillas_parameters
from ctapipe.reco.FitGammaHillas import FitGammaHillas
import numpy as np
from queue import Queue
from astropy import units as u
import time
# import astropy
import os
import gzip
import pickle
import os.path
import logging
from itertools import cycle
from tqdm import tqdm
# from functools import lru_cache


# logger = logging.getLogger('CTA')


def get_cam_geoms(instrument):
    geoms = {}
    for tel_id, pixel_pos in tqdm(instrument.pixel_pos.items()):
        pix_x = pixel_pos[0]
        pix_y = pixel_pos[1]
        foc = instrument.optical_foclen[tel_id]
        geoms[tel_id] = CameraGeometry.guess(pix_x, pix_y, foc)

    return geoms


def reconstruction(hillas_dict, instrument):
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


def hillas(event, geoms):
    hillas_dict = {}

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


def load_event_generator(working_dir):
    p = os.path.join(working_dir, 'resources', 'gammas.pickle.gz')
    with gzip.open(p, 'rb') as f:
        event_list = pickle.load(f)

    events = []
    for e in event_list:
        d = {
             'event_id': np.asscalar(e.dl0.event_id),
             'run_id': e.dl0.run_id,
             'data': {},
             }

        for tel_id, t in e.dl0.tel.items():
            d['data'][tel_id] = {'adc_sums': t.adc_sums[0].tolist()}

        events.append(d)
    return cycle(events)


def add_event_to_queue(q, events):
    while True:
        logging.info('emitting event')
        q.put(next(events))
        time.sleep(0.1)


def monitor_q(q, name='result queue'):
    while True:
        print('Items currently in {} : {}'.format(name, q.qsize()))
        time.sleep(0.5)



def get_results(q):
    while True:
        print('Latest result: {}'.format(q.get()))
        time.sleep(1)

def main():

    def load_instrument(DIR):
        p = os.path.join(DIR, 'resources', 'instrument.pickle.gz')
        with gzip.open(p, 'rb') as f:
            return pickle.load(f)

    from distributed import Client
    client = Client('127.0.0.1:8786')

    generator = load_event_generator('./')
    instrument = load_instrument('./')
    geoms = get_cam_geoms(instrument)

    logging.info('loaded geometries')
    # from IPython import embed
    # embed()
    input_q = Queue()
    remote_queue = client.scatter(input_q)

    hillas_q = client.map(lambda x: hillas(x, geoms), remote_queue)
    reco_q = client.map(lambda x: reconstruction(x, instrument), hillas_q)

    result_q = client.gather(reco_q)
    # print(type(result_q))
    from threading import Thread
    Thread(target=add_event_to_queue, args=(input_q, generator)).start()

    Thread(target=monitor_q, args=(result_q, )).start()

    Thread(target=monitor_q, args=(input_q, 'input queue')).start()

    Thread(target=get_results, args=(result_q, )).start()

if __name__ == '__main__':
    main()
