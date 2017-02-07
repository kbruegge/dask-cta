import gzip
import pickle
from itertools import cycle
import numpy as np
import pkg_resources


def load_instrument():
    p = pkg_resources.resource_filename(__name__, 'resources/instrument.pickle.gz')
    with gzip.open(p, 'rb') as f:
        return pickle.load(f)


def load_cam_geoms():
    p = pkg_resources.resource_filename(__name__, 'resources/geoms.pickle.gz')
    with gzip.open(p, 'rb') as f:
        return pickle.load(f)


def load_event_generator():
    p = pkg_resources.resource_filename(__name__, 'resources/gammas.pickle.gz')
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
