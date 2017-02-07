from ctapipe.image.cleaning import tailcuts_clean
from ctapipe.image.hillas import hillas_parameters
from ctapipe.reco.FitGammaHillas import FitGammaHillas
import numpy as np
from astropy import units as u
import logging
from ctapipe.io.containers import InstrumentContainer


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
