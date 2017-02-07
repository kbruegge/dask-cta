from cta import load_cam_geoms, load_instrument, load_event_generator
import cta.analysis


def main():
    generator = load_event_generator()
    instrument = load_instrument().as_dict()
    geoms = load_cam_geoms()
    r = []

    for _ in range(25):
        event = next(generator)
        hillas_dict = cta.analysis.hillas(event, geoms)
        r.append(cta.analysis.reco(hillas_dict, instrument))

    print('Finished analysing {} events'.format(len(r)))


if __name__ == '__main__':
    main()
