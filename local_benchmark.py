from daskcta import load_cam_geoms, load_instrument, load_event_generator
import daskcta.analysis as analysis


def main():
    generator = load_event_generator()
    instrument = load_instrument().as_dict()
    geoms = load_cam_geoms()
    r = []

    for _ in range(25):
        event = next(generator)
        hillas_dict = analysis.hillas(event, geoms)
        r.append(analysis.reco(hillas_dict, instrument))

    print('Finished analysing {} events'.format(len(r)))


if __name__ == '__main__':
    main()
