from daskcta import load_cam_geoms, load_instrument, load_event_generator
import daskcta.analysis as analysis


generator = load_event_generator()
instrument = load_instrument().as_dict()
geoms = load_cam_geoms()


def calc():
    event = next(generator)
    hillas_dict = analysis.hillas(event, geoms)
    return analysis.reco(hillas_dict, instrument)


def main():
    r = []

    for _ in range(25):
        r.append(calc())

    print('Finished analysing {} events'.format(len(r)))


if __name__ == '__main__':
    main()
