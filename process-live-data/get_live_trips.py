import os
import json

from src.PRIM_API import PRIM_API

# Load JSON data from file
with open("settings.json", "r") as json_file:
    settings_data = json.load(json_file)

if __name__ == '__main__':
    prim = PRIM_API(api_key=settings_data["prim_api_key"])

    if not os.path.exists(prim.NETWORK_DATA_FILE_PATH):
        prim.download_network()
    if not os.path.exists(prim.STOPS_DATA_FILE_PATH):
        prim.download_stops()

    prim.load_network()
    prim.load_stops()

    # # Test SNCF
    # stop=prim.stops['IDFM:monomodalStopPlace:46725']
    # d1 = prim.get_arrival_times_by_stop(stop)

    # # Test metro
    # prim.get_arrival_times_by_stop(prim.stops['IDFM:22227'])
    # prim.get_arrival_times_by_stop(prim.stops['IDFM:462975'])
    # prim.get_arrival_times_by_stop(prim.stops['IDFM:478395'])

    prim.get_arrival_times_by_stop(prim.stops['IDFM:22034'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:22023'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:463250'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:463210'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:22028'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:463069'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:463316'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:21902'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:463250'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:22022'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:463286'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:22033'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:22032'])
    prim.get_arrival_times_by_stop(prim.stops['IDFM:462987'])


    # prim.get_arrival_times()