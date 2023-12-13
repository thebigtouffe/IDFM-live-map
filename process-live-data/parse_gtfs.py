import dask.dataframe as dd
import pandas as pd
import json
import os

from src.PRIM_API import PRIM_API
from src.Utils import Utils

# DEBUG = False

# Load settings from settings.json
with open('settings.json', 'r') as json_file:
    settings_data = json.load(json_file)

prim = PRIM_API(api_key=settings_data["prim_api_key"])

# if not DEBUG:
#     print("Download static GTFS data.")
#     prim.download_static_gtfs()

calendar_path = os.path.join(prim.STATIC_GTFS_PATH, 'calendar.txt')
calendar = dd.read_csv(calendar_path, dtype={'service_id': 'str',
                                             'monday': 'bool',
                                             'tuesday': 'bool',
                                             'wednesday': 'bool',
                                             'thursday': 'bool',
                                             'friday': 'bool',
                                             'saturday': 'bool',
                                             'sunday': 'bool',
                                             'start_date': 'int',
                                             'end_date': 'int'
                                             })
calendar = calendar.set_index('service_id').compute()

stops_path = os.path.join(prim.STATIC_GTFS_PATH, 'stops.txt')
stops = dd.read_csv(stops_path, dtype={'stop_id': 'str',
                                       'stop_code': 'str',
                                       'stop_name': 'str',
                                       'stop_desc': 'str',
                                       'stop_lon': 'float',
                                       'stop_lat': 'float',
                                       'zone_id': 'str',
                                       'stop_url': 'str',
                                       'location_type': 'str',
                                       'parent_station': 'str',
                                       'stop_timezone': 'str',
                                       'level_id': 'str',
                                       'wheelchair_boarding': 'str',
                                       'platform_code': 'str', })

trips_path = os.path.join(prim.STATIC_GTFS_PATH, 'trips.txt')
trips = dd.read_csv(trips_path, dtype={'route_id': 'str',
                                       'service_id': 'str',
                                       'trip_id': 'str',
                                       'trip_headsign': 'str',
                                       'trip_short_name': 'str',
                                       'direction_id': 'str',
                                       'wheelchair_accessible': 'int',
                                       'bikes_allowed': 'int'})
trips['route_short_id'] = trips['route_id'].apply(
    lambda x: x.split(':')[-1], meta=('route_id', 'str'))

stop_times_path = os.path.join(prim.STATIC_GTFS_PATH, 'stop_times.txt')
stop_times = dd.read_csv(stop_times_path, dtype={'trip_id': 'str',
                                                 'arrival_time': 'str',
                                                 'departure_time': 'str',
                                                 'stop_id': 'str',
                                                 'stop_sequence': 'int',
                                                 'pickup_type': 'int',
                                                 'drop_off_type': 'int',
                                                 'local_zone_id': 'str',
                                                 'stop_headsign': 'str',
                                                 'timepoint': 'str', })

# Get all lines from GTFS
all_lines = set(trips['route_short_id'].values.compute())
# Get list of lines with shortest_paths computed
computed_lines_path = os.path.join('data', 'shortest_paths')
computed_lines = {x.split('.')[0] for x in os.listdir(computed_lines_path)}
# Get relevant lines
lines = computed_lines.intersection(all_lines)

for line in lines:
    print(f"Summarize data for line {line}")

    print("- Join trips with calendar data. Store in memory.")
    l_trips = trips[trips['route_short_id'] == line]
    l_trips = l_trips.set_index('service_id')
    l_trips = l_trips.join(calendar, how='inner')
    l_trips = l_trips.reset_index().set_index('trip_id').compute()
    l_trips_id = set(l_trips.index.values)

    print("- Join time table with trips")
    l_stop_times = stop_times
    l_stop_times = stop_times[stop_times['trip_id'].isin(l_trips_id)]
    print("- Retrieve filtered time table in memory")
    l_stop_times = l_stop_times.compute()
    l_stop_times = l_stop_times.set_index('trip_id').join(l_trips, how='inner',
                                                          lsuffix='stop_times_',
                                                          rsuffix='trips_')
    l_stop_times = l_stop_times.reset_index()

    # Export enriched time table as a parquet file (faster than csv/json)
    print("- Saving data")
    save_directory = os.path.join('data', 'gtfs')
    if not os.path.exists(save_directory):
        os.mkdir(save_directory)
    l_stop_times.to_parquet(os.path.join(save_directory, line))
