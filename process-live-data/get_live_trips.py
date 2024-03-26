import os
import json
import geopandas as gpd
import pandas as pd
import numpy as np
import asyncio
import threading
import time, datetime, pytz
import traceback
import aiohttp
from aiolimiter import AsyncLimiter
import json, gzip

from src.PRIM_API import PRIM_API
from src.ArrivalTime import ArrivalTime
from src.GTFS import GTFS
from src.Utils import Utils

import logging
logging.basicConfig(format='[%(asctime)s.%(msecs)03d] %(levelname)-8s %(message)s',
                    level=logging.INFO, datefmt='%H:%M:%S')

# Load settings from settings.json
with open('settings.json', 'r') as json_file:
    settings_data = json.load(json_file)
prim = PRIM_API(api_key=settings_data["prim_api_key"])
logging.info("Read settings and instantiate PRIM API.")

# Load stops and network
network = gpd.read_parquet('data/network.parquet')
logging.info("Loaded network dataframe.")

stops = gpd.read_parquet('data/stops.parquet')
logging.info("Loaded stops dataframe.")

# Dict to store data for trips for each line
trips_last_data = {}
all_lines_trips = {}

def get_remaining_time_until_next_fetch():
    # Get the current time
    now = datetime.datetime.now()
    h = now.hour
    m = now.minute

    # Define data fetch frequency in minutes
    if h == 5:
        fetch_frequency = 4
    elif h == 6:
        fetch_frequency = 3
    elif h >= 0 and h < 2:
        fetch_frequency = 4
    elif h >= 7 and h <= 22:
        fetch_frequency = 2
    elif h == 23:
        fetch_frequency = 3
    else:
        fetch_frequency = 15

    remaining_time = 60 * (fetch_frequency - m % fetch_frequency)
    return remaining_time


class NumpyEncoder(json.JSONEncoder):
    """ Special json encoder for numpy types """
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def compute_coords_timestamps(trips):
    # Get line attributes
    line_name = trips.iloc[0]['line_name']
    line_short_id = trips.iloc[0]['line_short_id']

    # Load shortest paths database
    sp = gpd.read_parquet(f'data/shortest_paths/{line_short_id}.parquet')
    sp['stop_short_id_start'] = sp['stop_id_start'].apply(lambda x: Utils.compute_short_id(x))
    sp['stop_short_id_end'] = sp['stop_id_end'].apply(lambda x: Utils.compute_short_id(x))
    logging.info(f"[{line_name}] Loaded shortest paths dataframe.")

    # Link time and positions for each trip
    trips['time_position'] = list(zip(trips.arrival_time,
                                      trips.stop_short_id,
                                      trips.stop_name))
    
    # TODO: remove duplicates in stop sequence

    # Get all stops with arrival time for each trip
    groupby_fields = ['id', 'line_short_id', 'name', 'destination_id']
    df = trips.groupby(groupby_fields)['time_position'].unique().reset_index()

    # Sort list of stops by arrival time
    def sort_by_arrival_time(tps):
        return sorted(tps, key=lambda x: x[0])
    df['time_position'] = df['time_position'].apply(lambda x: sort_by_arrival_time(x))

    # Build trip path for each pairs of consecutive stops
    def build_path(tps, sp, line_short_id):
        coords = []
        timestamps = []

        for i in range(len(tps)-1):
            start_stop_short_id = tps[i][1]
            end_stop_short_id = tps[i+1][1]

            if start_stop_short_id == end_stop_short_id:
                continue

            start_time = tps[i][0].timestamp()
            end_time = tps[i+1][0].timestamp()

            # Get shortest path between A and B
            cond1 = sp['stop_short_id_start'] == start_stop_short_id
            cond2 = sp['stop_short_id_end'] == end_stop_short_id
            cond3 = sp['line_short_id'] == line_short_id
            path = sp[cond1 & cond2]['line_geometry_interpolated']

            if not path.empty:
                path = path.iloc[0]

                # Number of points of the shortest path between A and B
                n_points = len(path.coords[:])

                # Compute timestamps
                ts = np.linspace(start_time, end_time, n_points)

                # Add data to list of coords/timestamps
                timestamps.append(ts)
                coords += path.coords[:]

            else:
                logging.warning(f'Could not find path between {start_stop_short_id} and {end_stop_short_id}.')
                print(f"-----\n{stops[stops['short_id'] == start_stop_short_id].iloc[0]}\n-----\n")
                print(f"-----\n{stops[stops['short_id'] == end_stop_short_id].iloc[0]}\n-----\n")

        if len(coords) > 0 and len(timestamps) > 0:
            # Concatenate timestamps for each segment of the trip
            timestamps = np.concatenate(timestamps)

            return pd.Series([list(zip(list(timestamps), coords))])
        else:
            return pd.Series([None])

    df['time_position'] = df.apply(lambda x: build_path(x.time_position,
                                                        sp,
                                                        line_short_id),
                                            axis=1)
    logging.info(f"[{line_name}] Computed coordinates and timestamps.")

    return df


def rebuild_trip_ids_from_timetable(trips, timetable):
    output_keys = [
        'id',
        'name',
        'stop_short_id',
        'stop_name',
        'line_short_id',
        'line_name',
        'destination_id',
        'destination_name',
        'arrival_time',
    ]

    # Get the current date in Paris time zone
    day_of_week = datetime.datetime.now().strftime("%A")
    paris_tz = pytz.timezone('CET')
    today = datetime.date.today()
    now = datetime.datetime.now(paris_tz)

    # Filter timetable for current day of week
    timetable = timetable[timetable[day_of_week.lower()] == True]
    tt = timetable.copy()

    # Parse data
    tt['stop_short_id'] = tt['stop_id'].apply(lambda x: Utils.compute_short_id(x))
    tt['arrival_time'] = tt['arrival_time'].apply(lambda x: GTFS.parse_time(x, tzinfo=paris_tz))
    tt['start_date'] = tt['start_date'].apply(lambda x: GTFS.parse_date(x))
    tt['end_date'] = tt['end_date'].apply(lambda x: GTFS.parse_date(x))
    tt['destination_name'] = tt['trip_headsign']
    tt['id'] = tt['trip_id']
    tt['line_short_id'] = tt['route_short_id']
    tt['name'] = ''

    # Filter on trains of the day
    tt = tt[today >= tt['start_date']]
    tt = tt[today - datetime.timedelta(days=1) <= tt['end_date']]

    # Assuming trains can be up to 2 minutes early
    tt = tt[tt['arrival_time'] > now - datetime.timedelta(minutes=2)]

    # Filter on trains expected within the next 60 minutes
    tt = tt[tt['arrival_time'] < now + datetime.timedelta(minutes=60)]

    # Parse trip destination
    def append_destination(group):
        group['destination_id'] = group['stop_short_id'].iloc[-1]
        return group
    tt = tt.sort_values(by=['stop_sequence']).groupby('trip_id').apply(append_destination, include_groups=False)
    tt = tt.reset_index()

    # Get stop name from real-time data
    stops_data = trips[['stop_short_id', 'stop_name']].drop_duplicates().set_index('stop_short_id')
    tt = tt.set_index('stop_short_id').join(stops_data,
                                            how='left',
                                            lsuffix='tt_',
                                            rsuffix='trips_')
    tt = tt.reset_index()

    # Get line name
    tt['line_name'] = trips.iloc[0]['line_name']

    # if trips.iloc[0]['line_name'] == "METRO 14":
    #     print("DEBUG")
    #     trips_last_data['debug'] = tt.copy()

    # TODO: Use real-time data when trains are delayed

    try:
        if not tt.empty:
            return tt[output_keys]
        else:
            return None
    except Exception as e:
        logging.error(traceback.format_exc())
        print(tt)


async def get_line_trips(line_short_id):
    tasks = []

    # Get line attributes (name, type, stops)
    line_name = network[network['short_id'] == line_short_id].iloc[0]['name']
    transportation_type = network[network['short_id'] == line_short_id].iloc[0]['transportation_type']
    stop_short_ids = stops[stops['line_short_id'] == line_short_id].short_id
    stop_short_ids = list(set(stop_short_ids.values))

    async with aiohttp.ClientSession() as session:
        # Fetch arrival times at each stop
        for short_id in stop_short_ids:
            tasks.append(asyncio.ensure_future(prim.get_next_trips_at_stop(short_id,
                                                                           line_short_id,
                                                                           session)))
        logging.info(f"[{line_name}] Created async tasks.")

        responses = await asyncio.gather(*tasks)
        logging.info(f"[{line_name}] Executed {len(tasks)} tasks.")

        # Generate trips dataframe for the line
        trips = [t for r in responses for t in r if t is not None]
        trips = pd.DataFrame.from_dict(trips)
        trips['line_name'] = line_name
        logging.info(f"[{line_name}] Generated dataframe with {len(trips)} rows from response.")

        if len(trips) == 0:
            logging.warning(f'[{line_name}] Dataframe trips is empty!')
            return None

        # RATP data is not complete for metro and tramway
        # Thus we have to manually build trips using the timetable and real-time data for next trains.
        if transportation_type in ("TRAMWAY", "METRO"):
            timetable_dir = os.path.join('data', 'timetable')
            timetable_path = os.path.join(timetable_dir, line_short_id)
            timetable = pd.read_parquet(timetable_path)

            logging.info(f"[{line_name}] Rebuilding trips using train schedule.")
            trips = rebuild_trip_ids_from_timetable(trips, timetable)
        
        # Add previous data for lines with trip id. Keep latest data.
        else:
            if line_short_id in trips_last_data.keys():
                previous_trips = trips_last_data[line_short_id]
                trips = pd.concat([trips, previous_trips])
                trips = trips.sort_values('update_time').drop_duplicates(subset=['id', 'stop_short_id'],
                                                                         keep='last')
                logging.info(f"[{line_name}] Enrich dataframe with previous data.")

                # Clean data older than 2 hours
                trips = trips[trips['update_time'] >= np.datetime64('now') - np.timedelta64(2, 'h')]
                logging.info(f"[{line_name}] Clean old data out of dataframe.")
            trips_last_data[line_short_id] = trips

        return trips


async def retrieve_data():
    global all_lines_trips

    line_short_ids = set(stops.line_short_id)
    # Only select first 5 lines in list for testing
    line_short_ids = list(line_short_ids)[:5]
    
    try:
        # Retrieve real-time data for all lines
        all_trips = await asyncio.gather(*[get_line_trips(line_id) for line_id in line_short_ids])
        
        for trips in all_trips:
            if trips is not None and not trips.empty:
                # Get interpolated coordinates/timestamps for line trips
                trips = compute_coords_timestamps(trips)

                # Get line attributes
                line_short_id = trips['line_short_id'].iloc[0]
                
                # Update all_line_trips
                all_lines_trips[line_short_id] = trips
        
                # # Save data to disk as compressed json
                # os.makedirs(os.path.join("data", "time_positions"))
                # filename = f'data/time_positions/{line_short_id}.json.gz'
                # with gzip.open(filename, 'wt', encoding="utf-8") as file:
                #     json.dump(time_positions.to_dict(), file, cls=NumpyEncoder)
                #     logging.info(f'[{line_name}] Saved data to {filename}.')

    except Exception as e:
        logging.error(traceback.format_exc())

    # Once data is retrieved, sleep until next scheduled fetch
    time_to_sleep = get_remaining_time_until_next_fetch()
    logging.info(f"Will sleep {time_to_sleep} seconds until next fetch...")
    await asyncio.sleep(time_to_sleep)

# Define the function that continuously retrieves data
def run_retrieve_data():
    while True:
        asyncio.run(retrieve_data())


async def retrieve_next_position(timestamp, frequency):
    global all_lines_trips

    data = {}

    def get_next_ts(lst, min_value, max_value):
        for element in lst:
            # Assuming each element is a tuple or a list with at least 2 elements
            if len(element) >= 2 and element[0] >= min_value and element[0] <= max_value:
                # Return the first time_position with time between min_value and max_value
                return element

        # Return None if no element meets the condition
        return None  

    for short_line_id in all_lines_trips:
        df = all_lines_trips[short_line_id]
        for i in range(len(df)):
            line = df.iloc[i].copy()
            if line.time_position:
                next_time_position = get_next_ts(line.time_position,
                                                 timestamp,
                                                 frequency+timestamp)
                
                if next_time_position:
                    line['time_position'] = next_time_position
                    line['time_generated'] = timestamp
                    data[line.id] = line.to_dict()

    # Save data to disk as compressed json
    if len(data) > 0:
        filename = 'data/next.json.gz'
        with gzip.open(filename, 'wt', encoding="utf-8") as file:
            # json.dump(data, file, cls=NumpyEncoder)
            json.dump(data, file)
            logging.info(f'Saved data to {filename}.')

def run_get_next_position():
    frequency = 10

    # Run every 10 seconds
    while True:
        # Get the current time
        now = time.time()
        print('run_get_next_position')
        asyncio.run(retrieve_next_position(now, frequency))

        # Sleep until next execution
        time.sleep(frequency - (time.time() - now))

# Create threads
thread1 = threading.Thread(target=run_retrieve_data)
thread2 = threading.Thread(target=run_get_next_position)

# Start threads
thread1.start()
thread2.start()


### DEBUG ###

# Test for one line
line_id = "C01383"  # METRO 13
# line_id = "C01727"  # RER C
# line_id = "C01743"  # RER B
shortest_paths = gpd.read_parquet(f'data/shortest_paths/{line_id}.parquet')
t = asyncio.run(get_line_trips(line_id))
