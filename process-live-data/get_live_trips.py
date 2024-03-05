import os
import json
import urllib
import geopandas as gpd
import pandas as pd
import numpy as np
import asyncio
import time
import datetime
import pytz
import aiohttp
from aiolimiter import AsyncLimiter

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

all_trips = {}

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


def compute_coords_timestamps(trips, line_name, line_short_id):
    # Load shortest paths database
    sp = gpd.read_parquet(f'data/shortest_paths/{line_short_id}.parquet')
    sp['stop_short_id_start'] = sp['stop_id_start'].apply(lambda x: Utils.compute_short_id(x))
    sp['stop_short_id_end'] = sp['stop_id_end'].apply(lambda x: Utils.compute_short_id(x))
    logging.info(f"[{line_name}] Loaded shortest paths dataframe.")

    # Link time and positions for each trip
    trips['time_position'] = list(zip(trips.arrival_time,
                                      trips.stop_short_id,
                                      trips.stop_name))

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
            if len(timestamps) > 1:
                timestamps = np.concatenate(timestamps)

            return pd.Series([coords, timestamps])
        else:
            return pd.Series([None, None])

    df[['coords', 'timestamps']] = df.apply(lambda x: build_path(x.time_position,
                                                                 sp,
                                                                 line_short_id),
                                            axis=1)
    logging.info(f"[{line_name}] Computed coordinates and timestamps.")

    return df


def rebuild_trip_ids_from_timetable(trips, timetable):
    trips['processed'] = False
    trips['row_id'] = trips.index

    # Get the current date
    day_of_week = datetime.datetime.now().strftime("%A")

    # Define Paris time zone
    paris_tz = pytz.timezone('CET')

    today = datetime.date.today()
    now = datetime.datetime.now(paris_tz)

    # Filter timetable for current day
    timetable = timetable[timetable[day_of_week.lower()] == True]
    tt = timetable.copy()
    print(tt)

    # Parse data
    tt['stop_short_id'] = tt['stop_id'].apply(lambda x: Utils.compute_short_id(x))
    tt['arrival_time'] = tt['arrival_time'].apply(lambda x: GTFS.parse_time(x, tzinfo=paris_tz))
    tt['start_date'] = tt['start_date'].apply(lambda x: GTFS.parse_date(x))
    tt['end_date'] = tt['end_date'].apply(lambda x: GTFS.parse_date(x))
    print(tt.iloc[0])

    # Filter on future scheduled trains
    tt = tt[today >= tt['start_date']]
    tt = tt[today - datetime.timedelta(days=1) <= tt['end_date']]
    tt = tt[tt['arrival_time'] >= now]
    print(tt)

    # Parse trip destination
    def append_destination(group):
        group['destination_id'] = group['stop_short_id'].iloc[-1]
        return group
    tt = tt.sort_values(by=['stop_sequence']).groupby('trip_id').apply(append_destination)
    tt = tt.reset_index(drop=True)
    print(tt)

    # Substract 120 seconds to arrival_time because the timetable is only accurate to the minute
    # and trains can arrive up to 120 seconds earlier than expected
    # We'll use search_earliest for matching with real time data
    tt['search_earliest'] = tt['arrival_time'] - datetime.timedelta(seconds=120)
    print(tt)

    # Filter on trains expected within the next 60 minutes
    tt = tt[tt['arrival_time'] < now + datetime.timedelta(minutes=60)]
    print(tt)

    # Iterate over stops from real-time data
    for stop in set(trips.stop_short_id):
        print("-------------------")
        print(f"Processing stop {stop}")

        # Get next scheduled trains for the stop and sort by their arrival time
        stop_tt_key = ['trip_id', 'destination_id', 'arrival_time', 'search_earliest']
        stop_tt = tt[tt['stop_short_id'] == stop][stop_tt_key]
        stop_tt = stop_tt.sort_values(by=['arrival_time'])
        stop_tt = stop_tt.head(5)

        print("\nScheduled trips:")
        print(stop_tt)

        # Bind scheduled trains with the closest real-time arrivals
        stop_trips = trips[trips['stop_short_id'] == stop]
        stop_trips = stop_trips.sort_values(by=['arrival_time'])

        print("\nStop trips:")
        print(stop_trips)

        print("\nIterating over timetable at stop...\n")
        for i, x in stop_tt.iterrows():
            # Get the first unprocessed real-time data trip 
            filter = stop_trips['processed'] == False
            # that is expected after scheduled arrival time
            filter &= stop_trips['arrival_time'] >= x.search_earliest
            # for the correct destination
            filter &= stop_trips['destination_id'] == x.destination_id

            if not stop_trips[filter].empty:
                rt_row_id = stop_trips[filter].iloc[0].row_id
                print(f"\tFound scheduled trip to {x.destination_id} at index {rt_row_id}")

                # Replace trip id of real-time data with trip_id from schedule
                stop_trips.at[rt_row_id, 'processed'] = True
                trips.at[rt_row_id, 'id'] = x.trip_id
                trips.at[rt_row_id, 'processed'] = True

                # print(stop_trips)

        print("-------------------\n")

    return trips


async def get_line_trips(line_short_id):
    tasks = []

    stop_short_ids = stops[stops['line_short_id'] == line_short_id].short_id
    stop_short_ids = list(set(stop_short_ids.values))

    line_name = network[network['short_id'] == line_short_id].iloc[0]['name']
    transportation_type = network[network['short_id']
                                  == line_short_id].iloc[0]['transportation_type']

    async with aiohttp.ClientSession() as session:
        # Fetch arrival times at each stop
        for short_id in stop_short_ids:
            tasks.append(asyncio.ensure_future(
                prim.get_next_trips_at_stop(short_id, session)))
        logging.info(f"[{line_name}] Created async tasks.")

        responses = await asyncio.gather(*tasks)
        logging.info(f"[{line_name}] Executed {len(tasks)} tasks.")

        # Generate trips dataframe for the line
        trips = [t for r in responses for t in r if t is not None]
        trips = pd.DataFrame.from_dict(trips)
        logging.info(f"[{line_name}] Generated dataframe from response.")

        # RATP data is not complete for metro and tramway
        # Thus we have to manually build trips using the timetable and real-time data for next trains.
        if transportation_type in ("TRAMWAY", "METRO"):
            timetable_dir = os.path.join('data', 'timetable')
            timetable_path = os.path.join(timetable_dir, line_short_id)
            timetable = pd.read_parquet(timetable_path)

            # rebuild_trip_ids_from_timetable(trips, timetable)
            # trips = trips[trips.processed == True]
        
        # Add previous data for lines with trip id. Keep latest data.
        else:
            if line_id in all_trips.keys():
                previous_trips = all_trips[line_id]
                trips = pd.concat([trips, previous_trips])
                trips = trips.sort_values('update_time').drop_duplicates(subset=['id', 'stop_short_id'], keep='last')
                logging.info(f"[{line_name}] Enrich dataframe with previous data.")

                # Clean data older than 2 hours
                trips = trips[trips['update_time'] >= np.datetime64('now') - np.timedelta64(2, 'h')]
                logging.info(f"[{line_name}] Clean old data out of dataframe.")
            all_trips[line_id] = trips

        return trips
        return compute_coords_timestamps(trips, line_name, line_short_id)


async def main():
    while True:
        line_short_ids = set(stops.line_short_id)
        # Only select first 5 lines in list for testing
        line_short_ids = list(line_short_ids)[:5]

        arrival_times = await asyncio.gather(*[get_line_trips(x) for x in line_short_ids])

        # Once data is retrieved, sleep until next scheduled fetch
        time_to_sleep = get_remaining_time_until_next_fetch()
        logging.info(f"Will sleep {time_to_sleep} seconds until next fetch...")
        await asyncio.sleep(time_to_sleep)


# Run the main coroutine
# thread = asyncio.run(main())

# Test for one line
# line_id = "C01383"  # METRO 13
line_id = "C01727"  # RER C
# line_id = "C01743"  # RER B
shortest_paths = gpd.read_parquet(f'data/shortest_paths/{line_id}.parquet')
t = asyncio.run(get_line_trips(line_id))
