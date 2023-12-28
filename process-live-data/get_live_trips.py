import os
import json
import urllib
import geopandas as gpd
import pandas as pd
import numpy as np
import asyncio
import time
import datetime
import aiohttp
from aiolimiter import AsyncLimiter

from src.PRIM_API import PRIM_API
from src.ArrivalTime import ArrivalTime

import logging
logging.basicConfig(format='[%(asctime)s.%(msecs)03d] %(levelname)-8s %(message)s',
                    level=logging.INFO, datefmt='%H:%M:%S')

# Limit to 50 requests/second
limiter = AsyncLimiter(50, time_period=1)

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

def get_train_name_from_trip_data(trip):
    name = ""

    # Get train name (if available)
    try:
        name = trip['MonitoredVehicleJourney']['JourneyNote'][0]['value']
    except:
        pass

    return name


def compute_short_id(x):
    return x.rstrip(":").split(":")[-1]


def parse_trip_json(trip, stop_short_id):
    stop_name = stops[stops.short_id == stop_short_id].iloc[0]['name']

    # Get trip attributes
    try:
        if 'ExpectedArrivalTime' in trip['MonitoredVehicleJourney']['MonitoredCall'].keys():
            arrival_time = trip['MonitoredVehicleJourney']['MonitoredCall']['ExpectedArrivalTime']
            arrival_time = ArrivalTime.parse_date_from_string(arrival_time)
            arrival_time = ArrivalTime(arrival_time)
        else:
            return None

        trip_id = trip['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']

        line_id = trip['MonitoredVehicleJourney']['LineRef']['value']
        line_short_id = compute_short_id(line_id)

        destination_id = trip['MonitoredVehicleJourney']['DestinationRef']['value']
        destination_id = compute_short_id(destination_id)
        
        trip_dict = {'id': trip_id,
                     'name': get_train_name_from_trip_data(trip),
                     
                     'stop_short_id': stop_short_id,
                     'stop_name': stop_name,
                     
                     'line_short_id': line_short_id,
                     'destination_id': destination_id,
                     
                     'arrival_time': arrival_time
                     }
        return trip_dict

    except Exception as e:
        logging.error(e)
        logging.error(trip)
        return None


async def get_next_trips_at_stop(stop_short_id, session, prim_api):
    # Create URL for the next trips of the stop
    url_arg = urllib.parse.quote(f"STIF:StopPoint:Q:{stop_short_id}:")
    url = prim_api.NEXT_TRIPS_BASE_URL + url_arg

    # Fetch data using the AsyncLimiter
    async with limiter:
        headers = {"apiKey": prim_api.api_key,
                   "accept": "application/json"
                   }

        # Fetch data using AioHttp
        async with session.get(url, headers=headers) as resp:
            json_data = await resp.json()
            logging.info(f"Got data for stop {stop_short_id} from {url}")

            try:
                trips = json_data['Siri']['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit']
            except Exception as e:
                logging.error(e)
                logging.error(json_data)

            return [parse_trip_json(trip, stop_short_id) for trip in trips]


def compute_coords_timestamps(trips, line_name, line_short_id):
    # Load shortest paths database
    shortest_paths = gpd.read_parquet(f'data/shortest_paths/{line_id}.parquet')
    shortest_paths['stop_short_id_start'] = shortest_paths['stop_id_start'].apply(lambda x: compute_short_id(x))
    shortest_paths['stop_short_id_end'] = shortest_paths['stop_id_end'].apply(lambda x: compute_short_id(x))
    logging.info(f"[{line_name}] Loaded shortest paths dataframe.")

    # Link time and positions for each trip
    trips['time_position'] = list(zip(trips.arrival_time, trips.stop_short_id, trips.stop_name))

    # Get all stops with arrival time for each trip
    groupby_fields = ['id', 'line_short_id', 'name', 'destination_id']
    df = trips.groupby(groupby_fields)['time_position'].unique().reset_index()

    # Sort list of stops by arrival time
    def sort_by_arrival_time(tps):
        return sorted(tps, key=lambda x: x[0].unix_timestamp)
    df['time_position'] = df['time_position'].apply(lambda x: sort_by_arrival_time(x))

    # Build trip path for each pairs of consecutive stops
    def build_path(tps, shortest_paths, line_short_id):
        coords = []
        timestamps = []

        for i in range(len(tps)-1):
            start_stop_short_id = tps[i][1]
            end_stop_short_id = tps[i+1][1]

            start_time = tps[i][0].unix_timestamp
            end_time = tps[i+1][0].unix_timestamp
            
            # Get shortest path between A and B
            cond1 = shortest_paths['stop_short_id_start'] == start_stop_short_id
            cond2 = shortest_paths['stop_short_id_end'] == end_stop_short_id
            cond3 = shortest_paths['line_short_id'] == line_short_id
            path = shortest_paths[cond1 & cond2]['line_geometry_interpolated']

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
                print(f"-----\n{stops[stops['short_id']==start_stop_short_id].iloc[0]}\n-----\n")
                print(f"-----\n{stops[stops['short_id']==end_stop_short_id].iloc[0]}\n-----\n")
                

        if len(coords) > 0 and len(timestamps) > 0:
            # Concatenate timestamps for each segment of the trip
            if len(timestamps) > 1:
                timestamps = np.concatenate(timestamps)

            return pd.Series([coords, timestamps])
        else:
            return pd.Series([None, None])
    
    df[['coords', 'timestamps']] = df.apply(lambda x: build_path(x.time_position, shortest_paths, line_id), axis=1)
    logging.info(f"[{line_name}] Computed coordinates and timestamps.")

    return df


def rebuild_trips(trips):
    return trips


async def get_line_trips(line_short_id):
    tasks = []

    stop_short_ids = stops[stops['line_short_id'] == line_short_id].short_id
    stop_short_ids = list(set(stop_short_ids.values))
    
    line_name = network[network['short_id']== line_short_id].iloc[0]['name']
    transportation_type = network[network['short_id']== line_short_id].iloc[0]['transportation_type']

    async with aiohttp.ClientSession() as session:
        # Fetch arrival times at each stop
        for short_id in stop_short_ids:
            tasks.append(asyncio.ensure_future(get_next_trips_at_stop(short_id, session, prim)))
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
            rebuild_trips(trips)

        return compute_coords_timestamps(trips, line_name, line_short_id)


async def main():
    while True:
        line_short_ids = set(stops.line_short_id)
        line_short_ids = list(line_short_ids)[:5] # Only select first 5 lines in list for testing

        arrival_times = await asyncio.gather(*[get_line_trips(x) for x in line_short_ids])

        # Once data is retrieved, sleep until next scheduled fetch
        time_to_sleep = get_remaining_time_until_next_fetch()
        logging.info(f"Will sleep {time_to_sleep} seconds until next fetch...")
        await asyncio.sleep(time_to_sleep)


# Run the main coroutine
# thread = asyncio.run(main())

# Test for one line
line_id = "C01383" # METRO 13
line_id = "C01727" # RER C
line_id = "C01743" # RER B
shortest_paths = gpd.read_parquet(f'data/shortest_paths/{line_id}.parquet')
t = asyncio.run(get_line_trips(line_id))
