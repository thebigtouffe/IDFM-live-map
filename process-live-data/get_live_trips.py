import os
import json
import urllib

import geopandas as gpd
import pandas as pd
import networkx as nx
import momepy

from shapely import LineString, Point, MultiPoint
from shapely.ops import nearest_points, linemerge

from itertools import combinations
import random

import aiohttp
from aiolimiter import AsyncLimiter
import asyncio
import time
import datetime

from src.PRIM_API import PRIM_API
from src.ArrivalTime import ArrivalTime

start_time = time.time()

# Limit to X requests/second
limiter = AsyncLimiter(50, time_period=1)

# Load settings from settings.json
with open('settings.json', 'r') as json_file:
    settings_data = json.load(json_file)

prim = PRIM_API(api_key=settings_data["prim_api_key"])

print("Get settings: %s seconds" % (time.time() - start_time))

network = gpd.read_file('data/network.json')
stops = gpd.read_file('data/stops.json')

print("Load network and stops: %s seconds" % (time.time() - start_time))

def get_remaining_time_until_next_fetch():
    # Get the current time
    now = datetime.datetime.now()
    print(now)
    h = now.hour
    m = now.minute

    if h == 5:
        fetch_frequency = 4
    elif h == 6:
        fetch_frequency = 3
    elif h >= 0 and h < 2:
        fetch_frequency = 4
    elif h >=7 and h <= 22:
        fetch_frequency = 2
    elif h == 23:
        fetch_frequency = 3
    else:
        fetch_frequency = 15
    
    remaining_time = 60 * (fetch_frequency - m % fetch_frequency)
    return remaining_time


async def get_next_trains_from_stop(stop_id, url, session, prim_api):
    async with limiter:
        async with session.get(url, headers={"apiKey": prim_api.api_key, "accept": "application/json"}) as resp:
            json_data = await resp.json()
            print(f"Got data from {url}")

            try:
                trips = json_data['Siri']['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit']
            except:
                print(json_data)
                

            def parse_json_data(trip):
                # Get train name (if SNCF)
                try:
                    name = trip['MonitoredVehicleJourney']['TrainNumbers']['TrainNumberRef'][0]['value']
                except:
                    name = ""

                # Get trip attributes
                try:
                    arrival_time = trip['MonitoredVehicleJourney']['MonitoredCall']['ExpectedArrivalTime']
                    arrival_time = ArrivalTime.parse_date_from_string(arrival_time)
                    arrival_time = ArrivalTime(arrival_time)

                    id = trip['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']
                    line_id = trip['MonitoredVehicleJourney']['LineRef']['value']
                    line_id = line_id.rstrip(":").split(":")[-1]

                    return {'id': id, 'stop_id': stop_id, 'name': name, 'line_id': line_id, 'arrival_time': arrival_time}
                except:
                    print(trip)
                    return None

            return [parse_json_data(trip) for trip in trips]

def build_time_path(df, line_id):
    shortest_paths = gpd.read_file(f'data/shortest_paths/{line_id}.gpkg')


async def fetch_arrival_times(line_id):
    tasks = []
    short_ids = list(set(stops[stops['line_id']==line_id].short_id.values))

    async with aiohttp.ClientSession() as session:
        for short_id in short_ids:
            url = prim.NEXT_TRIPS_BASE_URL + urllib.parse.quote(f"STIF:StopPoint:Q:{short_id}:")
            tasks.append(asyncio.ensure_future(get_next_trains_from_stop(short_id, url, session, prim)))
        print("Created async tasks: %s seconds" % (time.time() - start_time))

        responses = await asyncio.gather(*tasks)
        print("Execute taks: %s seconds" % (time.time() - start_time))

        next_trains = [t for r in responses for t in r if t is not None]
        next_trains = pd.DataFrame.from_dict(next_trains)

        next_trains['time_position'] = list(zip(next_trains.stop_id, next_trains.arrival_time))
        next_trains = next_trains.groupby(['id', 'line_id'])['time_position'].agg(['unique'])

        

        print("Generate dataframe from response: %s seconds" % (time.time() - start_time))
        return next_trains


async def main():
    while True:
        # line_ids = set(stops.line_id)
        line_ids = list(set(stops.line_id))[:5]

        arrival_times = await asyncio.gather(
            *[fetch_arrival_times(line_id) for line_id in line_ids]
        )

        time_to_sleep = get_remaining_time_until_next_fetch()
        print(f"Will sleep {time_to_sleep} seconds until next fetch...")
        await asyncio.sleep(time_to_sleep)


# Run the main coroutine
thread = asyncio.run(main())