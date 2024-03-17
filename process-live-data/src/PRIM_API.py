import requests
import json
import urllib.parse
import shutil
import logging
import pytz
import traceback
from tenacity import retry, stop_after_attempt, wait_fixed

from src.Line import Line
from src.Stop import Stop
from src.Trip import Trip
from src.Utils import Utils
from src.ArrivalTime import ArrivalTime

# Limit to 50 requests/second
from aiolimiter import AsyncLimiter
limiter = AsyncLimiter(50, time_period=1)

logging.basicConfig(format='[%(asctime)s.%(msecs)03d] %(levelname)-8s %(message)s',
                    level=logging.INFO, datefmt='%H:%M:%S')

class PRIM_API:

    # Data on stops
    STOPS_DATA_URL = "https://data.iledefrance-mobilites.fr/explore/dataset/arrets-lignes/download/?format=json"
    STOPS_DATA_FILE_PATH = "raw_data/stops.json"

    # Data on network (GeoJSON routes)
    NETWORK_DATA_URL = "https://data.iledefrance-mobilites.fr/explore/dataset/traces-du-reseau-ferre-idf/download/?format=json"
    NETWORK_DATA_FILE_PATH = "raw_data/network.json"

    # Next trip data
    NEXT_TRIPS_BASE_URL = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring?MonitoringRef=%s"

    # GTFS data (used for timetable)
    STATIC_GTFS_URL = "https://eu.ftp.opendatasoft.com/stif/GTFS/IDFM-gtfs.zip"
    STATIC_GTFS_FILE_PATH = "raw_data/gtfs.zip"
    STATIC_GTFS_PATH = "raw_data/gtfs"

    def __init__(self, api_key="dummy_api_key"):
        self.api_key = api_key
        self.lines = {}
        self.stops = {}
        self.trips = {}

    def __download_json_data(self, url, file_path):
        try:
            # Sending a GET request to the API endpoint
            response = requests.get(url)
            
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse JSON data from the response
                json_data = response.json()
                
                # Save JSON data to a file
                with open(file_path, "w") as file:
                    json.dump(json_data, file, indent=4)
                
                print(f"JSON data has been downloaded and saved to {file_path}.")
            else:
                # If the request was not successful, print the error code
                print("Error: Unable to fetch data from API - Status Code:", response.status_code)

        except requests.exceptions.RequestException as e:
            # Handle exceptions like network errors, timeout, etc.
            print("Error: ", e)

    def download_stops(self):
        url = self.STOPS_DATA_URL
        print("Downloading stops...")
        self.__download_json_data(url, self.STOPS_DATA_FILE_PATH)
    
    def download_network(self):
        url = self.NETWORK_DATA_URL
        print("Downloading network...")
        self.__download_json_data(url, self.NETWORK_DATA_FILE_PATH)
    
    def download_static_gtfs(self):
        url = self.STATIC_GTFS_URL
        print("Downloading static GTFS data...")
        with requests.get(url, stream=True) as r:
            with open(self.STATIC_GTFS_FILE_PATH, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
        print(f"Unzipping data to {self.STATIC_GTFS_PATH}...")
        shutil.unpack_archive(self.STATIC_GTFS_FILE_PATH, self.STATIC_GTFS_PATH)

    @staticmethod
    def get_train_name_from_trip_data(trip):
        name = ""

        # Get train name (if available)
        try:
            name = trip['MonitoredVehicleJourney']['JourneyNote'][0]['value']
        except:
            pass

        return name

    @staticmethod
    def parse_trip_json(trip, stop_short_id, line_short_id):
        # Get trip attributes
        try:
            stop_name = trip['MonitoredVehicleJourney']['MonitoredCall']['StopPointName'][0]['value']

            update_time = ArrivalTime.parse_date_from_string(trip['RecordedAtTime'])

            # Get Arrival Time in UTC
            monitoredcall_keys = trip['MonitoredVehicleJourney']['MonitoredCall'].keys()
            if 'ExpectedArrivalTime' in monitoredcall_keys:
                arrival_time = trip['MonitoredVehicleJourney']['MonitoredCall']['ExpectedArrivalTime']
            elif 'ExpectedDepartureTime' in monitoredcall_keys:
                arrival_time = trip['MonitoredVehicleJourney']['MonitoredCall']['ExpectedDepartureTime']
            else:
                return None
            arrival_time = ArrivalTime.parse_date_from_string(arrival_time)
            arrival_time = arrival_time.replace(tzinfo=pytz.timezone('UTC'))

            trip_id = trip['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']

            line_id = trip['MonitoredVehicleJourney']['LineRef']['value']

            # Sometimes data from other lines pollute trips
            if line_short_id != Utils.compute_short_id(line_id):
                logging.debug(f'Ignoring trip {trip_id} from line {line_id}')
                return None

            destination_id = trip['MonitoredVehicleJourney']['DestinationRef']['value']
            destination_id = Utils.compute_short_id(destination_id)
            

            def coalesce(*arg):
                return next((a for a in arg if a is not None), None)

            destination_name = coalesce(
                trip['MonitoredVehicleJourney']['MonitoredCall']['DestinationDisplay'][0]['value'] if len(trip['MonitoredVehicleJourney']['MonitoredCall']['DestinationDisplay']) > 0 else None,
                trip['MonitoredVehicleJourney']['DestinationName'][0]['value'] if len(trip['MonitoredVehicleJourney']['DestinationName']) > 0 else None,
                trip['MonitoredVehicleJourney']['DirectionName'][0]['value'] if len(trip['MonitoredVehicleJourney']['DirectionName']) > 0 else None
            )

            trip_dict = {'id': trip_id,
                        'name': PRIM_API.get_train_name_from_trip_data(trip),
                        'update_time': update_time,

                        'stop_short_id': stop_short_id,
                        'stop_name': stop_name,

                        'line_short_id': line_short_id,
                        'destination_id': destination_id,
                        'destination_name': destination_name,

                        'arrival_time': arrival_time
                        }

            return trip_dict

        except Exception as e:
            logging.error(traceback.format_exc())
            print(trip)
            return None


    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1)) # Retry using tenacity
    async def get_next_trips_at_stop(self, stop_short_id, line_short_id, session):
        # Create URL for the next trips of the stop
        url_arg_stop = urllib.parse.quote(f"STIF:StopPoint:Q:{stop_short_id}:")
        url = self.NEXT_TRIPS_BASE_URL % url_arg_stop

        # Fetch data using the AsyncLimiter
        async with limiter:
            headers = {
                "apiKey": self.api_key,
                "accept": "application/json"
            }

            # Fetch data using AioHttp
            async with session.get(url, headers=headers) as resp:
                json_data = await resp.json()
                logging.debug(f"Got data for stop {stop_short_id} from {url}")

                try:
                    trips = json_data['Siri']['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit']
                    return [self.parse_trip_json(trip, stop_short_id, line_short_id) for trip in trips]
                except Exception as e:
                    logging.error(e)
                    logging.error(json_data)
        
    
    def get_arrival_times_by_stop(self, stop):
        try:
            url = self.NEXT_TRIPS_BASE_URL + urllib.parse.quote(f"STIF:StopPoint:Q:{stop.get_short_id()}:")

            # Sending a GET request to the API endpoint
            response = requests.get(url, headers={"apiKey": self.api_key, "accept": "application/json"})
            
            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse JSON data from the response
                json_data = response.json()

                trips = json_data['Siri']['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit']
                for trip_data in trips:
                    self.__load_trip(trip_data, stop)
                
            else:
                # If the request was not successful, print the error code
                print("Error: Unable to fetch data from API - Status Code:", response.status_code)

        except requests.exceptions.RequestException as e:
            # Handle exceptions like network errors, timeout, etc.
            print("Error: ", e)

    def get_arrival_times(self):
        for stop_id, stop in self.stops.items():
            print(f"Get arrival times for stop {stop}")
            self.get_arrival_times_by_stop(stop)
