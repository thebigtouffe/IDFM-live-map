import requests
import json
import urllib.parse

from src.Line import Line
from src.Stop import Stop
from src.Trip import Trip
from src.ArrivalTime import ArrivalTime

class PRIM_API:

    # Data on stops
    STOPS_DATA_URL = "https://data.iledefrance-mobilites.fr/explore/dataset/arrets-lignes/download/?format=json"
    STOPS_DATA_FILE_PATH = "data/stops.json"

    # Data on network (GeoJSON routes)
    NETWORK_DATA_URL = "https://data.iledefrance-mobilites.fr/explore/dataset/traces-du-reseau-ferre-idf/download/?format=json"
    NETWORK_DATA_FILE_PATH = "data/network.json"

    # Next trip data
    NEXT_TRIPS_BASE_URL = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring?MonitoringRef="

    def __init__(self, api_key="dummy_api_key"):
        self.api_key = api_key
        self.lines = {}
        self.stops = {}
        self.trips = {}

    def __download_data(self, url, file_path):
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
                
                print("JSON data has been downloaded and saved to 'data.json' file.")
            else:
                # If the request was not successful, print the error code
                print("Error: Unable to fetch data from API - Status Code:", response.status_code)

        except requests.exceptions.RequestException as e:
            # Handle exceptions like network errors, timeout, etc.
            print("Error: ", e)
    
    def download_stops(self):
        url = self.STOPS_DATA_URL
        print("Downloading stops...")
        self.__download_data(url, self.STOPS_DATA_FILE_PATH)
    
    def download_network(self):
        url = self.NETWORK_DATA_URL
        print("Downloading network...")
        self.__download_data(url, self.NETWORK_DATA_FILE_PATH)

    def __load_line_segment(self, data):
        id = data["fields"]["idrefligc"]
        segment = data["fields"]["geo_shape"]["coordinates"]

        # Add each line segment to segment attribute of Line instance
        if not id in self.lines.keys():
            name = data["fields"]["res_com"]
            company = data["fields"]["exploitant"]
            transportation_type = data["fields"]["mode"]
            
            line = Line(id, name, company, transportation_type, segments=[segment])
            self.lines[id] = line
        else:
            self.lines[id].segments.append(segment) 

    def load_network(self, file_path=NETWORK_DATA_FILE_PATH):
        try:
            # Load JSON data from file
            with open(file_path, "r") as json_file:
                json_data = json.load(json_file)

            # Parse JSON data into a class instance
            print(f"Loaded {len(json_data)} objects from {file_path}")
            for data in json_data:
                self.__load_line_segment(data)
            
            # Compute line graph from segments
            for id, line in self.lines.items():
                line.compute_graph()

        except FileNotFoundError:
            print("Error: File not found.")
        except json.JSONDecodeError:
            print("Error: Invalid JSON format in the file.")
        except KeyError:
            print("Error: JSON data does not contain expected keys.")
        except Exception as e:
            print("Error:", e)

    def __parse_stop_line_id(self, line_id):
        return line_id.split(":")[-1]
    
    def __load_stop(self, data):
        id = data["fields"]["stop_id"]

        if not id in self.lines.keys():
            name = data["fields"]["stop_name"]
            company = data["fields"]["operatorname"]
            lon = data["fields"]["stop_lon"]
            lat = data["fields"]["stop_lat"]
            city = data["fields"]["nom_commune"]
            line_id = self.__parse_stop_line_id(data["fields"]["id"])
            
            if line_id in self.lines.keys():
                line = self.lines[line_id]
                stop = Stop(id, company, name, lon, lat, city, line)
                stop.get_nearest_point_on_graph()
                stop.get_line_graph_segment()
                self.stops[id] = stop
    
    def load_stops(self, file_path=STOPS_DATA_FILE_PATH):
        try:
            # Load JSON data from file
            with open(file_path, "r") as json_file:
                json_data = json.load(json_file)

            # Parse JSON data into a class instance
            print(f"Loaded {len(json_data)} objects from {file_path}")
            for data in json_data:
                self.__load_stop(data)

        except FileNotFoundError:
            print("Error: File not found.")
        except json.JSONDecodeError:
            print("Error: Invalid JSON format in the file.")
        except KeyError:
            print("Error: JSON data does not contain expected keys.")
        except Exception as e:
            print("Error:", e)
    
    def __load_trip(self, data, stop):
        # print(data)

        try:
            # Get trip attributes
            id = data['MonitoredVehicleJourney']['FramedVehicleJourneyRef']['DatedVehicleJourneyRef']

            # Get train name (if SNCF)
            try:
                name = data['MonitoredVehicleJourney']['TrainNumbers']['TrainNumberRef'][0]['value']
            except:
                name = ""
            
            line_id = data['MonitoredVehicleJourney']['LineRef']['value']
            line_id = line_id.rstrip(":").split(":")[-1]
        
            arrival_time = data['MonitoredVehicleJourney']['MonitoredCall']['ExpectedArrivalTime']
            arrival_time = ArrivalTime.parse_date_from_string(arrival_time)
            arrival_time = ArrivalTime(arrival_time)

            # Update/create trip object and update list of stops
            if id not in self.trips.keys():
                self.trips[id] = Trip(id, self.lines[line_id], name=name)
            self.trips[id].stops[stop.id] = (stop, arrival_time)

        except Exception as e:
            print("Error: ", e)
        
    
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
