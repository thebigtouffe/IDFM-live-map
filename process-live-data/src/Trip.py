from src.Stop import Stop
from src.Line import Line
from src.ArrivalTime import ArrivalTime

import re

class Trip:
    RATP_ID_PARSE = re.compile("RATP-SIV:VehicleJourney::(\d+\.\d+\.[A-Z])\.\w+")

    def __init__(self, id, line, name=""):
        self.id = id
        self.name = name
        self.line : Line = line
        self.stops : dict = {} # dict format: {stop_id: (stop, ArrivalTime)}
        self.stop_list : list[(Stop, ArrivalTime)] = []
    
    def compute_stop_list(self):
        """Compute list[(Stop, ArrivalTime)] sorted by arrival time"""
        self.stop_list = [x[1] for x in self.stops.items()]
        self.stop_list.sort(key=lambda x: x[1].time_seconds)
    
    def compute_graph_with_time(self):
        self.compute_stop_list()


    
    @classmethod
    def parse_metro_trip_short_name_from_id(cls, id):
        result = re.search(cls.RATP_ID_PARSE, id)
        if result:
            name = result.group(1)
            return name

    def __repr__(self):
        if self.name != "":
            return f"{self.name} ({self.line.name})"
        
        elif self.line.transportation_type == "METRO":
            metro_trip_short_name = self.parse_metro_trip_short_name_from_id(self.id)
            return f"{self.line.name}: {metro_trip_short_name}"
        
        else:
            return f"{self.id}"