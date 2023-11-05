from src.Stop import Stop
from src.Line import Line
from src.ArrivalTime import ArrivalTime

from shapely.ops import linemerge, substring
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
    
    def compute_position_times(self):
        self.compute_stop_list()

        def build_time_segment_between_two_stops(line, stop1, time1, stop2, time2):
            """Return List[{coordinates: List, timestamp: float}] for the trip section between stop1 and stop2.
            time1 and time2 are timestamps (int).
            time2 must be greater than time1.
            """

            if line.graph.geometryType() == "MultiLineString":
                segment_indices = line.shortest_paths[stop1.segment_idx][stop2.segment_idx]
                segment = linemerge([line.graph.geoms[i] for i in segment_indices])
            else:
                segment = line.graph

            start_distance = segment.project(stop1.position_on_graph)
            end_distance = segment.project(stop2.position_on_graph)
            segment = substring(segment, start_distance, end_distance)
            print(segment)

            # Animate using 10 point interpolation
            INTERPOLATION_POINTS = 10
            time_segment = []
            for i in range(INTERPOLATION_POINTS):
                timestamp = time1 + (i * (time2 - time1) / (INTERPOLATION_POINTS - 1))
                x, y = segment.interpolate(i/(INTERPOLATION_POINTS - 1), normalized=True).coords[0]
                time_segment.append({'coordinates': [x, y], 'timestamp': timestamp})
            return time_segment
        
        time_segments = []
        for i in range(len(self.stop_list) - 1):
            stop1, time1 = self.stop_list[i]
            stop2, time2 = self.stop_list[i+1]

            # Get departure time in seconds at n-th stop and arrival time in seconds at (n+1)-th stop
            timestamp1 = time1.time_seconds + stop1.estimate_waiting_time()
            timestamp2 = time2.time_seconds

            time_segments.append(build_time_segment_between_two_stops(self.line, stop1, timestamp1, stop2, timestamp2))
        self.time_segments = time_segments

    
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