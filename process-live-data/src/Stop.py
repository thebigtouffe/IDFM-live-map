from src.Line import Line

from shapely import Point
from shapely.ops import nearest_points

class Stop:
    def __init__(self, id, company, name, lon, lat, city, line):
        self.id = id
        self.company = company
        self.name = name
        self.position = Point(lon, lat)
        self.city = city
        self.line : Line = line
        self.ridership = 0

    def __repr__(self):
        return f"{self.name} ({self.line.name})"

    def get_short_id(self):
        return self.id.split(":")[-1]
    
    def get_nearest_point_on_graph(self):
        # Get the closest segment of the line graph
        if self.line.graph.geom_type == "MultiLineString":
            distances = [segment.distance(self.position) for segment in list(self.line.graph.geoms)]
            self.segment_idx = min(enumerate(distances), key=lambda x: x[1])[0]
            segment = self.line.graph.geoms[self.segment_idx]
        else:
            self.segment_idx = 0
            segment = self.line.graph
        
        distances = [Point(s_point).distance(self.position) for s_point in segment.coords[:]]
        point_on_segment_idx = min(enumerate(distances), key=lambda x: x[1])[0]
        self.point_on_graph = Point(segment.coords[point_on_segment_idx])
    
    def get_line_graph_segment(self):
        if not "point_on_graph" in dir(self):
            self.get_nearest_point_on_graph()

        # Get the closest segment of the line graph
        if self.line.graph.geom_type == "MultiLineString":
            distances = [segment.distance(self.point_on_graph) for segment in list(self.line.graph.geoms)]
            self.segment_idx = min(enumerate(distances), key=lambda x: x[1])[0]
        else:
            self.segment_idx = 0
    
    def estimate_waiting_time(self):
        """Estimate the time spent waiting for passengers at station based on ridership and time of the day"""
        return 10