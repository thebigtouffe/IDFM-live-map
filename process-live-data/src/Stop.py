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

    def __repr__(self):
        return f"{self.name} ({self.line.name})"

    def get_short_id(self):
        return self.id.split(":")[-1]
    
    def get_nearest_point_on_graph(self):
        self.position_on_graph = nearest_points(self.line.graph, self.position)[0]
    
    def get_line_graph_segment(self):
        if not "position_on_graph" in dir(self):
            self.get_nearest_point_on_graph()

        # Get the closest segment of the line graph
        if "geoms" in dir(self.line.graph):
            distances = [segment.distance(self.position_on_graph) for segment in list(self.line.graph.geoms)]
            self.segment_idx = min(enumerate(distances), key=lambda x: x[1])[0]
        else:
            self.segment_idx = 0