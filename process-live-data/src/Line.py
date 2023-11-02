from shapely import MultiLineString
from shapely.ops import linemerge
import numpy as np

class Line:
    def __init__(self, id, name, company, transportation_type, segments=[]):
        self.id = id
        self.name = name
        self.company = company
        self.transportation_type = transportation_type
        self.segments : List = segments
        self.graph = None
    
    def compute_graph(self):
        self.graph = linemerge(MultiLineString(self.segments))
    
    def compute_adjacence_matrix(self):
        if self.graph is None:
            self.compute_graph()
        
        graph_segments = list(self.graph.geoms)
        adjacence_matrix=np.zeros((len(graph_segments), len(graph_segments)))
        for i, x in enumerate(graph_segments):
            for j, y in enumerate(graph_segments):
                adjacence_matrix[i,j] = x.distance(y)
        print(adjacence_matrix)


    def __repr__(self):
        return f"{self.name}"