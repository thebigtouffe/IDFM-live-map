from shapely import MultiLineString
from shapely.ops import linemerge

import numpy as np
import networkx as nx

class Line:
    def __init__(self, id, name, company, transportation_type):
        self.id = id
        self.name = name
        self.company = company
        self.transportation_type = transportation_type
        self.segments = []
        self.graph = None
    
    def compute_graph(self):
        self.graph = linemerge(MultiLineString(self.segments))
    
    def __compute_adjacency_matrix(self):
        # TODO: make graph connected
        if self.graph is None:
            self.compute_graph()
        
        graph_segments = list(self.graph.geoms)
        adjacency_matrix=np.zeros((len(graph_segments), len(graph_segments)))
        for i, x in enumerate(graph_segments):
            for j, y in enumerate(graph_segments):
                connected = 0 if x.distance(y) > 0 else 1
                adjacency_matrix[i,j] = connected
        
        self.adjacency_matrix = adjacency_matrix
    
    def compute_shortest_paths(self):
        self.__compute_adjacency_matrix()
        G = nx.from_numpy_array(self.adjacency_matrix)
        self.shortest_paths = nx.shortest_path(G)

    def __repr__(self):
        return f"{self.name}"