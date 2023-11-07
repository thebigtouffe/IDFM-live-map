from shapely import MultiLineString, LineString
from shapely.ops import linemerge
from shapely.ops import linemerge, substring

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
        self.shortest_path = {}
    
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
    
    def compute_segment_shortest_paths(self):
        self.__compute_adjacency_matrix()
        G = nx.from_numpy_array(self.adjacency_matrix)
        self.segment_shortest_paths = nx.shortest_path(G)
    
    def compute_path_between_two_stops(self, stop1, stop2):
        line = self
        path = None

        if line.graph.geom_type == "MultiLineString":
            """
            If the geometry of the Line object is a graph with max degree > 2, we
            have to build the shortest path LineString between stop1 and stop2 using the 
            trimmed sub-segments (a sub-segment is a LineString within a MultiLineString)
            from the graph.
            For each sub-segment we find the intersection with the next sub-segment
            and we trim the sub-segment to that intersection.
            """
            segment_indices = line.segment_shortest_paths[stop1.segment_idx][stop2.segment_idx]
            
            left = line.graph.geoms[segment_indices[0]]
            first_segment_start_idx = left.coords[:].index(stop1.point_on_graph.coords[0])

            intermediates_points_idx = []
            for i in range(len(segment_indices) - 1):
                left = line.graph.geoms[segment_indices[i]]
                right = line.graph.geoms[segment_indices[i+1]]

                left_point_idx = left.coords[:].index((left.intersection(right)).coords[0])
                right_point_idx = right.coords[:].index((right.intersection(left)).coords[0])

                intermediates_points_idx.append(left_point_idx)
                intermediates_points_idx.append(right_point_idx)
            
            right = line.graph.geoms[segment_indices[-1]]
            last_segment_end_idx = right.coords[:].stop2.point_on_graph.coords[0]

            # Convert list of indices to list of tuple (start_idx, end_idx) corresponding to each segment
            points_idx = [first_segment_start_idx, *intermediates_points_idx, last_segment_end_idx]
            points_idx = [(points_idx[i], points_idx[i+1]) for i in range(0, len(points_idx2), 2)]

            path = []
            for i in range(segment_indices):
                segment_part_i = line.graph.geoms[segment_indices[i]]
                start, end = points_idx[i]
                if start > end:
                    path.append(segment_part_i[start:end:-1])
                else:
                    path.append(segment_part_i[start:end:1])
            
            path = LineString(*path)
        
        elif line.graph.geom_type == "LineString":
            path = line.graph
            start_distance = path.project(stop1.point_on_graph)
            end_distance = path.project(stop2.point_on_graph)
            path = substring(path, start_distance, end_distance)
        
        if not stop1.id in line.shortest_path:
            line.shortest_path[stop1.id] = {}
        line.shortest_path[stop1.id][stop2.id] = path
    
    def get_path_between_two_stops(self, stop1, stop2):
        if not (stop1.id in self.shortest_path and stop2.id in self.shortest_path[stop1.id]):
            self.compute_path_between_two_stops(stop1, stop2)
        return self.shortest_path[stop1.id][stop2.id]
    
    def __repr__(self):
        return f"{self.name}"