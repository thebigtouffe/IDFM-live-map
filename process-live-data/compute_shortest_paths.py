import os
import json

import geopandas as gpd
import pandas as pd
import networkx as nx
import momepy

from shapely import LineString, Point, MultiPoint
from shapely.ops import nearest_points, linemerge

import matplotlib.pyplot as plt

from itertools import combinations
import random

from src.PRIM_API import PRIM_API

# Load settings from settings.json
with open('settings.json', 'r') as json_file:
    settings_data = json.load(json_file)

prim = PRIM_API(api_key=settings_data["prim_api_key"])

# Download railroad network and stops database
if not os.path.exists(prim.NETWORK_DATA_FILE_PATH):
    print("Download network data.")
    prim.download_network()
if not os.path.exists(prim.STOPS_DATA_FILE_PATH):
    print("Download stops data.")
    prim.download_stops()

# Load railroad network and get relevant fields
print("Loading networks...")
with open(prim.NETWORK_DATA_FILE_PATH, 'r') as data:
    network_df = pd.json_normalize(json.load(data))

network_relevant_fields = {    
                           "fields.idrefligc": 'id',
                           "fields.geo_shape.coordinates": 'geometry',
                           "fields.res_com": 'name',
                           "fields.exploitant": 'company',
                           "fields.mode": 'transportation_type',
                           "fields.colourweb_hexa": 'color',
                           "fields.idf": 'in_idf',
                           "fields.picto_final": 'picture_url'
}
network_df = network_df[list(network_relevant_fields.keys())]
network_df = network_df.rename(columns=network_relevant_fields)
network_df = network_df[network_df.transportation_type.isin(['TRAMWAY', 'RER', 'METRO', 'TRAIN'])]
network_df.geometry = network_df.geometry.apply(lambda x: LineString(x))
network_df = gpd.GeoDataFrame(network_df, geometry='geometry')

print("Network loaded as a GeoDataFrame.")

# Load stop database and get relevant fields
print("Loading stops...")
with open(prim.STOPS_DATA_FILE_PATH, 'r') as data:
    stops_df = pd.json_normalize(json.load(data))

stops_relevant_fields = {    
                           "fields.stop_id": 'id',
                           "fields.stop_lon": 'longitude',
                           "fields.stop_lat": 'latitude',
                           "fields.stop_name": 'name',
                           "fields.id": 'line_id',
                           "fields.operatorname": 'company',
}
stops_df = stops_df[list(stops_relevant_fields.keys())]
stops_df = stops_df.rename(columns=stops_relevant_fields)

# Match line ID format with network dataframe
stops_df.line_id = stops_df.line_id.apply(lambda x: x.split(":")[-1])

# Only use stops of railroad network (metro, train, tramway)
stops_df = stops_df[stops_df.line_id.isin(network_df.id)]
stops_df = gpd.GeoDataFrame(stops_df, geometry=gpd.points_from_xy(stops_df.longitude, stops_df.latitude))

print("Stops loaded as a GeoDataFrame.")

# Iterate over each line
line_names = list(set(network_df.name.values))
# line_names = ['METRO 13']
all_line_stops_pairs = []
for name in line_names:
    print(f"\n-------\nComputing data for {name}.")
    line = network_df[network_df.name == name].copy()
    line_stops=stops_df[stops_df.line_id == line.id.iloc[0]].copy()

    # Compute network graph from geospatial data
    G = momepy.gdf_to_nx(line, approach="primal")
    nodes = list(G.nodes)
    print("Graph computed.")

    # Network graph is sometimes not connected due to data error
    if not nx.is_connected(G):
        print("--- Graph not connected!")
        graph_components = list(nx.connected_components(G))

        # Get pairs of disconnected subgraphes
        segments = []
        for pair in list(combinations(graph_components, 2)):
            x = MultiPoint(list(pair[0]))
            y = MultiPoint(list(pair[1]))
            distance = x.distance(y)

            # Create the shortest segment linking subgraphes nodes
            if distance > 0.0 and distance < settings_data["max_distance_between_two_subgraphes"]:
                node1, node2 = nearest_points(x, y)
                segment = LineString([node1, node2])
                segments.append(segment)
                
        new_rows = line.head(len(segments)).copy()
        new_rows.geometry = segments
        line = pd.concat([line, new_rows])

        # Recompute network graph
        G = momepy.gdf_to_nx(line, approach="primal")
        nodes = list(G.nodes)
        if nx.is_connected(G):
            print("--- Network graph artificially connected.")
            print(f"--- Added segments: {segments}")

    # Get nodes as geodataframe
    nodes_gdf = momepy.nx_to_gdf(G, points=True, lines=False, spatial_weights=True)[0]
    nodes_position = nodes_gdf.geometry.unary_union

    def nearest_node(point, nodes_position=nodes_position, nodes_gdf=nodes_gdf):
        # find the nearest point and return the corresponding Node in graph
        nearest = nodes_gdf.geometry == nearest_points(point, nodes_position)[1]
        return nodes_gdf[nearest].nodeID.values[0]

    # Compute nearest node on graph for each stop
    print("Computing nearest node on graph for each stop.")
    line_stops['nearest_node_on_graph'] = line_stops.apply(lambda row: nearest_node(row.geometry), axis=1)

    # Plot
    # f, ax = plt.subplots(1, 1, figsize=(6, 6), sharex=True, sharey=True)
    # line.plot(color="#"+line.color.iloc[0], ax=ax)
    # line_stops.plot(color="blue", ax=ax)
    # ax.set_title(line.name.iloc[0])
    # nx.draw(G, {n: [n[0], n[1]] for n in nodes}, ax=ax, node_size=3)
    # plt.show()

    # Compute shortest paths on line graph
    print("Computing shortest paths on network graph for each pair of stops...")
    shortest_paths = nx.shortest_path(G)

    # Compute pairs of stops by using the cartesian product of the dataframe with itself
    line_stops_pairs = line_stops.assign(dummy=1).merge(line_stops.assign(dummy=1), on='dummy', how='outer', suffixes=('_start', '_end'))
    line_stops_pairs = line_stops_pairs.drop('dummy', axis=1)
    line_stops_pairs = line_stops_pairs[line_stops_pairs.nearest_node_on_graph_start != line_stops_pairs.nearest_node_on_graph_end]
    print(f"{len(line_stops_pairs)} pairs to process.")

    # Compute shortest path for each pair
    line_stops_pairs['shortest_path'] = line_stops_pairs.apply(lambda row: shortest_paths[nodes[row.nearest_node_on_graph_start]][nodes[row.nearest_node_on_graph_end]], axis=1)
    # Get the segments of the shortest path for further comparison with network dataframe
    line_stops_pairs['shortest_path_segments'] = line_stops_pairs.apply(lambda row: [LineString(x) for x in list(zip(row.shortest_path[0:-1], row.shortest_path[1:]))], axis=1)

    # Compute line network with reverse segment
    print("Extending network data with flipped segments.")
    reverse_line = line.copy()
    reverse_line.geometry = reverse_line.reverse()
    line = pd.concat([line, reverse_line])

    print("Building actual paths for every pair...")
    
    # Compute (start, end) of each line segment
    line['start_node'] = line.geometry.apply(lambda r: r.coords[0])
    line['end_node'] = line.geometry.apply(lambda r: r.coords[-1])

    # Build a path on network using segments of the shortest path between two stops
    def get_shortest_path_from_segments(segments, line):
        path = []
        for s in segments:
            s_start_node, s_end_node = s.coords[:]
            s_path = line[(line.start_node == s_start_node) & (line.end_node == s_end_node)].geometry.iloc[0]
            path.append(s_path)
        return linemerge(path)

    line_stops_pairs['shortest_path'] = line_stops_pairs.shortest_path_segments.apply(lambda row: get_shortest_path_from_segments(row, line))
    all_line_stops_pairs.append(line_stops_pairs)

    print("Shortest paths computed.")
    print("-------")

# Export data
print("Exporting processed network data.")
network_df.to_file("data/network.json", driver="GeoJSON")

print("Exporting stop database.")
stops_df.to_file("data/stops.json", driver="GeoJSON")

print("Exporting shortest paths.")
all_line_stops_pairs = pd.concat(all_line_stops_pairs)
line_stops_pairs_labels_to_export = [
    'id_start',
    'id_end',
    'shortest_path',
]
all_line_stops_pairs = all_line_stops_pairs[line_stops_pairs_labels_to_export]
all_line_stops_pairs = gpd.GeoDataFrame(all_line_stops_pairs)
all_line_stops_pairs = all_line_stops_pairs.set_geometry("shortest_path")
all_line_stops_pairs.to_file("data/shortest_paths.json", driver="GeoJSON")

# Plot a few shortest path
# for i in random.sample(range(len(line_stops_pairs)), 6):
#     f, ax = plt.subplots(1, 1, figsize=(6, 6), sharex=True, sharey=True)
#     example = line_stops_pairs.iloc[i]
#     print(example.name_start, example.name_end)
#     sp = gpd.GeoSeries(example.shortest_path)
#     sp.plot(color="red", ax=ax)
#     sp_segments = gpd.GeoSeries(linemerge(example.shortest_path_segments))
#     sp_segments.plot(color="blue", ax=ax)
#     plt.show()
