import os
import json
import geopandas as gpd
import pandas as pd
import networkx as nx
from shapely import LineString, Point
from shapely.ops import nearest_points, linemerge
import matplotlib.pyplot as plt
import momepy

from src.PRIM_API import PRIM_API

# Load settings from settings.json
with open('settings.json', 'r') as json_file:
    settings_data = json.load(json_file)

prim = PRIM_API(api_key=settings_data["prim_api_key"])

# Download railroad network and stop database
if not os.path.exists(prim.NETWORK_DATA_FILE_PATH):
    prim.download_network()
if not os.path.exists(prim.STOPS_DATA_FILE_PATH):
    prim.download_stops()

# Load railroad network and get relevant fields
with open('data/network.json', 'r') as data:
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

# Load stop database and get relevant fields
with open('data/stops.json', 'r') as data:
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

# Iterate over each line
line_names = list(set(network_df.name.values))
# for name in line_names:
for name in ['RER D']:
    print(name)
    line = network_df[network_df.name == name].copy()
    line_stops=stops_df[stops_df.line_id == line.id.iloc[0]].copy()

    # Compute network graph from geospatial data
    G = momepy.gdf_to_nx(line, approach="primal")
    nodes = list(G.nodes)

    # Get nodes as geodataframe
    nodes_gdf = momepy.nx_to_gdf(G, points=True, lines=False, spatial_weights=True)[0]
    nodes_position = nodes_gdf.geometry.unary_union

    def nearest_node(point, nodes_position=nodes_position, nodes_gdf=nodes_gdf):
        # find the nearest point and return the corresponding Node in graph
        nearest = nodes_gdf.geometry == nearest_points(point, nodes_position)[1]
        return nodes_gdf[nearest].nodeID.values[0]

    # Compute nearest node on graph for each stop
    line_stops['nearest_node_on_graph'] = line_stops.apply(lambda row: nearest_node(row.geometry), axis=1)

    # Compute shortest paths on line graph
    shortest_paths = nx.shortest_path(G)

    # Plot
    # f, ax = plt.subplots(1, 1, figsize=(6, 6), sharex=True, sharey=True)
    # line.plot(color="#"+line.color.iloc[0], ax=ax)
    # line_stops.plot(color="blue", ax=ax)
    # ax.set_title(line.name.iloc[0])
    # nx.draw(G, {n: [n[0], n[1]] for n in nodes}, ax=ax, node_size=3)
    # plt.show()

    # Compute pairs of stops by using the cartesian product of the dataframe with itself
    line_stops_pairs = line_stops.assign(dummy=1).merge(line_stops.assign(dummy=1), on='dummy', how='outer', suffixes=('_start', '_end'))
    line_stops_pairs = line_stops_pairs.drop('dummy', axis=1)
    line_stops_pairs = line_stops_pairs[line_stops_pairs.nearest_node_on_graph_start != line_stops_pairs.nearest_node_on_graph_end]

    # Compute shortest path for each pair
    line_stops_pairs['shortest_path'] = line_stops_pairs.apply(lambda row: shortest_paths[nodes[row.nearest_node_on_graph_start]][nodes[row.nearest_node_on_graph_end]], axis=1)
    # Get the segments of the shortest path for further comparison with network dataframe
    line_stops_pairs['shortest_path_segments'] = line_stops_pairs.apply(lambda row: [LineString(x) for x in list(zip(row.shortest_path[0:-1], row.shortest_path[1:]))], axis=1)

    # Compute line network with reverse segment
    reverse_line = line.copy()
    reverse_line.geometry = reverse_line.reverse()
    line = pd.concat([line, reverse_line])
    
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

# Plot a few shortest path
for i in range(6):
    f, ax = plt.subplots(1, 1, figsize=(6, 6), sharex=True, sharey=True)
    example = line_stops_pairs.iloc[i]
    print(example.name_start, example.name_end)
    sp = gpd.GeoSeries(example.shortest_path)
    sp.plot(color="red", ax=ax)
    sp_segments = gpd.GeoSeries(linemerge(example.shortest_path_segments))
    sp_segments.plot(color="blue", ax=ax)
    plt.show()

# Build a route between two stops
print(line_stops.name.iloc[-1])
start_idx = line_stops.nearest_node_on_graph.iloc[-1]
print(line_stops.name.iloc[-2])
end_idx = line_stops.nearest_node_on_graph.iloc[-2]
sp = shortest_paths[nodes[start_idx]][nodes[end_idx]]


# if __name__ == '__main__':
#     prim = PRIM_API(api_key=settings_data["prim_api_key"])

#     if not os.path.exists(prim.NETWORK_DATA_FILE_PATH):
#         prim.download_network()
#     if not os.path.exists(prim.STOPS_DATA_FILE_PATH):
#         prim.download_stops()

#     prim.load_network()
#     prim.load_stops()

#     # # Test SNCF
#     # stop=prim.stops['IDFM:monomodalStopPlace:46725']
#     # d1 = prim.get_arrival_times_by_stop(stop)

#     # # Test metro
#     # prim.get_arrival_times_by_stop(prim.stops['IDFM:22227'])
#     # prim.get_arrival_times_by_stop(prim.stops['IDFM:462975'])
#     # prim.get_arrival_times_by_stop(prim.stops['IDFM:478395'])

#     # Metro 3

#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463071'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22031'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463090'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463297'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22037'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463068'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22036'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463119'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463258'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462971'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22020'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:21950'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463174'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462991'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462946'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463122'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22029'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463246'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463189'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:21963'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463070'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463228'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:21946'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:21947'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463262'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22021'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22027'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:21906'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22030'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463276'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22019'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22018'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463210'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22028'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463069'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463316'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:21902'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463250'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22022'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463286'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22033'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22032'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462987'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22034'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:21945'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:21948'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462990'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462997'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22035'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22024'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462986'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463091'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:21994'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22025'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22026'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462989'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22023'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462985'])

#     # Metro 1 (multiline)
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22100'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463019'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22083'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22076'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22080'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463012'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463307'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463227'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463170'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463044'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463257'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22082'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22090'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22087'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22103'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22105'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:462943'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463181'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463013'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22078'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463193'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22101'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463121'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22084'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22079'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22074'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463130'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463040'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463294'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463197'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22075'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463150'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463185'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463217'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22099'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22102'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22085'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22081'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463080'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22089'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22091'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463218'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463010'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22086'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463041'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22077'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22104'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463149'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:463160'])
#     prim.get_arrival_times_by_stop(prim.stops['IDFM:22088'])

#     for k, v in prim.trips.items():
#         print(v.id, v.stops)


#     # prim.get_arrival_times()