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
