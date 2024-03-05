import numpy as np
from shapely.geometry import LineString
from shapely.ops import unary_union
from pyproj import Geod

class Utils:

    @staticmethod
    def compute_short_id(x):
        return x.rstrip(":").split(":")[-1]

    @staticmethod
    def get_linestring_length_in_meters(line):
        # Distance in meter
        geod = Geod(ellps="WGS84")
        return geod.geometry_length(line)

    @staticmethod
    def interpolate_linestring(line, distance_between_points=None, n=None):
        if distance_between_points and distance_between_points > 0:
            n = round(Utils.get_linestring_length_in_meters(line) / distance_between_points)
        elif n and n > 1:
            n = n
        else:
            n = 10
        
        distances = np.linspace(0, line.length, n)
        points = [line.interpolate(distance) for distance in distances]
        interpolated_line = LineString(points)
        return interpolated_line
