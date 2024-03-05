from datetime import datetime
import pytz

class ArrivalTime:
    def __init__(self, time):
        self.time : datetime = time
        self.unix_timestamp : float = (time - datetime(1970, 1, 1)).total_seconds()
    
    @staticmethod
    def parse_date_from_string(s):
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")

    @staticmethod
    def parse_time_from_string(s):
        return datetime.strptime(s, "H:%M:%S")

    def __repr__(self):
        return str(self.time)