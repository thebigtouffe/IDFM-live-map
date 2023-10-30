from datetime import datetime

class ArrivalTime:
    def __init__(self, time):
        self.time : datetime = time
        self.time_seconds : float = (time - datetime(1970, 1, 1)).total_seconds()
    
    @staticmethod
    def parse_date_from_string(s):
        return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")

    def __repr__(self):
        return str(self.time)