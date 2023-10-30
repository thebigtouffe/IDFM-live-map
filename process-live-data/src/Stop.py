from src.Line import Line

class Stop:
    def __init__(self, id, company, name, lon, lat, city, line):
        self.id = id
        self.company = company
        self.name = name
        self.lon = lon
        self.lat = lat
        self.city = city
        self.line : Line = line

    def __repr__(self):
        return f"{self.name} ({self.line.name})"

    def get_short_id(self):
        return self.id.split(":")[-1]