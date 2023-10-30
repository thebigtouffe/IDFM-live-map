class Line:
    def __init__(self, id, name, company, transportation_type, segments=[]):
        self.id = id
        self.name = name
        self.company = company
        self.transportation_type = transportation_type
        self.segments : List = segments

    def __repr__(self):
        return f"{self.name}"