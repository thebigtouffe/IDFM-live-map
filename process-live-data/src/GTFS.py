import datetime
import pytz

class GTFS:

    @staticmethod
    def parse_time(x, tzinfo=pytz.timezone('CET')):
        if type(x) == str:
            # get hours, minutes and seconds
            x = x.split(':')
            if len(x) == 3:
                # convert to timedelta
                delta = datetime.timedelta(hours=int(x[0]),
                                           minutes=int(x[1]),
                                           seconds=int(x[2]))

                return delta + datetime.datetime.combine(datetime.date.today(),
                                                         datetime.datetime.min.time(),
                                                         tzinfo=tzinfo)
            else:
                return None


    @staticmethod
    def parse_date(x):
        if type(x) == str or type(x) == int:
            x = str(x)
            if len(x) == 8:
                year = int(x[0:4])
                month = int(x[4:6])
                day = int(x[6:8])
                return datetime.date(year=year, month=month, day=day)
            else:
                return None