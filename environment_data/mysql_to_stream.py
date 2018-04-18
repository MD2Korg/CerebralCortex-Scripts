from data_replay.db_helper_methods import SqlData
import os
from cerebralcortex.cerebralcortex import CerebralCortex
import statistics

class SqlToCCStream():
    def __init__(self, config):
        self.config = config
        self.sqlData = SqlData(config)
        self.CC = CerebralCortex(config)

    def process(self):
        user_ids = self.filter_user_ids()
        # get all locations lats/longs
        all_locations = self.sqlData.get_latitude_llongitude()
        for uid in user_ids:
            stream_ids = self.CC.get_stream_id(uid, 'LOCATION--org.md2k.phonesensor--PHONE')
            for sid in stream_ids:
                days = self.CC.get_stream_days(sid)
                for day in days:
                    # get gps data from stream-name 'LOCATION--org.md2k.phonesensor--PHONE'
                    location_stream = self.CC.get_stream(sid, day)

                    # compute median on lat. and long. vals
                    user_loc = self.compute_lat_long_median(location_stream.data)

                    # get weather data for match lat/long values
                    location_id = self.get_location(user_loc, all_locations)

                    weather_data = self.sqlData.get_weather_data_by_city_id(location_id, day)

                    # convert data into datastream

                    # store data stream






        # find distance between user location and weather lat/long

    def compute_lat_long_median(self, data):
        latitude = []
        longitude = []
        for dp in data:
            latitude.append(dp.sample[0])
            longitude.append(dp.sample[1])
        return [statistics.median(latitude),statistics.median(longitude)]

    def get_location(self, user_loc, all_locations):
        closest = None
        for loc in all_locations:
            pass

    def filter_user_ids(self):

        active_users = []
        all_users = self.sqlData.get_all_users()

        data_dir = self.config["data_replay"]["data_dir"]
        for owner_dir in os.scandir(data_dir):
            if owner_dir in all_users:
                active_users.append(owner_dir)

        return active_users