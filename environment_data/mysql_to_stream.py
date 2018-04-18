from data_replay.db_helper_methods import SqlData
import os
import json
import yaml
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
import statistics
import uuid
import argparse
from haversine import haversine

class SqlToCCStream():
    def __init__(self, config):

        self.CC = CerebralCortex(config)
        self.config = self.CC.config
        self.sqlData = SqlData(self.config)
        self.process()

    def process(self):
        user_ids = self.filter_user_ids()
        # get all locations lats/longs
        all_locations = self.sqlData.get_latitude_llongitude()
        with open("weather_data.json", "r") as wd:
            metadata = wd.read()
        metadata = json.loads(metadata)
        for uid in user_ids:
            stream_ids = self.CC.get_stream_id(uid, 'LOCATION--org.md2k.phonesensor--PHONE')
            for sid in stream_ids:
                days = self.CC.get_stream_days(sid)
                for day in days:
                    output_stream_id=""
                    # get gps data from stream-name 'LOCATION--org.md2k.phonesensor--PHONE'
                    location_stream = self.CC.get_stream(sid, day)

                    # compute median on lat. and long. vals
                    user_loc = self.compute_lat_long_median(location_stream.data)
                    offset = location_stream.data[0].offset
                    # get weather data for match lat/long values
                    location_id = self.get_location_id(user_loc, all_locations)

                    weather_data = self.sqlData.get_weather_data_by_city_id(location_id, day)

                    # convert data into datastream
                    execution_context = metadata["execution_context"]
                    dp = DataPoint(weather_data["added_date"], None, offset, weather_data)
                    # generate UUID for stream
                    output_stream_id = str(metadata["data_descriptor"])+str(execution_context)+str(metadata["annotations"])
                    output_stream_id += "weather-data-stream"
                    output_stream_id += "weather-data-stream"
                    output_stream_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, output_stream_id))
                    ds = DataStream(output_stream_id, uid, metadata["name"], metadata["data_descriptor"], execution_context, metadata["annotations"], weather_data["added_date"], None, dp)

                    # store data stream
                    self.CC.save_stream(ds)


    def compute_lat_long_median(self, data):
        latitude = []
        longitude = []
        for dp in data:
            latitude.append(dp.sample[0])
            longitude.append(dp.sample[1])
        return statistics.median(latitude),statistics.median(longitude)

    def get_location_id(self, user_loc, all_locations):
        # find distance between user location and weather lat/long
        closest = None
        location_id = None
        for loc in all_locations:
            distance = haversine(user_loc,loc["latitude"],loc["longitude"])
            if closest is None:
                closest = distance
                location_id = loc["id"]
            elif distance<closest:
                closest = distance
                location_id = loc["id"]
        if closest<=15: #if distance is below then 15 miles then select it as weather location
            return location_id
        else:
            return None

    def filter_user_ids(self):

        active_users = []
        all_users = self.sqlData.get_all_users()

        data_dir = self.config["data_replay"]["data_dir"]
        for owner_dir in os.scandir(data_dir):
            if owner_dir in all_users:
                active_users.append(owner_dir)

        return active_users

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='CerebralCortex - Weather data importer')
    parser.add_argument('-conf','--conf', help='CerebralCortex configuration file', required=True)

    args = vars(parser.parse_args())

    with open(args["conf"]) as ymlfile:
        config = yaml.load(ymlfile)


    SqlToCCStream(args["conf"])
