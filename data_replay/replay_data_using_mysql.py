# Copyright (c) 2017, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import glob
import os
import json
import argparse
import mysql.connector

class ReplayCerebralCortexData:
    def __init__(self, mysql_host, mysql_database, mysql_username, mysql_password, data_dir):
        """
        Constructor
        :param configuration:
        """
        self.data_dir = data_dir
        self.mysql_con = mysql.connector.connect(user=mysql_username, password=mysql_password,
                                     host=mysql_host,
                                     database=mysql_database)
        self.cursor = self.mysql_con.cursor(dictionary=True)
        self.read_data_dir()

    def add_db(self, owner_id, stream_id, full_filename, metadata):

        qry = "INSERT INTO data_replay (id, filename, metadata, processed) VALUES(%s, %s, %s, %s, %s, %s)"
        vals = str(owner_id), str(stream_id), str(stream_name), json.dumps(
            data_descriptor), json.dumps(execution_context), json.dumps(
            annotations), stream_type, start_time, end_time

    def read_data_dir(self):
        data_dirs_dict = []
        days_dirs = [entry.path for entry in os.scandir(self.data_dir) if entry.is_dir()]

        for day_dir in days_dirs:
            for stream_dir in os.scandir(day_dir):
                if stream_dir.is_dir():
                    stream_dir = stream_dir.path
                    tmp = stream_dir.split("/")[-3:]
                    user_id  = tmp[0]
                    day = tmp[1]
                    stream_id = tmp[2]
                    files_list = list(filter(os.path.isfile, glob.glob(stream_dir+ "/*.gz")))
                    data_dirs_dict.append({"user_id": user_id, "day": day, "stream_id": stream_id, "files_list": files_list})


    def produce_kafka_message(self, data_dirs_dict):
        metadata = ""
        for filename in data_dirs_dict:
            base_dir_path = self.data_dir.replace(filename["user_id"],"")
            day = filename["day"]
            if day!=filename["day"]:
                pass
            if filename["files_list"][0]:
                metadata_filename = filename["files_list"][0].replace(".gz", ".json")
                metadata_file = open(metadata_filename, 'r')
                metadata = metadata_file.read()
                metadata_file.close()
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = metadata

            files_list = ','.join(filename["files_list"])
            files_list = files_list.replace(base_dir_path, "")

            self.producer.send("nosql_filequeue", {"metadata": metadata, "filename": files_list})

            print("Yielding file:", filename["files_list"][0])

        self.producer.flush()
        print("Total Messages:", len(data_dirs_dict))


if __name__ == "__main__":
    # python3 replay_data_using_kafka.py 127.0.0.1:9092 data-folder-path start-time(opt) end-time(opt)

    parser = argparse.ArgumentParser(description='Replay all or part of cerebralcortex data.')
    parser.add_argument("-mh", "--mysql_host", help="Kafka Broker IP and port", required=True)
    parser.add_argument("-mp", "--mysql_database", help="Kafka Broker IP and port", required=True)
    parser.add_argument("-mu", "--mysql_username", help="Kafka Broker IP and port", required=True)
    parser.add_argument("-mpass", "--mysql_password", help="Kafka Broker IP and port", required=True)
    parser.add_argument("-d", "--data", help="Data folder path. For example, -d /home/ali/data/", required=True)

    args = vars(parser.parse_args())
    data_dirs = []
    if not args["mysql_host"] or  not args["mysql_port"] or  not args["mysql_username"] or  not args["mysql_password"]:
        raise ValueError("Missing MySQL host, ip, port, and/or password.")
    elif not args["data"]:
        raise ValueError("Missing data directory path.")


    data_path = args["data"]
    if (data_path[-1] != '/'):
        data_path += '/'

    for dir in data_dirs:
        ReplayCerebralCortexData(mysql_host=args["mysql_host"], mysql_database=args["mysql_database"], mysql_username=args["mysql_username"], mysql_password=args["mysql_password"], data_dir=dir)
