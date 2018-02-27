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

import os
import json
import yaml
from data_replay.db_helper_methods import SqlData
from kafka import KafkaProducer


class ReplayCerebralCortexData:
    def __init__(self, config):
        """
        Constructor
        :param configuration:
        """
        self.sqlData = SqlData()
        self.config = config
        self.blacklist_regex = self.config["blacklist"]
        self.replay_type = self.config["data_replay"]["replay_type"]
        self.kafka_broker = self.config["kafkaserver"]["host"]
        self.data_dir = self.config["data_replay"]["data_dir"]
        if (self.data_dir[-1] != '/'):
            self.data_dir += '/'

        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker, api_version=(0, 10),
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        participant_ids = []
        if str(config["users"]["uuids"]).strip()!="":
            participant_ids = str(config["users"]["uuids"]).split(",")

        if self.replay_type=="filez":
            self.read_data_dir(participant_ids)
        elif self.replay_type=="mydb":
            self.db_data(participant_ids)
        else:
            raise ValueError("Replay type can only be filez or mydb")

    def db_data(self, participant_ids):
        results = self.sqlData.get_data(participant_ids, self.blacklist_regex)
        if len(results)>0:
            for row in results:
                files_list = []
                for f in json.loads(row["files_list"]):
                    files_list.append(self.data_dir+f)
                self.produce_kafka_message({"user_id": row["owner_id"], "day": row["day"], "stream_id": row["stream_id"], "files_list": files_list})
            self.producer.flush()
        else:
            print("No record. You may need to run store_dirs_to_db.py if you want to use mydb data replay type.")

    def read_data_dir(self, participant_ids):
        data_dir = []
        if len(participant_ids)>0:
            for participant_id in participant_ids:
                data_dir.append(self.data_dir+participant_id.strip())
        else:
            data_dir = [entry.path for entry in os.scandir(self.data_dir) if entry.is_dir()]
        for participant in data_dir:
            for day_dir in os.scandir(participant):
                if day_dir.is_dir():
                    for stream_dir in os.scandir(day_dir):
                        if stream_dir.is_dir():
                            stream_dir = stream_dir.path
                            tmp = stream_dir.split("/")[-3:]
                            user_id  = tmp[0]
                            day = tmp[1]
                            stream_id = tmp[2]
                            files_list = []
                            for f in os.listdir(stream_dir):
                                if f.endswith(".gz"):
                                    files_list.append(stream_dir+"/"+f)
                            self.produce_kafka_message({"user_id": user_id, "day": day, "stream_id": stream_id, "files_list": files_list})

    def produce_kafka_message(self, filename):
        metadata = ""

        base_dir_path = self.data_dir.replace(filename["user_id"],"")
        day = filename["day"]

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

        self.producer.send("hdfs_filequeue", {"metadata": metadata, "day":day, "filename": files_list})

        print("Yielding file:", filename["files_list"][0])

if __name__ == "__main__":
    with open("config.yml") as ymlfile:
        config = yaml.load(ymlfile)

    ReplayCerebralCortexData(config=config)
