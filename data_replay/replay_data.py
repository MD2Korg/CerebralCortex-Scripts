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

from kafka import KafkaProducer


class ReplayCerebralCortexData:
    def __init__(self, kafka_broker, data_dir, start_time="", end_time=""):
        """
        Constructor
        :param configuration:
        """
        self.start_time = start_time
        self.end_time = end_time
        self.kafka_broker = kafka_broker
        self.data_dir = data_dir
        self.read_data_dir()

    def publish_filequeue(self, metadata, filename):
        self.producer("filequeue", {"metadata": metadata, "filename": filename})

    def read_data_dir(self):

        self.data_dir = self.data_dir
        filenames = list(filter(os.path.isfile, glob.glob(self.data_dir + "*.gz")))
        if self.start_time or self.end_time:
            filenames = list(filter(lambda x: self.filter_filenames(x), filenames))
        filenames.sort(key=lambda x: os.path.getmtime(x))

        self.produce_kafka_message(filenames)

    def filter_filenames(self, file_name):
        if self.start_time and not self.end_time:
            if os.path.getmtime(file_name) >= self.start_time:
                return file_name
        elif not self.start_time and self.end_time:
            if os.path.getmtime(file_name) <= self.end_time:
                return file_name
        elif self.start_time and self.end_time:
            if os.path.getmtime(file_name) >= self.start_time and os.path.getmtime(file_name) <= self.end_time:
                return file_name
        else:
            return file_name

    def produce_kafka_message(self, filenames):
        producer = KafkaProducer(bootstrap_servers=self.kafka_broker, api_version=(0, 10),
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        for filename in filenames:
            metadata_filename = filename.replace(".gz", ".json")
            data_filename = filename.replace(self.data_dir, "")

            metadata_file = open(metadata_filename, 'r')
            metadata = metadata_file.read()
            try:
                metadata = json.loads(metadata)
            except:
                metadata = metadata
            producer.send("filequeue", {"metadata": metadata, "filename": data_filename})
            producer.flush()
            metadata_file.close()
            print("Yielding file:", metadata_filename, data_filename)


if __name__ == "__main__":
    # python3 replay_data.py 127.0.0.1:9092 data-folder-path start-time(opt) end-time(opt)

    parser = argparse.ArgumentParser(description='Replay all or part of cerebralcortex data.')
    parser.add_argument("-b", "--broker", help="Kafka Broker IP and port", required=True)
    parser.add_argument("-d", "--data", help="Data folder path. For example, /home/ali/data/", required=True)
    parser.add_argument("-st", "--st", help="Start time (timestamp in milliseconds)", required=False)
    parser.add_argument("-et", "--et", help="End time (timestamp in milliseconds)", required=False)
    args = vars(parser.parse_args())

    if not args["broker"]:
        raise ValueError("Missing Kafka broker URL/IP and Port.")
    elif not args["data"]:
        raise ValueError("Missing data directory path.")

    try:
        if args["st"]:
            start_time = args["st"]
        elif args["et"]:
            end_time = args["et"]
        else:
            start_time = ""
            end_time = ""
    except:
        start_time = ""
        end_time = ""

    ReplayCerebralCortexData(kafka_broker=args["broker"], data_dir=args["data"], start_time=start_time, end_time=end_time)
