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

from kafka import KafkaProducer
import json
import sys
import os; import glob
producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(0,10), value_serializer=lambda v: json.dumps(v).encode('utf-8'))

class ReplayCerebralCortexData:
    def __init__(self, start_time=None, end_time=None):
        if not sys.argv[1]:
            raise ValueError("Missing Kafka broker URL/IP and Port.")
        elif not sys.argv[2]:
            raise ValueError("Missing data directory path.")

        self.start_time = start_time
        self.end_time = end_time
        self.kafka_broker = sys.argv[1]
        self.data_dir = sys.argv[2]
        self.producer = KafkaProducer(bootstrap_servers=self.kafka_broker, api_version=(0,10), value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.read_data_dir()

    def publish_filequeue(self, metadata, filename):
        self.producer("filequeue", {"metadata":metadata, "filename":filename})

    def read_data_dir(self):

        self.data_dir = self.data_dir
        files = list(filter(os.path.isfile, glob.glob(self.data_dir + "*.gz")))
        if self.start_time or self.end_time:
            files = list(filter(lambda x: self.filter_filenames(x), files))
        files.sort(key=lambda x: os.path.getmtime(x))

        # dd=max([os.path.join(self.data_dir,d) for d in os.listdir(self.data_dir)], key=os.path.getmtime)
        # print(dd)

    def filter_filenames(self, file_name, start_time, end_time):
        if self.start_time and not self.end_time:
            if os.path.getmtime(file_name)>=start_time:
                return file_name
        elif not self.start_time and self.end_time:
            if os.path.getmtime(file_name)<=end_time:
                return file_name
        elif self.start_time and self.end_time:
            if os.path.getmtime(file_name)>=start_time and os.path.getmtime(file_name)<=end_time:
                return file_name
        else:
                return file_name

ReplayCerebralCortexData(start_time= 1506979290.049632, end_time= 1506979309.049632)