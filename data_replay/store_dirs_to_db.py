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
import argparse
import gzip
from datetime import datetime
from db_helper_methods import SqlData

class ReplayCerebralCortexData:
    def __init__(self, users, config, scan_type):
        """
        Constructor
        :param configuration:
        """
        self.config = config
        self.data_dir = self.config["data_replay"]["data_dir"]
        if (self.data_dir[-1] != '/'):
            self.data_dir += '/'

        self.sqlData = SqlData(config)
        self.users = users
        self.lab_participants = []
        # group/lab names:
        # mperf-alabsi
        # mperf-buder
        # sobclab
        # md2k_labtest
        with open("lab_user_ids.txt", "r") as lf:
            data = lf.read()
        data = data.split("\n")
        for id in data:
            self.lab_participants.append(id)

        if scan_type=="mperf":
            self.read_data_dir()
        elif scan_type=="demo":
            self.read_demo_dir()
        else:
            raise ValueError("Only acceptable parameters for type are mperf OR demo.")

    def read_demo_dir(self):
        for stream_dir in os.scandir(self.data_dir):
            file_ext = stream_dir.path[-3:]
            filename = stream_dir.path
            print("Processing:", filename)
            if file_ext==".gz":
                metadata = self.read_json_file(filename.replace(".gz", ".json"))
                day = self.read_gz_file(filename)
                owner_id = metadata["owner"]
                stream_id = metadata["identifier"]
                stream_name = metadata["name"]
                files_list = [filename.replace(self.data_dir, "")]
                self.sqlData.add_to_db(owner_id, stream_id, stream_name, day, files_list, 0, metadata)


    def read_data_dir(self):
        for stream_dir in os.scandir(self.data_dir):
            if stream_dir.is_dir():
                metadata = self.read_json_file()
                owner = stream_dir.path[-36:]
                if self.users=="all":
                    self.scan_stream_dir(stream_dir)
                elif owner in self.lab_participants:
                        self.scan_stream_dir(stream_dir)

    def scan_stream_dir(self, stream_dir1):
        for day_dir in os.scandir(stream_dir1.path):
            if day_dir.is_dir():
                for stream_dir in os.scandir(day_dir):
                    if stream_dir.is_dir():
                        stream_dir = stream_dir.path
                        tmp = stream_dir.split("/")[-3:]
                        owner_id  = tmp[0]
                        day = tmp[1]
                        stream_id = tmp[2]
                        files_list = []
                        dir_size = 0
                        for f in os.listdir(stream_dir):
                            if f.endswith(".gz"):
                                new_filename = (stream_dir+"/"+f).replace(self.data_dir,"")
                                files_list.append(new_filename)
                                dir_size += os.path.getsize(stream_dir+"/"+f)
                        metadata_filename = self.data_dir+files_list[0].replace(".gz", ".json")
                        with open(metadata_filename, 'r') as mtd:
                            metadata = mtd.read()
                        metadata = json.loads(metadata)
                        stream_name = metadata["name"]
                        self.sqlData.add_to_db(owner_id, stream_id, stream_name, day, files_list, dir_size, metadata)

    def read_json_file(self, filename):
        with open(filename, "r") as jfile:
            metadata = jfile.read()
        return json.loads(metadata)

    def read_gz_file(self, filename):
        with gzip.open(filename, "r") as gzfile:
            for line in gzfile:
                day = line.decode("utf-8").split(",")[0]
                day = datetime.fromtimestamp(int(day)/1000)
                day = day.strftime('%Y%m%d')
                break
        return day

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='CerebralCortex Data Replay')
    parser.add_argument('-users','--users', help='Scan all users directories or only for the list provided in the script.', type=str, default="all", required=False)
    parser.add_argument('-conf','--conf', help='CerebralCortex configuration file', required=True)
    parser.add_argument('-type','--type', help='Type of directory scan. Demo directory contains all raw/json files in one folder. mperf/demo', default="mperf", required=False)

    args = vars(parser.parse_args())

    with open(args["conf"]) as ymlfile:
        config = yaml.load(ymlfile)

    scan_type = args["type"]
    if scan_type!="mperf" and scan_type!="demo":
        raise ValueError("Only acceptable parameters for type are mperf OR demo.")

    ReplayCerebralCortexData(args["users"],config, scan_type)
