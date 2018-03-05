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
from data_replay.db_helper_methods import SqlData

class ReplayCerebralCortexData:
    def __init__(self, config):
        """
        Constructor
        :param configuration:
        """
        self.config = config
        self.data_dir = self.config["data_replay"]["data_dir"]
        if (self.data_dir[-1] != '/'):
            self.data_dir += '/'

        self.sqlData = SqlData()
        self.read_data_dir()

    def read_data_dir(self):
        for stream_dir in os.scandir(self.data_dir):
            if stream_dir.is_dir():
                for day_dir in os.scandir(stream_dir.path):
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

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='CerebralCortex Data Replay')
    parser.add_argument('-conf','--conf', help='CerebralCortex configuration file', required=True)

    args = vars(parser.parse_args())

    with open(args["conf"]) as ymlfile:
        config = yaml.load(ymlfile)


    ReplayCerebralCortexData(config)
