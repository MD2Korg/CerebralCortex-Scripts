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

import pickle
import yaml
import argparse
from data_replay.db_helper_methods import SqlData
import pyarrow


class fixMySQLTime:
    def __init__(self, config):
        """
        Constructor
        :param configuration:
        """
        self.config = config
        self.sqlData = SqlData(config)
        # Using libhdfs
        self.hdfs = pyarrow.hdfs.connect(self.config["hdfs"]["host"], self.config["hdfs"]["port"])
        self.get_hdfs_files(self.config["hdfs"]["raw_files_dir"])

    def get_hdfs_files(self, dir_path):
        participant_ids = self.hdfs.ls(dir_path)
        for pid in participant_ids:
            stream_ids = self.hdfs.ls(pid)
            for sid in stream_ids:
                days = self.get_days(self.hdfs.ls(sid, detail=True))
                days.sort()
                start_day = min(days)
                end_day = max(days)
                base_path = sid.replace(dir_path, "")
                stream_id = base_path[-36:]
                owner_id = base_path[:36]
                print("Processing (owner-id, stream-id, start-day, end-day) ", owner_id, stream_id, start_day, end_day)
                start_time = self.get_datetime(sid, start_day, days, "start")
                end_time = self.get_datetime(sid, end_day, days, "end")
                self.sqlData.update_start_end_time(stream_id, start_time, end_time)


    def get_days(self, days_files):
        days  = []
        for day in days_files:
            if day["size"]>0:
                try:
                    days.append(int(day["name"][-15:][:8]))
                except:
                    pass
        return days

    def get_datetime(self, filepath, day, days, day_type):
        data = None
        if filepath[-1:]!="/":
            filepath = filepath+"/"
        with self.hdfs.open(filepath+str(day)+".pickle", "rb") as f:
            data = f.read()
            if data is not None and data!=b'':
                data = pickle.loads(data)

        #TODO: sort list
        # data = sorted(data)
        if len(data)>0:
            if day_type=="start":
                return data[0].start_time
            elif day_type=="end":
                return data[len(data)-1].start_time
            else:
                raise ValueError("Day type is unknown.")

if __name__ == "__main__":
    # export CC path before running this (export PYTHONPATH="${PYTHONPATH}:/home/ali/IdeaProjects/CerebralCortex/")
    parser = argparse.ArgumentParser(description='CerebralCortex Data Replay')
    parser.add_argument('-conf','--conf', help='CerebralCortex configuration file', required=True)

    args = vars(parser.parse_args())

    with open(args["conf"]) as ymlfile:
        config = yaml.load(ymlfile)

    fixMySQLTime(config)