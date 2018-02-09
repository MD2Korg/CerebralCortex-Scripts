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


class LabelCorruptData:
    def __init__(self, data_dir):
        """
        Constructor
        :param configuration:
        """
        self.data_dir = data_dir
        self.read_data_dir()

    def read_data_dir(self):
        data_dirs_dict = []

        for day_dir in os.scandir(self.data_dir):
            if day_dir.is_dir():
                for stream_dir in os.scandir(day_dir):
                    if stream_dir.is_dir():
                        files_list = list(filter(os.path.isfile, glob.glob(stream_dir+ "/*.gz")))
                        for filepath in files_list:
                            self.process_file(filepath)
    def process_file(self,filepath):
        participant_id
    def produce_kafka_message(self, data_dirs_dict):
        is_metadata_read = 0
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
                is_metadata_read = 1

            files_list = ','.join(filename["files_list"])
            files_list = files_list.replace(base_dir_path, "")


            self.producer.send("filequeue", {"metadata": metadata, "filename": files_list})

            print("Yielding file:", filename["files_list"][0])

        self.producer.flush()
        print("Total Messages:", len(data_dirs_dict))


if __name__ == "__main__":
    # python3 label_corrupt_data.py data-folder-path

    parser = argparse.ArgumentParser(description='Replay all or part of cerebralcortex data.')
    parser.add_argument("-d", "--data", help="Data folder path. For example, -d /home/ali/data/", required=True)
    parser.add_argument("-uid", "--user_id", help="User/Participant ID, For example, -uid UUID1,UUID2,UUID3", required=False)
    args = vars(parser.parse_args())
    data_dirs = []
    if not args["data"]:
        raise ValueError("Missing data directory path.")

    data_path = args["data"]
    if (data_path[-1] != '/'):
        data_path += '/'

    if args["user_id"]:
        participant_ids = args["user_id"].split(",")
        for participant_id in participant_ids:
            data_dirs.append(data_path+participant_id.strip())
    else:
        data_dirs = [entry.path for entry in os.scandir(data_path) if entry.is_dir()]

    for dir in data_dirs:
        LabelCorruptData(data_dir=dir)
