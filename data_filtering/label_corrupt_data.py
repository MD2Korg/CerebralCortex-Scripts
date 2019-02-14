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
import gzip
import json
import argparse

# TODO: this labeling corrupt data labeling might not work due to unknown number of columns
# TODO: use metadata (if it's corrected) to apply corrupt data filtering

class LabelCorruptData:
    def __init__(self, data_dir):
        """
        Constructor
        :param configuration:
        """
        self.data_dir = data_dir
        self.read_data_dir()

    def read_data_dir(self):
        file_samples = []
        corrupt_data = []
        # dir structure "user-uuid/day/stream-uuid/filename.extension"
        for day_dir in os.scandir(self.data_dir): # scan user-uuid directory, contains day folders
            if day_dir.is_dir():
                print("Processing: ", day_dir.path)
                for stream_dir in os.scandir(day_dir): #scan day directory, contains stream uuid folders
                    if stream_dir.is_dir():
                        files_list = list(filter(os.path.isfile, glob.glob(stream_dir.path+ "/*.gz")))
                        for filepath in files_list:
                            if "bed5d03b-5f1f-31fa-b7b6-0c9f2bf7ab4a" not in filepath:
                                file_samples.append({"filename": filepath, "sample": self.process_file(filepath)})
                        # check what directories have different columns
                        bad_entries = self.check(file_samples)
                        if len(bad_entries)>0:
                            corrupt_data.append({"stream_dir":stream_dir, "bad_files":self.check(file_samples)})
                        file_samples = []
        return corrupt_data

    def process_file(self,filepath):
        with gzip.open(filepath) as gzfile:
            try:
                return str(gzfile.readline(),"utf-8")
            except:
                return None

    def check(self, file_samples: dict):
        tmp = None
        tmp2 = ""
        bad_rows = []
        for data in file_samples:
            if "20171211" in data["filename"] and "4f2d6378-43fd-3c51-b418-d71beb72daa0" in data["filename"]:
                print(data)
            if data["sample"] is not None:
                sample_size = self.convert_sample(data["sample"])
                if tmp is None:
                    tmp2 = data
                    tmp = sample_size
                elif tmp != sample_size:
                    bad_rows.append(data)
                else:
                    tmp = sample_size
        return bad_rows

    def convert_sample(self, sample):
        try:
            if sample.startswith("[") or sample.startswith("{"):
                return 1
            else:
                tmp = sample.split(",")[2]
                try:
                    float(tmp)
                    return len(list([float(x.strip()) for x in sample.split(',')]))
                except:
                    return 1

        except:
            return 1


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

    for dir in data_dirs: # scan all users uuid folders
        LabelCorruptData(data_dir=dir)
