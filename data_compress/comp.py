# Copyright (c) 2018, MD2K Center of Excellence
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

import pyarrow
import argparse
import pickle
import gzip
import traceback
import yaml


def compress_hdfs_data(config, start, end):
    """

    :param CC: CerebralCortex object
    :param start: starting index of list
    :param end: ending index of list
    """
    all_users = []
    hdfs = pyarrow.hdfs.connect(config["hdfs"]["host"], config["hdfs"]["port"])
    all_users = hdfs.ls(config["hdfs"]["raw_files_dir"])
    start = int(start)
    end = int(end)
    users = all_users[start:end]
    print("Total participants to process: ", len(users))
    for user in users:
        print("Processing, participant ID: ", user)
        streams = hdfs.ls(user)
        for stream in streams:
            day_files = hdfs.ls(stream)
            for day_file in day_files:
                if day_file[-3:] != ".gz":
                    print(day_file)
                    process_file(hdfs, day_file)

def process_file(hdfs, filename):
    data = None
    try:
        if hdfs.exists(filename):
            with hdfs.open(filename, "rb") as curfile:
                data = curfile.read()
        else:
            print(filename.replace("pickle",""), "does not exist.")

        if data is not None and data!=b'':
            data = pickle.loads(data)
            clean_data = filter_sort_datapoints(data)
            compress_store_pickle(filename, clean_data,hdfs)
    except Exception as e:
        print("Error processing:", filename, "Error: ", str(traceback.format_exc()))

def filter_sort_datapoints(data):
    if len(data)>0:
        clean_data = dedup(sorted(data))
        return clean_data
    else:
        return data

def dedup(data):
    result = [data[0]]
    for dp in data[1:]:
        if dp.start_time == result[-1].start_time:
            continue
        result.append(dp)

    return result
def compress_store_pickle(filename: str, data: pickle, hdfs: object=None):
    """

    :param filename: pickle file name
    :param data: pickled data
    :param hdfs: hdfs connection object
    """
    gz_filename = filename.replace(".pickle", ".gz")
    if len(data)>0:
        data = pickle.dumps(data)
        compressed_data = gzip.compress(data)

        try:
            if not hdfs.exists(gz_filename):
                with hdfs.open(gz_filename, "wb") as gzwrite:
                    gzwrite.write(compressed_data)
            if hdfs.exists(filename):
                if hdfs.info(gz_filename)["size"]>0:
                    hdfs.delete(filename)
        except:
            print("Error in generating gz file.")
            # delete file if file was opened and no data was written to it
            if hdfs.info(gz_filename)["size"]==0:
                hdfs.delete(gz_filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CerebralCortex Data Compress Script')
    parser.add_argument('-conf', '--conf', help='CerebralCortex configuration file', required=True)
    parser.add_argument('-start', '--start', help='Starting index of list. From which index process shal being.', required=True)
    parser.add_argument('-end', '--end', help='Ending index of list. At what index processing shall end.', required=True)

    args = vars(parser.parse_args())

    with open(args["conf"]) as ymlfile:
        config = yaml.load(ymlfile)
    compress_hdfs_data(config, args["start"], args["end"])
