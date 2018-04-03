import pyarrow
import argparse
import pickle
import gzip
import traceback
from cerebralcortex.core.datatypes.datapoint import DataPoint
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.cerebralcortex import CerebralCortex


def compress_hdfs_data(start, end):
    """

    :param CC: CerebralCortex object
    :param start: starting index of list
    :param end: ending index of list
    """
    all_users = []
    hdfs = pyarrow.hdfs.connect(CC.config["hdfs"]["host"], CC.config["hdfs"]["port"])
    all_users = hdfs.ls(CC.config["hdfs"]["raw_files_dir"])
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
                    tmp = day_file.split("/")
                    # owner_id = tmp[3]
                    # stream_id = tmp[4]
                    # day = tmp[5].replace(".pickle", "")
                    print(day_file)
                    process_file(hdfs, day_file)
                    exit(1)

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

    # if args["conf"] is not None and args["conf"] != "":
    #     CC = CerebralCortex(args["conf"])
    compress_hdfs_data(args["start"], args["end"])
