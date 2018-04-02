import pyarrow
import argparse
from cerebralcortex.cerebralcortex import CerebralCortex


def compress_hdfs_data(CC, start, end):
    """

    :param CC: CerebralCortex object
    :param start: starting index of list
    :param end: ending index of list
    """
    all_users = []
    hdfs = pyarrow.hdfs.connect(CC.config["hdfs"]["host"], CC.config["hdfs"]["port"])
    all_users = hdfs.ls(CC.config["hdfs"]["raw_files_dir"])
    users = all_users[start:end]
    print("Total participants to process: ", len(users))
    for user in all_users:
        print("Processing, participant ID: ", user)
        streams = hdfs.ls(user)
        for stream in streams:
            day_files = hdfs.ls(stream)
            for day_file in day_files:
                if day_file[-3:] != ".gz":
                    tmp = day_file.split("/")
                    owner_id = tmp[3]
                    stream_id = tmp[4]
                    day = tmp[5].replace(".pickle", "")
                    CC.get_stream(stream_id, owner_id, day)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='CerebralCortex Data Compress Script')
    parser.add_argument('-conf', '--conf', help='CerebralCortex configuration file', required=True)
    parser.add_argument('-start', '--start', help='Starting index of list. From which index process shal being.', required=True)
    parser.add_argument('-end', '--end', help='Ending index of list. At what index processing shall end.', required=True)

    args = vars(parser.parse_args())

    if args["conf"] is not None and args["conf"] != "":
        CC = CerebralCortex(args["conf"], args["start"], args["end"])
