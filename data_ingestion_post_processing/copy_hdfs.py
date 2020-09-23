import pyarrow as pa
import pyarrow.parquet as pq
import os

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Data File Tester').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

host = "dagobah10dot.memphis.edu"
port = 8020
hdfs_url = "hdfs://" + str(host) + ":" + str(port)


fs = pa.hdfs.connect(host, port)

def copy_files(file_path):
    local_file_path = "/md2k/data5/tmp_cc3_data"+file_path
    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
    with fs.open(file_path) as f:
        data = pq.read_table(f)
    # with open(local_file_path, "wb+") as lf:
    #     lf.write(data)

stream_names = fs.ls("/cc3_data")
for stream_name in stream_names:
    if os.path.exists("/md2k/data2/t_temp"+stream_name):
        print("ALREADY PROCESSED", stream_name)
    else:
        print("NOT FOUND")
        version_no = fs.ls(hdfs_url + stream_name)
        for vr in version_no:
            user_ids = fs.ls(hdfs_url + vr)
            for user_id in user_ids:
                file_list = fs.ls(hdfs_url + user_id)
                dist_file_list = spark.sparkContext.parallelize(file_list)
                dist_file_list.foreach(lambda msg: copy_files(msg))
                print("PROCESSED:", user_id, len(file_list))

