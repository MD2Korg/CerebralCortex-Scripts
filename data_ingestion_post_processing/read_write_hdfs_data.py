import pyarrow as pa


from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.driver.memory','24g').config('spark.driver.maxResultSize','10g').appName('Data File Tester').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

host = "dantooine10dot.memphis.edu"
port = 8020
hdfs_url = "hdfs://" + str(host) + ":" + str(port)
filesystem_url = "/md2k/data2/hdfs_data/"

fs = pa.hdfs.connect(host, port)

stream_names = fs.ls("/cc3_data")

def write_to_filesystem(df, stream_name):
    df.repartition(1).write.partitionBy(["version","user"]).format('parquet').mode("append").save(filesystem_url+stream_name.replace("/cc3_data/",""))

def read_from_hdfs(path):
    return spark.read.load(path)

for stream_name in stream_names:
    print("PROCESSING:", stream_name)
    df = read_from_hdfs(hdfs_url + stream_name)
    write_to_filesystem(df,stream_name)
