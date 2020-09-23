# remove index column from processed parquet files

from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("Remove Column").getOrCreate()

data_path="/md2k/data2/cc3_data/"

output_folder = "/md2k/data2/cc3_data2/"

for strm in os.scandir(data_path):
    print("*"*10, "PROCESSING ", "*"*10, "\n", strm.name)
    try:
        df = spark.read.load(data_path+"/"+strm.name)
        df2 = df.drop("__index_level_0__")
        df2.repartition(1).write.parquet(output_folder+"/"+strm.name)
    except Exception as e:
        print(" ERROR --- ", strm.name)
