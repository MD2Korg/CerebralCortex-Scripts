# this script will (only hdfs)
#   list directories
#   for each directory, read stream using spark
#   write read stream to a new location

import sys
import os
from pathlib import Path
import pyspark
from pyspark.sql import SparkSession
import shutil
import re
import pyarrow as pa

spark = SparkSession.builder.config('spark.driver.memory','16g').config('spark.driver.maxResultSize','10g').config('spark.local.dir','/md2k/data1/tmp').appName('Data Compressor').getOrCreate()
spark.sparkContext.setLogLevel('WARN')


hdfs_url = "hdfs://dantooine10dot.memphis.edu:8020"

fs = pa.hdfs.connect("dantooine10dot.memphis.edu", 8020)
stream_names_data2=[]
stream_names_data2 = fs.ls("/cc3_compressed/data2/")
stream_names_data3 = fs.ls("/cc3_compressed/data3/")
stream_names_data6 = fs.ls("/cc3_compressed/data6/")
stream_names_data7 = fs.ls("/cc3_compressed/data7/")
stream_names_data_cu_audio_feature = fs.ls("/cc3_compressed/cu_audio_feature/")

dir_names = stream_names_data2 + \
            stream_names_data3 + \
            stream_names_data6 + \
            stream_names_data7 + \
            stream_names_data_cu_audio_feature

stream_names = [re.sub('^\/cc3_compressed\/data.*\/', '', dir_name).strip() for dir_name in dir_names]

for stream_name in stream_names:
    df = spark.read.load(hdfs_url+"/cc3_compressed/*/"+stream_name)
    df.show(1)
