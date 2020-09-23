import sys
import os
from pathlib import Path
import pyspark
from pyspark.sql import SparkSession
import shutil

spark = SparkSession.builder.config('spark.driver.memory','16g').config('spark.driver.maxResultSize','10g').config('spark.local.dir','/md2k/data1/tmp').appName('Data Compressor').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

base_path = sys.argv[1]

hdfs_url = "hdfs://dantooine10dot.memphis.edu:8020/cc3_compressed/some_temp/"

for stream in os.scandir(base_path):
    if 'stream=' in stream.name and stream.is_dir():
        for version in os.scandir(stream):
            if version.is_dir():
                for user in os.scandir(version):
                    if 'SUCCESS' not in user.name:
                        try:
                            print('PROCESSING:',stream.name, version.name, user.name)

                            df = spark.read.load(user.path)

                            stream_path = hdfs_url + stream.name + "/version=1/" + user.name
                            df.repartition(1).write.format('parquet').mode('append').save(stream_path)
                            #df.repartition(1).write.mode('append').parquet(str(p), compression='snappy')

                        #shutil.rmtree(tmp_path)

                        except pyspark.sql.utils.AnalysisException as e:
                            print('-'*80)
                            print('ERROR in user',user.path, str(e))
                            print('-'*80)
                            pass