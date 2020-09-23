# Join ingestion_log MySQL table and HDFS directory structure
# Both data are in parquet format

import pandas as pd
import numpy as np
desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)

# with open("/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/mismatched_metadata.pkl", "rb") as f:
#     data = f.read()
#     data = pickle.loads(data)
# df = pd.DataFrame(data)
# df.to_parquet('/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/mismatched_metadata.parquet')
# print("read")

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ColumnMissMatch").getOrCreate()

df = spark.read.load("/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/hdfs_user_dir_names/")

def split_colz(pdf):
    user_folder = pdf["user_folder"].iloc[0]
    try:
        stream_name = user_folder.split("stream=")[1].split("/version=")[0]
        user_id = user_folder.split("stream=")[1].split("/version=")[1].split("/user=")[1]
        pdf2 = pd.DataFrame([[user_id,stream_name, user_folder]], columns=["user_id","stream_name", "user_folder"])
        #print(user_id,stream_name)
        return pdf2
    except Exception as e:
        print(user_folder, str(e))
        return  pd.DataFrame()

df2 = df.groupBy("user_folder").applyInPandas(split_colz,schema="user_id string, stream_name string, user_folder string")

df2.repartition(1).write.parquet("/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/hdfs_user_stream")