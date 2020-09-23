import pandas as pd
import numpy as np
import pickle

desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)

# with open("/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/mismatched_metadata_all.pkl", "rb") as f:
#     data = f.read()
#     data = pickle.loads(data)
# df = pd.DataFrame(data)
# df.to_parquet('/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/mismatched_metadata_all.parquet')
# print("read")

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ColumnMissMatch").getOrCreate()

df = spark.read.load("/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/mismatched_metadata_all.parquet")

def add_streamname_column(pdf):
    for index, row in pdf.iterrows():
        lst = row["user_folder"].split("/")
        stream_name = "/"+lst[1]+"/"+lst[2]+"/"+lst[3]
        # if 'stream=accelerometer--org.md2k.motionsense--motion_sense_hrv--right_wrist' in row["user_folder"] and 'ef93c573-b6d1-45e4-b676-cb8517e15974' in row["user_folder"]:
        #     print(row)
        #     print(row["schema"])
        #     exit()
        pdf.at[index, 'stream_name'] = stream_name
    return pdf

def emptyUDF(pdf):
    pdf['corrected_schema2'] = pdf.corrected_schema.apply(tuple)
    pdf2 = pdf.sort_values('total_files', ascending=False).drop_duplicates('corrected_schema2').drop('corrected_schema2',axis=1).reset_index(drop=True)
    pdf2["total_files"] = pdf2.shape[0]
    return pdf2

def checkMissMatch(pdf):
    #pdf['schema2'] = pdf.schema.apply(tuple)
    #pdf2 = pdf.sort_values('total_files', ascending=False).drop_duplicates('schema2').drop('schema2',axis=1).reset_index(drop=True)
    pdf2 = pdf
    pdf2["total_files"] = pdf2.shape[0]
    pdf2['corrected_schema'] = ""

    replace_column_names = {'__index_level_0__':None, "accelerometer_x":"x", "accelerometer_y":"y", "accelerometer_z":"z", "batter_level":"level", "dataquality":"quality", "data_quality_accelerometer":"quality" \
                            ,"gyroscope_x":"x","gyroscope_y":"y", "gyroscope_z":"z"}
    for index, row in pdf2.iterrows():
        corrected_schema = []
        schema = row['schema'].tolist()

        for s in schema:
            if s in replace_column_names:
                corrected_schema.append(replace_column_names.get(s))
            else:
                corrected_schema.append(s)
        corrected_schema = list(filter(None, corrected_schema))
        pdf2.at[index, 'corrected_schema'] = corrected_schema
    return pdf2

# add a stream_name (or maybe with version) column so column mismatch could be detected at stream level
df1 = df.groupBy("user_folder").applyInPandas(add_streamname_column,schema="total_files long, file_name string, schema array<string>, user_folder string, stream_name string")

df2 = df1.groupBy("stream_name").applyInPandas(checkMissMatch,schema="total_files long, file_name string, schema array<string>, user_folder string, stream_name string, corrected_schema array<string>")

df3 = df2.groupBy("user_folder").applyInPandas(emptyUDF,schema="total_files long, file_name string, schema array<string>, user_folder string, stream_name string, corrected_schema array<string>")

df3.where("schema!=corrected_schema").repartition(1).write.parquet("/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/final")





#df3.where("total_files>1").repartition(1).write.parquet("/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/temp.parquet")
#df3.select(*["total_files", "file_name", "user_folder", "schema", "corrected_schema"]).where("total_files>1").show(500,False)
#print(df2.select(*["total_files", "file_name", "user_folder", "schema", "corrected_schema"]).where("schema!=corrected_schema").count())