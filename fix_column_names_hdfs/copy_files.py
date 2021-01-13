from pyspark.sql import SparkSession
import pyarrow as pa
import pyarrow.parquet as pq
from subprocess import PIPE, Popen
import pandas as pd
import json
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType, TimestampType

def copy_files():
    spark = SparkSession.builder.appName("Copy Files").getOrCreate()
    parquet_file_path = "/home/cnali/fix_column_names/mismatched_metadata.parquet"

    @pandas_udf("user_folder string, success boolean", PandasUDFType.GROUPED_MAP)
    def copyFiles(pdf):
        """
        Copy files from cc3 folder to temporary location for fixing column names
        :return:
        """
        try:
            hdfs = pa.hdfs.connect("dantooine10dot",8020)
            new_folder_name = "/corrupted_column_mismatch/"
            user_folder = pdf["user_folder"].iloc[0]
            new_location = user_folder.replace("/cc3/", new_folder_name)
            hdfs.mkdir(new_location)
            # print("*"*100)
            # print(user_folder,"\n", new_location)
            for index, row in pdf.iterrows():
                put = Popen(["hadoop", "fs", "-cp", row["file_name"], row["file_name"].replace("/cc3/", new_folder_name)], stdin=PIPE, bufsize=-1)
                put.communicate()
            return pd.DataFrame([[user_folder,1]],columns=['user_folder','success'])
        except:
            return pd.DataFrame([[user_folder,0]],columns=['user_folder','success'])

    # read parquet file
    df = spark.read.load(parquet_file_path)
    results = df.groupBy("user_folder").apply(copyFiles)
    print("Total success", results.where("success=1").count())

def copy_corrected_files():
    spark = SparkSession.builder.appName("Copy Files").getOrCreate()
    parquet_file_path = "/home/cnali/fix_column_names/mismatched_metadata.parquet"

    @pandas_udf("user_folder string, success boolean", PandasUDFType.GROUPED_MAP)
    def copyCorrectedFiles(pdf):
        """
        Copy files from cc3 folder to temporary location for fixing column names
        :return:
        """
        try:
            user_folder = pdf["user_folder"].iloc[0]
            for index, row in pdf.iterrows():
                corrected_file = row["file_name"].replace("/cc3/", "/fixed_column_names/")
                cc3_file = row["file_name"]
                corrected_file_name = "hdfs://dantooine10dot:8020"+corrected_file
                cc3_file_name = "hdfs://dantooine10dot:8020"+cc3_file
                data = pq.read_table(corrected_file_name)
                pq.write_table(data, cc3_file_name)
            return pd.DataFrame([[user_folder,1]],columns=['user_folder','success'])
        except:
            return pd.DataFrame([[user_folder,0]],columns=['user_folder','success'])

    # read parquet file
    df = spark.read.load(parquet_file_path)
    results = df.groupBy("user_folder").apply(copyCorrectedFiles)
    print("Total success", results.where("success=1").count())

def fix_column_names():
    spark = SparkSession.builder.appName("Copy Files").getOrCreate()
    parquet_file_path = "/home/cnali/fix_column_names/mismatched_metadata.parquet"

    @pandas_udf("user_folder string, success boolean", PandasUDFType.GROUPED_MAP)
    def fixColumnNames(pdf):
        """
        Copy files from cc3 folder to temporary location for fixing column names
        :return:
        """
        try:
            hdfs = pa.hdfs.connect("dantooine10dot",8020)
            old_folder_name = "/cc3/"
            new_folder_name = "/fixed_column_names/"
            user_folder = pdf["user_folder"].iloc[0]
            new_location = user_folder.replace(old_folder_name, new_folder_name)
            hdfs.mkdir(new_location)
            for index, row in pdf.iterrows():
                file_name = "hdfs://dantooine10dot:8020"+row["file_name"]
                if hdfs.exists(row["file_name"].replace(old_folder_name,new_folder_name)):
                    #print("ALREADY PROCESSED - ",row["file_name"].replace(old_folder_name,new_folder_name))
                    pass
                else:
                    data = pq.read_table(file_name)
                    try:
                        data2 = data.drop(["__index_level_0__"])
                    except:
                        data2 = data
                    if isinstance(row["corrected_schema"], str):
                        new_column_names = eval(row["corrected_schema"])
                    else:
                        new_column_names = row["corrected_schema"]
                    data3 = data2.rename_columns(new_column_names)
                    pq.write_table(data3, file_name.replace(old_folder_name,new_folder_name))
            return pd.DataFrame([[user_folder,1]],columns=['user_folder','success'])
        except Exception as e:
            print("*"*10, user_folder, str(e))
            return pd.DataFrame([[user_folder,0]],columns=['user_folder','success'])

    # read parquet file
    df = spark.read.load(parquet_file_path)
    results = df.groupBy("user_folder").apply(fixColumnNames)
    print("Total success", results.where("success=1").count())

def delete_corrupt_files():
    spark = SparkSession.builder.appName("Copy Files").getOrCreate()

    parquet_file_path = "/home/cnali/fix_column_names/mismatched_metadata.parquet"

    @pandas_udf("user_folder string, success boolean", PandasUDFType.GROUPED_MAP)
    def deleteCorruptFiles(pdf):
        """
        Copy files from cc3 folder to temporary location for fixing column names
        :return:
        """
        try:
            hdfs = pa.hdfs.connect("dantooine10dot",8020)
            user_folder = pdf["user_folder"].iloc[0]
            for index, row in pdf.iterrows():
                hdfs.rm(row["file_name"])
            return pd.DataFrame([[user_folder,1]],columns=['user_folder','success'])
        except:
            return pd.DataFrame([[user_folder,0]],columns=['user_folder','success'])



    # read parquet file
    df = spark.read.load(parquet_file_path)

    results = df.groupBy("user_folder").apply(deleteCorruptFiles)

    print("Total success", results.where("success=1").count())

# Following sequence to correct files
# copy_files() # first take backup of corrupt files
fix_column_names() # fix column names
# delete_corrupt_files() # delete corrupt files
# copy_corrected_files() # upload fixed files

