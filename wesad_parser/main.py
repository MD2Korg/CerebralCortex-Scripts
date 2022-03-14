import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import numpy as np
from datetime import datetime, timezone
import os

data_folder_path = "/Users/ali/IdeaProjects/MD2K_DATA/WESAD/"

def get_data(object, user_name, start_time, total_records, frequency):
    chest = ["ACC","ECG","EMG","EDA","Temp","Resp"]
    wrist = ["ACC","BVP","EDA","TEMP"]
    label = ["label"]

    for cht in chest:
        if cht=="ACC":
            accel_convert_to_table(object.get("signal").get("chest").get(cht), stream_name="wesad.chest."+cht, user_name=user_name, start_time=start_time, total_records=total_records, frequency=700)
        else:
            convert_to_table(object.get("signal").get("chest").get(cht), column_name=cht, stream_name="wesad.chest."+cht, user_name=user_name, start_time=start_time, total_records=total_records, frequency=700)

    for cht in wrist:
        if cht=="ACC":
            accel_convert_to_table(object.get("signal").get("wrist").get(cht), stream_name="wesad.wrist."+cht, user_name=user_name, start_time=start_time, total_records=total_records, frequency=frequency)
        else:
            convert_to_table(object.get("signal").get("wrist").get(cht), column_name=cht, stream_name="wesad.wrist."+cht, user_name=user_name, start_time=start_time, total_records=total_records, frequency=frequency)
    label = np.atleast_2d(object.get("label")).T
    convert_to_table(label, column_name="label", stream_name="wesad.label", user_name=user_name, start_time=start_time, total_records=total_records, frequency=700)



def get_timestamps(start_time=None, total_records=None, frequency=None):
    start_time = start_time
    stop = start_time+(total_records/frequency)
    stepwidth = 1/frequency
    localtime = []
    timestamp = []
    for i in np.arange(start_time,stop,stepwidth):
        timestamp.append(datetime.fromtimestamp((i)))
        localtime.append(datetime.fromtimestamp((i)))
    return {"timestamp":timestamp, "localtime":localtime, "start_time": start_time, "stop_time":stop}

def accel_convert_to_table(accel, stream_name, user_name, start_time, total_records, frequency):
    ts = get_timestamps(start_time=start_time, total_records=accel[:, 0].size, frequency=frequency)
    print("Converting into Table: Stream Name: ", stream_name)
    try:
        ndarray_table = pa.table(
            {
                "timestamp": ts.get("timestamp"),
                "localtime": ts.get("localtime"),
                "x": accel[:, 0],
                "y": accel[:, 1],
                "z": accel[:, 2]
            }
        )
        file_path = (data_folder_path+"parsed/stream="+stream_name+"/version=1/user="+user_name+"/").lower()
        Path(file_path).mkdir(parents=True, exist_ok=True)
        pq.write_table(ndarray_table,file_path+"data.parquet")
    except Exception as e:
        print(e)

def convert_to_table(onedarray, column_name, stream_name, user_name, start_time, total_records, frequency):
    ts = get_timestamps(start_time=start_time, total_records=onedarray[:, 0].size, frequency=frequency)
    print("Converting into Table: Stream Name: ", stream_name)
    try:
        ndarray_table = pa.table(
            {
                "timestamp": ts.get("timestamp"),
                "localtime": ts.get("localtime"),
                column_name.lower(): onedarray[:, 0]
            }
        )
    except Exception as e:
        print(str(e))
    file_path = (data_folder_path+"parsed/stream="+stream_name+"/version=1/user="+user_name+"/").lower()
    Path(file_path).mkdir(parents=True, exist_ok=True)
    pq.write_table(ndarray_table,file_path+"data.parquet")

def convert_quest_df_to_table(quest_df, stream_name, user_name):
    quest_df["# ORDER"]=quest_df["# ORDER"].str.replace(r'[^A-Za-z0-9]+', '')
    quest_df=quest_df.rename(columns = {'# ORDER':'ORDER'})
    quest_df.columns = quest_df.columns.str.replace(" ", "_")
    try:
        ndarray_table = pa.Table.from_pandas(quest_df)
        file_path = (data_folder_path+"parsed/stream="+stream_name+"/version=1/user="+user_name+"/").lower()
        Path(file_path).mkdir(parents=True, exist_ok=True)
        pq.write_table(ndarray_table,file_path+"data.parquet")
    except Exception as e:
        print(e)


for i in range(2,3):
    try:
        print("Participant ID: ", i)
        object = pd.read_pickle(data_folder_path+"S"+str(i)+"/S"+str(i)+".pkl")
        df = pd.read_csv(data_folder_path+"S"+str(i)+"/S"+str(i)+"_E4_Data/ACC.csv", header=None)
        quest_df =pd.read_csv(data_folder_path+"S"+str(i)+"/S"+str(i)+"_quest.csv", skiprows=[0], sep=';')
        convert_quest_df_to_table(quest_df, stream_name="wesad.quest", user_name="S"+str(i))
        start_time = df[0][0]
        frequency = df[0][1]
        total_records = int(df.size)
        get_data(object, "S"+str(i), start_time, total_records, frequency)
    except Exception as e:
        print("ERROR",e)

stream_name = ""

