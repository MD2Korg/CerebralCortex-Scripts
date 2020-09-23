from mysql import connector
import os
import json
import simplejson
import glob
import pandas as pd
from datetime import datetime

db_name = "cerebralcortex"
db_user = "root"
db_password = "pass"
db_url = "localhost"

conn = connector.connect(host=db_url, database=db_name, user=db_user, password=db_password)

cursor = conn.cursor(dictionary=True)
#cursor = conn.cursor(buffered=True)

def get_study_name():
    qry = "select identifier, user_metadata from user"
    all_vals = []
    cursor.execute(qry)
    for row in cursor:
        all_vals.append({"user_id":row["identifier"],"study_name":json.loads(row["user_metadata"]).get("study_name")})
    return all_vals

study_names = get_study_name()

def find_study(user_id):
    for sn in study_names:
        if sn.get("user_id")==user_id:
            return sn.get("study_name")
    return "no-study-name"

qry = "select *, count(*) as total from ingestion_logs_tmp group by user_id, stream_name, fault_type, success"
cursor.execute(qry)
all_data = []
with open("/home/ali/IdeaProjects/MD2K_DATA/mysql_bk/summary.csv", "w+") as fd:
    for row in cursor:
        study_name = find_study(row["user_id"])
        fault_desc = row["fault_description"]
        if fault_desc=="":
            fault_desc = "NO-DESCRIPTION"
        fd.write(row["user_id"]+", "+study_name+", "+row["stream_name"]+", "+row["fault_type"]+", "+str(row["success"])+","+str(row["total"])+"\n")

cursor.close()
conn.close()

