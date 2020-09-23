from mysql import connector
import os
import json
import simplejson
import glob

db_name = "cerebralcortex"
db_user = "root"
db_password = "pass"
db_url = "localhost"

conn = connector.connect(host=db_url, database=db_name, user=db_user, password=db_password)

cursor = conn.cursor(dictionary=True)

base_dir = "/home/ali/IdeaProjects/MD2K_DATA/cc1/cc3_metadata/"


def tmp(base_dir, status):
    all_metadata = []
    for stream_name in os.listdir(base_dir):
        with open(base_dir+stream_name,"r") as md:
            try:
                metadata = md.read()
                metadata = json.loads(metadata)
            except Exception as e:
                print(str(e),"\n", stream_name)
                exit()
        all_metadata.append({"status":status, "metadata":metadata, "stream_name":metadata.get("name")})
    return all_metadata

all_files = tmp(base_dir, "include") # done_list
#all_files = tmp(base_dir+"_ignore/", "ignore") # ignore_list



for rd in all_files:
    qry = "insert IGNORE into corrected_metadata (stream_name, metadata, status) VALUES(%s,%s,%s)"
    vals = rd.get("stream_name"), json.dumps(rd.get("metadata")), rd.get("status")
    cursor.execute(qry, vals)
    conn.commit()
cursor.close()
conn.close()

