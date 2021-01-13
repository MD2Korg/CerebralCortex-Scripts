from mysql import connector
import os
import json
import simplejson
import glob


db_name = "cerebralcortex"
db_user = "root"
db_password = "pass"
db_url = "localhost"
output_folder_path = "/home/ali/IdeaProjects/MD2K_DATA/metadata_cc_v2/"

conn = connector.connect(host=db_url, database=db_name, user=db_user, password=db_password)

cursor = conn.cursor1(dictionary=True)

# cursor.execute("select name, data_descriptor, execution_context, annotations from stream where name not like '%beacon%'  group by name order by name")
# rows = cursor.fetchall()
# cursor.close()
# conn.close()
#
# for row in rows:
#     stream_path = output_folder_path+str(row["name"])
#
#     if not os.path.exists(stream_path):
#         os.mkdir(stream_path)
#         with open(stream_path+"/metadata.json", "w") as mdf:
#             frmt_md = {}
#             frmt_md["name"] = row["name"]
#             frmt_md["annotations"] = json.loads(row["annotations"])
#             frmt_md["data_descriptor"] = json.loads(row["data_descriptor"])
#             frmt_md["execution_context"] = json.loads(row["execution_context"])
#             mdf.write(json.dumps(frmt_md, indent=4))


base_dir = "/home/ali/IdeaProjects/metadata-verification/"


def tmp(base_dir, status):
    all_metadata = []
    for stream_name in os.listdir(base_dir):
        with open(base_dir+stream_name+"/metadata.json","r") as md:
            metadata = md.read()
            metadata = json.loads(metadata)
        all_metadata.append({"status":status, "metadata":metadata, "stream_name":stream_name})
    return all_metadata

#all_files = tmp(base_dir+"_done/", "include") # done_list
all_files = tmp(base_dir+"_ignore/", "ignore") # ignore_list



for rd in all_files:
    qry = "insert IGNORE into corrected_metadata (stream_name, metadata, status) VALUES(%s,%s,%s)"
    vals = rd.get("stream_name"), json.dumps(rd.get("metadata")), rd.get("status")
    cursor.execute(qry, vals)
    conn.commit()
cursor.close()
conn.close()

