from mysql import connector
import json
import uuid
import pyarrow as pa
from pyspark.sql import SparkSession


conn1 = connector.connect(host="localhost", database="cerebralcortex", user="root", password="mEm62$%YsoK*88!")
cursor1 = conn1.cursor(dictionary=True)

conn2 = connector.connect(host="localhost", database="cerebralcortex_cc33", user="root", password="mEm62$%YsoK*88!")
cursor2 = conn2.cursor(dictionary=True)

spark = SparkSession.builder.appName("Upgrade CC3 to CC33").getOrCreate()

def get_hash(metadata)->str:
    """
    Get the unique hash of metadata. Hash is generated based on "stream-name + data_descriptor + module-metadata"

    Returns:
        str: hash id of metadata

    """
    name = metadata.get("name")
    study_name = metadata.get("study_name")
    data_descriptor = ""
    modules = ""
    for dd in metadata.get("data_descriptor"):
        data_descriptor += str(dd.get("name"))+str(dd.get("type"))
    for mm in metadata.get("modules"):
        modules += str(mm.get("name")) + str(mm.get("version")) + str(mm.get("authors"))
    hash_string = str(study_name)+str(name)+"None"+str(data_descriptor)+str(modules)
    hash_string = hash_string.strip().lower().replace(" ", "")

    return str(uuid.uuid3(uuid.NAMESPACE_DNS, hash_string))

def get_stream_metadata(study_name, stream_name, version,stream_path):

    vals = (stream_name,version)
    cursor1.execute("select * from stream where name=%s and version=%s", vals)
    rows = cursor1.fetchall()
    if len(rows)<1:
        sample_metadata = {

            "modules": [
                {
                    "name": "not-available",
                    "authors": [],
                    "version": "1.0.0",
                    "attributes": {}
                }
            ],
            "annotations": [],
            "description": "no-description",
            "input_streams": [],

        }
        print("NOT FOUND: ",study_name, stream_name, version)
        df=spark.read.load("hdfs://dantooine10dot:8020"+stream_path)
        fields = json.loads(df.schema.json()).get("fields")
        dd = []
        for fld in fields:
            dd.append({fld.get("name"):fld.get("type")})
        sample_metadata["name"] = stream_name.lower()
        sample_metadata["data_descriptor"] = dd
        sample_metadata["study_name"] = study_name

        metadata_hash = get_hash(sample_metadata)
    else:
        print("FOUND: ",study_name, stream_name, version)
        sample_metadata = json.loads(rows[0].get("metadata"))
        sample_metadata["study_name"] = study_name
        metadata_hash = get_hash(sample_metadata)

    insert_vals = (stream_name, version, study_name, metadata_hash, json.dumps(sample_metadata))
    cursor2.execute("insert ignore into stream (name, version, study_name, metadata_hash, stream_metadata) values(%s, %s, %s, %s, %s)", insert_vals)
    conn2.commit()

fs = pa.hdfs.connect("dantooine10dot", 8020)
study_names = fs.ls("/cc3/")

for study_name in study_names:
    std_name =study_name.replace("/cc3/study=","")
    if (std_name=="moods" or std_name=="mcontain") and std_name!="moods_backup":
        for stream_name in fs.ls(study_name):
            strm_name = stream_name.replace("/cc3/study="+std_name+"/stream=","")
            for version in fs.ls(stream_name):
                if fs.isdir(version):
                    vrsn = version[-1]
                    get_stream_metadata(std_name, strm_name, int(vrsn), version)
                        #print(stream_name, vrsn)



