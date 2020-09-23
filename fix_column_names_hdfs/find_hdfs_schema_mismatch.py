#this script will read schema of parquet files in hdfs and check which files have diff schemas in the same folder


import pyarrow as pa
import pyarrow.parquet as pq
import pickle


hdfs_url = "hdfs://dantooine10dot:8020"
hdfs = pa.hdfs.connect(hdfs_url)
mismatched_metadata = []

for study in hdfs.ls("/cc3/"):
    print("PROCESSING STUDY", study)
    try:
        for stream in hdfs.ls(study):
            try:
                for version in hdfs.ls(stream):
                    try:
                        for user in hdfs.ls(version):
                            try:
                                files = hdfs.ls(user)
                                #if len(files)>2: put this check back again if schema mismatch is required
                                old_schema = []
                                for fle in hdfs.ls(user):
                                    try:
                                        if "_SUCCESS" not in fle:
                                            with hdfs.open(hdfs_url+fle) as f:
                                                current_schema = pq.read_schema(f).names
                                                mismatched_metadata.append({
                                                    "total_files":len(files),
                                                    "file_name":fle, "schema": current_schema, "user_folder":user
                                                })
                                    except Exception as e:
                                        print(str(e))
                            except Exception as e:
                                print(str(e))
                    except Exception as e:
                        print(str(e))
            except Exception as e:
                print(str(e))
    except Exception as e:
        print(str(e))

with open("mismatched_metadata.pkl", 'wb') as f:
    pickle.dump(mismatched_metadata, f, pickle.HIGHEST_PROTOCOL)