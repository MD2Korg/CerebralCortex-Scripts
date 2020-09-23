import json
import os
from cerebralcortex import Kernel
from cerebralcortex.core.metadata_manager.stream.metadata import Metadata
from cerebralcortex.core.data_manager.sql.data import SqlData

CC = Kernel("/home/ali/IdeaProjects/CerebralCortex-2.0/conf/")
metadata_files_path = "/home/ali/IdeaProjects/MD2K_DATA/hdfs/cc3_mperf/cc3_export/metadata/"
data_files_path = "/home/ali/IdeaProjects/MD2K_DATA/hdfs/cc3_mperf/cc3_export/"

sql_data = SqlData(CC)

def new_data_descript_frmt(data_descriptor, data):
    new_data_descriptor = {}
    basic_dd = {}
    attr = {}
    for field in data.schema.fields:
        if field.name not in ["timestamp", "localtime", "user", "version"]:
            basic_dd["name"] = field.name
            basic_dd["type"]= str(field.dataType)
    for key, value in data_descriptor.items():
        if key=="name" or key=="type":
            #basic_dd[key] = value
            pass
        else:
            attr[key] = value
    # remove any name inside attribute to avoid confusion
    if "name" in attr:
        attr.pop("name")
    new_data_descriptor["data_descriptor"] = basic_dd
    new_data_descriptor["data_descriptor"]["attributes"] =  attr
    sd = basic_dd
    sd["attributes"] = attr
    return sd

def new_module_metadata(ec_algo_pm):
    tmp = {}
    nm_attr = {}
    ec = ec_algo_pm
    tmp["name"] = ec["processing_module"]["name"]
    tmp["version"] = ec["algorithm"]["version"]
    tmp["authors"] = ec["algorithm"]["authors"]
    for key, value in ec["algorithm"].items():
        if key!="authors" and key!="version":
            nm_attr[key]=value

    for key, value in ec["processing_module"].items():
        if key!="input_streams":
            nm_attr[key]=value
    if "name" in nm_attr:
        nm_attr.pop("name")
    tmp["attributes"] = nm_attr

    return tmp

with os.scandir(metadata_files_path) as user_dir:
    for udir in user_dir:
        user_id = udir.name

        with os.scandir(udir.path) as metadata_files:
            for metadata_file in metadata_files:
                with open(metadata_file.path,"r") as mf:
                    new_metadata = {}
                    metadata = json.loads(mf.read())
                    # new data descriptor
                    new_dd_list = []
                    new_module = []
                    new_dd = {}

                    #data_file_path = "/home/ali/IdeaProjects/MD2K_DATA/hdfs/cc3_export/cc3_export/stream=org.md2k.data_analysis.day_based_data_presence/version=1/user=00ab666c-afb8-476e-9872-6472b4e66b68/org.md2k.data_analysis.day_based_data_presence.parquet"
                    data_file_path = data_files_path+"stream="+metadata["name"]+"/version=1/user="+str(user_id)+"/"+metadata["name"]+".parquet"
                    data = CC.sparkSession.read.load(data_file_path)

                    if isinstance(metadata["data_descriptor"],dict):
                        new_dd_list.append(new_data_descript_frmt(metadata["data_descriptor"], data))
                    else:
                        for dd in metadata["data_descriptor"]:
                            new_dd_list.append(new_data_descript_frmt(dd, data))

                    #TODO: this only support one module for now
                    new_module.append(new_module_metadata(metadata["execution_context"]))

                    input_streams = []
                    if "input_streams" in metadata["execution_context"]["processing_module"]:
                        for input_stream in metadata["execution_context"]["processing_module"]["input_streams"]:
                            input_streams.append(input_stream["name"])
                new_metadata["name"] = metadata["name"]
                new_metadata["description"] = metadata.get("description", "not-available")
                new_metadata["input_streams"] = input_streams
                new_metadata["data_descriptor"] = new_dd_list
                new_metadata["modules"] = new_module


                # for field in data.schema.fields:
                #     if field.name not in ["timestamp", "localtime", "user", "version"]:
                #         dd[field.name] = field.dataType
                sql_data.save_stream_metadata(Metadata().from_json_file(new_metadata))



