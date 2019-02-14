import json
import os

metadata_files_path = "/home/ali/IdeaProjects/MD2K_DATA/hdfs/cc3_export/cc3_export/metadata"

def new_data_descript_frmt(data_descriptor):
    new_data_descriptor = {}
    basic_dd = {}
    attr = {}
    for key, value in data_descriptor.items():
        if key=="name" or key=="type":
            basic_dd[key] = value
        else:
            attr[key] = value
    new_data_descriptor["data_descriptor"] = basic_dd
    new_data_descriptor["data_descriptor"]["attributes"] =  attr
    sd = basic_dd
    sd["attributes"] = attr
    return sd
def new_module_metadata(ec_algo_pm):
    nm_list = []
    tmp = {}
    new_module = {}
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
            try:
                nm_attr[key]=value
            except:
                pass
    tmp["attributes"] = nm_attr
    new_module["module"] = tmp
    return new_module

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
                    if isinstance(metadata["data_descriptor"],dict):
                        new_dd_list.append(new_data_descript_frmt(metadata["data_descriptor"]))
                    else:
                        for dd in metadata["data_descriptor"]:
                            new_dd_list.append(new_data_descript_frmt(dd))
                    new_dd["data_descriptor"] = new_dd_list

                    # new module block
                    new_module.append(new_module_metadata(metadata["execution_context"]))

                    if "input_streams" in metadata["execution_context"]["processing_module"]:
                        input_streams = []
                        for input_stream in metadata["execution_context"]["processing_module"]["input_streams"]:
                            input_streams.append(input_stream["name"])
                #print(metadata_file.name)
                new_dd_list


