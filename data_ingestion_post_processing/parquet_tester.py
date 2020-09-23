import sys
import os
import operator
import pickle
import pyarrow.parquet as pq
from pprint import pprint

from pyspark.sql import SparkSession
spark = SparkSession.builder.config('spark.driver.memory','24g').config('spark.driver.maxResultSize','10g').appName('Data File Tester').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

schema_file = 'schema_file.pickle'

base_path = sys.argv[1]
files_to_check = 9

proper_schema = {}

def file_to_schema( input_tuple ):
    sname, fpath = input_tuple
    try:
        df = pq.read_schema(fpath)
    except:
        print("ERROR",fpath)
    return ';'.join([str(df.names), str(df.types)])


if os.path.exists(schema_file):
    with open(schema_file, 'rb') as f:
        proper_schema = pickle.load(f)
else:
    print('Scanning files')
    for stream in os.scandir(base_path):
        print(stream)
        if stream.is_file():
            continue
        count = 0
        participants = 0
        type_list = {}
        for version in os.scandir(stream):
            for user in os.scandir(version):
                for f in os.scandir(user):
                    df = file_to_schema((stream.name, f.path))
                    if df not in type_list:
                        type_list[df] = 0
                    type_list[df] += 1

                    count += 1
                    if count > files_to_check:
                        break

                count = 0
                participants += 1
                if participants > files_to_check:
                    break

            break
        best_fit = max(type_list.items(), key=operator.itemgetter(1))[0]
        print(stream.name, best_fit)
        proper_schema[stream.name] = best_fit


    with open(schema_file, 'wb') as f:
        pickle.dump(proper_schema, f, pickle.HIGHEST_PROTOCOL)


def file_to_schema_bad( input_tuple ):
    sname, fpath = input_tuple
    matched = False
    #corrupted = False
    try:
        df = pq.read_schema(fpath)
        string_match = ';'.join([str(df.names), str(df.types)])


        if sname in proper_schema:
            if proper_schema[sname] == string_match:
                matched = True
        return (matched, sname, fpath, proper_schema[sname], string_match, "CORRECT-FILE")

    except:
        return (matched, sname, fpath, proper_schema[sname], "NULL", "NULL", "CORRUPTED-FILE")

with open('schema_error.log','at') as output_file:

    print('Identifying bad schemas')
    for stream in os.scandir(base_path):
        file_list = []
        if stream.is_file():
            continue
        for version in os.scandir(stream):
            for user in os.scandir(version):
                print('Processing:',stream.name, user.name)
                for f in os.scandir(user):
                    file_list.append( (stream.name, f.path) )


                print('File Count:', len(file_list))
                dist_file_list = spark.sparkContext.parallelize(file_list, 2000)

                listings = dist_file_list.map(file_to_schema_bad)
                for entry in listings.collect():
                    if not entry[0]:
                        print(entry)
                        output_file.write(';'.join(entry[1:]) + '\n')

                file_list = []