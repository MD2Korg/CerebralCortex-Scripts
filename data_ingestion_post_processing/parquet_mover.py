import os
import sys
from pprint import pprint
import json
from pathlib import Path
import shutil

results = {}
with open(sys.argv[1], 'rt') as input_file:
    for row in input_file:
        sname, fpath, correct_label, correct_type, incorrect_label, incorrect_type, corrupted = row.split(';')
        if sname not in results:
            results[sname] = {}

        if incorrect_label not in results[sname]:
            results[sname][incorrect_label] = 0

        results[sname][incorrect_label] += 1

        print('Moving',fpath)
        if "CORRUPTED-FILE" in corrupted:

            new_path = Path(fpath.replace('cc3_data','cc3_data_corrupted_parquet')).parent
        else:
            new_path = Path(fpath.replace('cc3_data','cc3_data_corrupt')).parent

        os.makedirs(new_path, exist_ok=True)

        if os.path.exists(fpath):
            shutil.move(fpath, new_path)


pprint(results)

with open('schema.json', 'wt') as output_file:
    output_file.write(json.dumps(results))
