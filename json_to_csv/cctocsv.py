# Copyright (c) 2018, MD2K Center of Excellence
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import json
import re
import flatten_json
import collections
import csv
import sys
import os
import bz2
from pprint import pprint

UUID_re = re.compile("^([a-z0-9]+-){4}[a-z0-9]+$")


def flatten(structure, key="", path="", flattened=None):
    if flattened is None:
        flattened = {}
    if type(structure) not in(dict, list):
        flattened[((path + "_") if path else "") + key] = structure
    elif isinstance(structure, list):
        for i, item in enumerate(structure):
            flatten(item, "%d" % i, path + "_" + key, flattened)
    else:
        for new_key, value in structure.items():
            flatten(value, new_key, path + "_" + key, flattened)
    return flattened

def processFile(f):
    s = []
    with bz2.open(f,'r') as file:
        for line in file:
            [ts, offset, jsondata] = line.decode().split(',', 2)
            a = re.sub('"{', "{", re.sub('}"', "}", jsondata))
            b = re.sub("u'(.*?)'", "\"\g<1>\"", a)
            c = re.sub("None", "\"None\"", b)
            d = re.sub("\"\"", "\\\"", c)

            j = json.loads(d[1:-3])
            s.append(j)
    return s


def determineKeys(data):
    allkeys = []
    for i in data:
        for e in data[i]:
            fJSON = flatten_json.flatten_json(e)
            for k in fJSON.keys():
                if k not in allkeys:
                    allkeys.append(k)

    allkeys.append('_id')
    return allkeys



def saveData(outputname, data):
    allkeys = determineKeys(data)
    with open(outputname, 'w') as outfile:
        header = sorted(allkeys)
        fp = csv.DictWriter(outfile, header)
        fp.writeheader()

        for p in data:
            for e in data[p]:
                r = collections.OrderedDict()
                for k in sorted(allkeys):
                    r[k] = ''
                r['_id'] = p
                fJSON = flatten_json.flatten_json(e)
                for k in fJSON.keys():
                    r[k] = fJSON[k]

                fp.writerow(r)


if __name__ == '__main__':
    """
    Usage: python3 <DATA_DIRECTORY> <OUTPUT_FILE> <PATTERN>
    > python3 cctocsv.py /home/hnat/testdata RANDOM_EMA.csv 'org.md2k.ema_scheduler+EMA+RANDOM_EMA+PHONE'
    """
    directory = sys.argv[1]
    outputname = sys.argv[2]
    pattern = sys.argv[3]
    filenames = []

    emaData = {}
    for p in os.scandir(directory):
        if p.is_dir and UUID_re.match(p.name):
            for f in os.scandir(p):
                if pattern in f.name and f.name[-4:] == '.bz2':
                    print(p.name,f)
                    emaData[p.name] = processFile(f)

    saveData(outputname, emaData)
