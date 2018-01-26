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


import glob
import sys
import csv
from os import scandir
import os
from pprint import pprint
from collections import OrderedDict
from functools import reduce
import re
import gzip
import datetime
import json
from pprint import pprint
from multiprocessing import Pool


dq_interval = 3.0/3600
UUID_re = re.compile("^([a-z0-9]+-){4}[a-z0-9]+$")
directory=sys.argv[1]


def process_participant(p):
    basedir = os.path.join(directory,p)
    participant_data = {}
    UUID_mapping = {}
    SIZE_mapping = {}
    for datedir in scandir(basedir):
        if datedir.is_dir:
            print('Processing:',p, datedir.name)
            for ds in scandir(datedir):
                if ds.is_dir:
                    for f in scandir(ds):
                        if f.name[-5:] == '.json':
                            with open(f,'r') as input_file:
                                metadata = json.loads(input_file.read())
                                
                                if metadata['identifier'] not in UUID_mapping and 'DATA_QUALITY--' in metadata['name']:
                                    UUID_mapping[metadata['identifier']] = metadata['name']
                                    #print(p,metadata['identifier'],metadata['name'])

                                if metadata['identifier'] not in SIZE_mapping and 'RAW--' in metadata['name']:
                                    SIZE_mapping[metadata['identifier']] = metadata['name']
                                    #print('SIZE',p,metadata['identifier'],metadata['name'])

                                break


                    if ds.name in SIZE_mapping:
                        datasource = SIZE_mapping[ds.name]
                        if datasource not in participant_data:
                            participant_data[datasource] = {}

                        for f in scandir(ds):
                            if f.name[-3:] == '.gz':
                                ts = datedir.name
                                if ts not in participant_data[datasource]:
                                    participant_data[datasource][ts] = (0,0)
                                temp = participant_data[datasource][ts]
                                participant_data[datasource][ts] = (temp[0] + 1, temp[1] + os.stat(f).st_size)
                                #print(ts,participant_data[datasource][ts])

                                    
                    if ds.name in UUID_mapping:
                        datasource = UUID_mapping[ds.name]
                        if datasource not in participant_data:
                            participant_data[datasource] = {}

                        for f in scandir(ds):
                            if f.name[-3:] == '.gz':
                                try:
                                    with gzip.open(f,'rt')as input_file:
                                        data = csv.reader(input_file)
                                        for row in data:
                                            ts = datetime.datetime.fromtimestamp(int(row[0])/1000).strftime('%Y%m%d')
                                            if row[2] == '0':
                                                good = dq_interval
                                            else:
                                                good = 0
                                            total = dq_interval
                                            if ts not in participant_data[datasource]:
                                                participant_data[datasource][ts] = (0,0)
                                            temp = participant_data[datasource][ts]
                                            participant_data[datasource][ts] = (temp[0] + good, temp[1] + total)
                                except:
                                    print("ERROR",f.path)
                                    #Corrupt file
                                    pass

    fieldnames = set()
    output = {}
    for s in participant_data:
        data = participant_data[s]
        fieldnames.add('0_day')
        fieldnames.add(s+'_Good')
        fieldnames.add(s+'_Total')
        for day in data:
            if day not in output:
                output[day] = {}
                         
            output[day]['0_day'] = day
            output[day][s+'_Good'] = data[day][0]
            output[day][s+'_Total'] = data[day][1]

    fieldnames = sorted(list(fieldnames))
    print(fieldnames)

    with open(p + '_report.csv','w') as csvfileoutput:
        writer = csv.DictWriter(csvfileoutput, fieldnames=fieldnames)
        writer.writeheader()
        for r in OrderedDict(sorted(output.items(), key=lambda t: t[0])):
            writer.writerow(output[r])

    return participant_data




if __name__ == '__main__':
    participants = []
    for f in scandir(directory):
        if f.is_dir and UUID_re.match(f.name):
            participants.append(f.name)


    participants = participants[:32]

    #print(participants)

    p = Pool(8)

    p.map(process_participant, participants)
    print("DONE")
