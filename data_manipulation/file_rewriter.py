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


UUID_re = re.compile("^([a-z0-9]+-){4}[a-z0-9]+$")
directory=sys.argv[1]

def update_stream_file_contents(filepath: str) -> str:
    new_file_contents = []
    rewrite_flag = False
    try:
        with gzip.open(filepath) as fp:
            for line in fp.readlines():
                gzip_line_content = line.decode('utf-8')
                try:
                    ts, offset, sample = gzip_line_content[:-1].split(',', 2)
                    
                    try:
                        ts = int(ts)

                        datapoints = sample.split(',')
                        try:
                            d = list(map(float, datapoints))
                        except:
                            rewrite_flag = True
                            pass

                        
                        new_file_contents.append( str(ts)+","+str(offset)+",1\n")
                    except:
                        print("Inner Error", filepath)
                        pass
                except:
                    print("Outer Error", filepath)
                    pass


        if rewrite_flag:
            print(filepath, rewrite_flag)            
#            print(new_file_contents[:5])

    except:
        print("ERROR", filepath)


    if len(new_file_contents) > 0:
        with gzip.open(filepath, 'wb') as new_file:
            for l in new_file_contents:
                new_file.write(l.encode('utf-8'))


def process_participant(p):
    basedir = os.path.join(directory,p)
    UUID_mapping = {}
    SKIP_mapping = {}
    for datedir in scandir(basedir):
        if datedir.is_dir:
            #print('Processing:',p, datedir.name)
            for ds in scandir(datedir):
                if ds.is_dir and ds.name not in SKIP_mapping:
                    #print(ds.name, len(SKIP_mapping))
                    for f in scandir(ds):
                        if f.name[-5:] == '.json':
                            #print("Evaluating:",f.path)
                            with open(f,'r') as input_file:
                                metadata = json.loads(input_file.read())
                                
                                if metadata['identifier'] not in UUID_mapping and 'CU_NOTIF_RM_TICKERTEXT' in metadata['name']:
                                    UUID_mapping[metadata['identifier']] = metadata['name']
                                    print(p,metadata['identifier'],metadata['name'])
                                else:
                                    SKIP_mapping[metadata['identifier']] = metadata['name']
                                break


                    if ds.name in UUID_mapping:
                        datasource = UUID_mapping[ds.name]
                        for f in scandir(ds):
                            if f.name[-3:] == '.gz':
                                #pass
                                update_stream_file_contents(f.path)


if __name__ == '__main__':
    participants = []
    for f in scandir(directory):
        if f.is_dir and UUID_re.match(f.name):
            participants.append(f.name)

    #p = Pool(1)

    #p.map(process_participant, participants)
    for p in participants:
        process_participant(p)
