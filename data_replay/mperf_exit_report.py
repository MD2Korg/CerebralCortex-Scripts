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
from pprint import pprint
from collections import OrderedDict
from functools import reduce


directory=sys.argv[1]

identifiers = {}
for f in scandir(directory):
    if f.is_file:
        id = f.name[6:10]
        if id not in identifiers:
            identifiers[id] = {}

        identifiers[id][f.name[11:-4]] = f


fieldnames = ['day', 'motionsense_left_led_good','motionsense_left_led_total','motionsense_right_led_good','motionsense_right_led_total', 'motionsense_left_accel_good','motionsense_left_accel_total','motionsense_right_accel_good','motionsense_right_accel_total', 'autosense_ble_respiration_good','autosense_ble_respiration_total', 'autosense_ble_accel_good', 'autosense_ble_accel_total']
for participant in identifiers:
    print("Processing:",participant)
    output = OrderedDict()
    for f in identifiers[participant]:
        if identifiers[participant][f].is_file:
            with open(identifiers[participant][f], 'rt') as inputfile:
                csvfile = csv.reader(inputfile)
                next(csvfile, None)  # skip the headers
                next(csvfile, None)  # skip the headers

                
                for r in csvfile:
                    day = r[0]
                    good = r[1]
                    total = r[2]#reduce(lambda x,y: float(x)+float(y), r[1:])
                
                    if day not in output:
                        output[day] = OrderedDict()

                    output[day]['day'] = day
                    output[day][f+'_good'] = good
                    output[day][f+'_total'] = total

#    pprint(output)
    
    with open(participant + '_report.csv','w') as csvfileoutput:
        writer = csv.DictWriter(csvfileoutput, fieldnames=fieldnames)
        writer.writeheader()
        for r in OrderedDict(sorted(output.items(), key=lambda t: t[0])):
            writer.writerow(output[r])
