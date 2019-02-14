import uuid
from cerebralcortex.core.data_manager.raw.stream_handler import DataSet
from cerebralcortex.cerebralcortex import CerebralCortex
from cerebralcortex.core.datatypes.datastream import DataStream
from cerebralcortex.core.datatypes.datastream import DataPoint
import os
import pickle
import datetime
import gzip
from multiprocessing import Pool
import argparse

parser = argparse.ArgumentParser(description='Export data from Cerebral Cortex')
parser.add_argument('participants_uuids', metavar='UUID', nargs='+', help='list of participant UUIDs')
parser.add_argument('--conf',dest='configuration_file', required=True, help='Cerebral Cortex configuration file')
parser.add_argument('--output',dest='root_dir', required=True, help='Base output directory')
parser.add_argument('--study',dest='study_name',default='mperf',help='Study name')
args = parser.parse_args()


root_dir = os.path.join(args.root_dir)
CC = CerebralCortex(args.configuration_file)

def append_csv(ds):
    with gzip.open(os.path.join(root_dir,ds.owner) + '/' + ds.name + '___' + ds.identifier + '.csv.gz', 'at',compresslevel=1,encoding='utf-8') as f:
        for dp in ds.data:
            if type(dp.sample) is list:
                dp.sample = ','.join(map(str,dp.sample))

            if type(dp.sample) is str and dp.sample[-1] is '\n':
                dp.sample = dp.sample[:-1]

            f.write(','.join([str(dp.start_time), str(dp.end_time), str(dp.sample)]) + '\n')


def process_stream(stream):
    start_date = stream['start_time']
    end_date = stream['end_time']
    all_days = []
    while True:
        all_days.append(start_date.strftime('%Y%m%d'))
        start_date += datetime.timedelta(days = 1)
        if start_date > end_date : break
    print('-'*20, 'Exporting stream: ',stream['name'], '-'*20)
    for day in all_days:
        print("Processing day:",stream['name'], day)
        temp  =CC.get_stream(stream['identifier'], user['identifier'], day, localtime=False)
        append_csv(temp)



if __name__ == '__main__':
    
    possible_users = CC.get_all_users(args.study_name)
    for user in possible_users:
        if user['identifier'] in args.participants_uuids:
            userdir = os.path.join(root_dir,user['identifier'])
            if not os.path.exists(userdir):
                os.mkdir(userdir)

            streams = CC.get_user_streams(user['identifier'])
            for name in streams:
                process_stream(streams[name])

