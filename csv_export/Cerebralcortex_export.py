from cerebralcortex.cerebralcortex import CerebralCortex
import json
import uuid
import datetime
import os
from joblib import Parallel, delayed
import coloredlogs, logging
import argparse


# In[2]:
# Create a logger object.
logger = logging.getLogger(__name__)

# If you don't want to see log messages from libraries, you can pass a
# specific logger object to the install() function. In this case only log
# messages originating from that logger will show up on the terminal.
coloredlogs.install(level='DEBUG', logger=logger)


parser = argparse.ArgumentParser()

parser.add_argument('--study', help="study name as appears in MySQL user metadata", required=True)
parser.add_argument('--output', help="Output directory for the exported files", required=True)
parser.add_argument('--participant', help="Participant username")
parser.add_argument('-n', '--num_jobs', help="Number of concurrent export to run", type=int, default=1)
args = parser.parse_args()



CC = CerebralCortex("/cerebralcortex/code/config/cc_starwars_configuration.yml")
output_dir = args.output
study_name = args.study

# In[3]:


users = CC.get_all_users(study_name=study_name)


# In[4]:


def get_stream_days(cc,identifier, stream):
    duration = cc.get_stream_duration(stream['identifier'])
    day = duration['start_time']
    result = []
    while day < (duration['end_time']+datetime.timedelta(days=1)):
        day_str = day.strftime('%Y%m%d')
        result.append(day_str)
        day = day + datetime.timedelta(days=1)

    return result

def write_metadata_file(file, metadata_input):
    with open(file,'wt') as output_file:
        metadata = metadata_input.copy()
        metadata['start_time'] = str(metadata['start_time'])
        metadata['end_time'] = str(metadata['end_time'])
        metadata['data_descriptor'] = json.loads(metadata['data_descriptor'])
        metadata['execution_context'] = json.loads(metadata['execution_context'])
        metadata['annotations'] = json.loads(metadata['annotations'])
        metadata.pop('tmp',None)
        metadata.pop('type',None)
        output_file.write(json.dumps(metadata, sort_keys=True,indent=4))
        logger.info('Metadata file %s written' % file)

def write_data_file(file, streams, user, s):
    cc = CerebralCortex("/cerebralcortex/code/config/cc_starwars_configuration.yml")


    if os.path.isfile(file + '.gz'):
        print("Already Processed %s" % file + '.gz')
        return True

    with open(file + '_temp','wt') as output_file:
        for stream_id in streams[s]['stream_ids']:
            logger.info('Processing %s' %  streams[s]['name'])
            print('Processing %s' %  streams[s]['name'])
            days = get_stream_days(cc,stream_id,streams[s])
            for day in days:
                st = datetime.datetime.now()

                print("XXXXXXXXXX",streams[s]['name'], user['identifier'], stream_id, day)

                datastream = cc.get_stream(stream_id,user['identifier'], day, localtime=False)
                et = datetime.datetime.now()
                if len(datastream.data) > 0:
                    if len(datastream.data) > 100000:
                        logger.info('%s %s %d %s' % (streams[s]['name'], day,len(datastream.data),str(et-st)))
                        print('%s %s %d %s' % (streams[s]['name'], day,len(datastream.data),str(et-st)))
                    try:
                        for d in datastream.data:
                            output_string = str(int(d.start_time.timestamp()*1e6))

                            if type(d.end_time) is datetime:
                                output_string += ',' + str(int(d.end_time.timestamp()*1e6))
                            else:
                                output_string += ',-1'

                            output_string += ',' + str(int(d.offset))

                            if type(d.sample) is list:
                                output_string += ',' + ','.join(map(str,d.sample))
                            else:
                                output_string += ',' + str(d.sample)

                            output_file.write(output_string + '\n')
                    except Exception as e:
                        logger.error("Stream %s has had a parsing error" % streams[s]['name'])
                        print("Stream %s has had a parsing error" % streams[s]['name'])
                        logger.error(str(e))
                        print(str(e))

    os.system('sort ' + file+'_temp | gzip > ' + file + '.gz')
    os.system('rm ' + file + '_temp')

    return True

def write_data_file_parallel(i):
    return write_data_file(i[0],i[1],i[2],i[3])


# In[5]:


beginning = datetime.datetime.now()
for user in users:
    if args.participant is not None:
        if args.participant != user['username']:
            continue

    if os.path.isfile(os.path.join(output_dir, user['username']) + '.complete'):
        print("User (%s) is already exported" % user['username'])
        continue

    os.makedirs(os.path.join(output_dir,user['username']),exist_ok=True)
    streams = CC.get_user_streams(user_id=uuid.UUID(user['identifier']))
    if len(streams) > 0:
        for s in sorted(streams):
            write_metadata_file(os.path.join(output_dir,user['username'],s+'.json'), streams[s])

        parallel_inputs = []
        for s in sorted(streams):
            parallel_inputs.append( (os.path.join(output_dir,user['username'],s+'.csv'), streams, user, s) )
            #write_data_file(os.path.join(output_dir,user['username'],s+'.csv'), streams, user, s)

        results = Parallel(n_jobs=args.num_jobs)(delayed(write_data_file_parallel)(i) for i in parallel_inputs)

    os.system('touch ' + os.path.join(output_dir, user['username']) + '.complete')

logger.info("Total Execution Time: %s" % str(datetime.datetime.now() - beginning))