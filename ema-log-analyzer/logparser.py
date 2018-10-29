from bz2 import BZ2File as bzopen
import json
import os
from datetime import datetime

log_files = []

def find_files(basedir):
    subdirs = os.listdir(base_dir)
    user_dirs = []
    for d in subdirs:
        sdpath = os.path.join(base_dir,d)
        if os.path.isdir(sdpath):
            user_dirs.append(sdpath)

    for sd in user_dirs:
        ufs = os.listdir(sd)
        for f in ufs:
            if 'LOG' in f and 'bz2' in f:
                log_files.append(os.path.join(sd,f))


mapping={}
def gen_usermapping():
    f = open('mapping.txt','r')
    for l in f:
        if len(l.strip()):
            lsplts = l.strip().split(',')
            mapping[lsplts[1].strip()] = lsplts[0]


def parse_log(input_file):
    ff = os.path.basename(input_file)
    userid = mapping[ff.split('+',1)[0]]
    count = 0
    tmpbuf = []
    with bzopen(input_file, "rb") as bzfin:
        """ Handle lines here """

        starttime = -1

        for i, line in enumerate(bzfin):
            ln = line.decode().rstrip()
            lsplits = ln.split(',',2)
            try:
                ema_json = json.loads(lsplits[2][1:-1])
                tmpbuf.append(ema_json)

                #['status', 'current_time', 'timestamp', 'id', 'logSchedule', 'message', 'type', 'operation']
            except Exception as e:
                print(e)
                print(lsplits[2][1:-1])

            count += 1

    groupedbuf = []
    tmp = []
    srtts = -1
    ema_started = False
    for l in tmpbuf:
        if 'message' in l and l['message'] == 'true: datapoint not found':
            ema_started = True
            continue

        if ema_started:
            #if 'message' in l and l['message'] != 'false: some conditions are failed':
            #    if l['message'] != 'true: all conditions okay':
            tmp.append(l)

        if 'status' in l and (l['status'] == 'COMPLETED' or l['status'] == 'MISSED' or l['status'] == 'ABANDONED_BY_TIMEOUT'):
            ema_started = False
            groupedbuf.append(tmp)
            tmp = []

        if 'message' in l and l['message'] == 'false: some conditions are failed':
            ema_started = False
            if len(tmp):
                groupedbuf.append(tmp)
            tmp = []

    tab = '\t'
    tmp_conditions = []
    for x in groupedbuf:
        for cond in x:
            if 'status' in cond:
                continue
            condition = cond['type'] + '-' + cond['id']
            if condition not in tmp_conditions:
                tmp_conditions.append(condition)

    conditions = []
    for cd in tmp_conditions:
        if 'VALID_BLOCK' in cd:
            conditions.append(cd)
    conditions.append('BLOCK')
    for cd in tmp_conditions:
        if 'VALID_BLOCK' not in cd:
            conditions.append(cd)

    expanded_conditions = []

    for cd in conditions:
        if cd == 'BLOCK' : 
            expanded_conditions.append(cd)
            continue
        expanded_conditions.append(cd+'--STATUS')
        expanded_conditions.append(cd+'--CONDITION')
        


    csvbuf = 'userid' + tab + 'current_time' + tab + 'id' + tab + 'status'
    for c in expanded_conditions:
        csvbuf += tab + c

    csvbuf += '\n'

    for x in groupedbuf:
        if not len(x): continue
        csv_entry = userid + tab
        if 'status' in x[-1]:
            csv_entry += x[-1]['current_time'] + tab+ x[-1]['id'] + tab + x[-1]['status']
        else:
            csv_entry += x[-1]['current_time'] + tab+ x[-1]['id'] + tab + 'NOT_DELIVERED'
        allconds = {}
        for cond in x:
            if 'status' in cond:
                continue
            condition = cond['type'] + '-' + cond['id']
            
            allconds[condition] = cond['message']

        block = -1
        for acond in conditions:
            #print(acond)
            if acond in allconds:
                tmpstr = allconds[acond]
                if 'VALID_BLOCK' in acond:
                    splits = tmpstr.split(':',1)
                    csv_entry += tab + splits[0].strip() + tab + splits[1].strip()

                    blocks = splits[1].split('block(')
                    if len(blocks) > 1:
                        block = splits[1].split('block(')[1][0]
                else:
                    splits = tmpstr.split(':',1)
                    csv_entry += tab + splits[0].strip() + tab + splits[1].strip()  
            elif acond == 'BLOCK':
                csv_entry += tab + str(block)
                continue
            else:
                csv_entry += tab + tab 
            #print(repr(csv_entry))
        csv_entry += '\n'
        csvbuf += csv_entry
        #exit(1)

    '''
    fo = open('parsed.csv','w')        
    fo.write(csvbuf)
    fo.close()
    '''
    return csvbuf

if __name__ == '__main__':
    gen_usermapping()

    base_dir = '/smb/md2k_lab/Data/Rice'
    find_files(base_dir)

    fo = open('parsed.csv','w')
    
    for f in log_files:
        print('parsing',f)
        csvbuf = parse_log(f)
        fo.write(csvbuf)
        fo.write('\n\n')

    fo.close()
    print('DONE')
