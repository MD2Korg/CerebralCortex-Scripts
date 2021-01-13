from bz2 import BZ2File as bzopen
import json
import os
from datetime import datetime

log_files = []
conditions = []
mapping={}

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


def gen_usermapping():
    f = open('participant.txt','r')
    for l in f:
        if len(l.strip()):
            lsplts = l.strip().split(',')
            mapping[lsplts[1].strip()] = lsplts[0]


def generate_header_row(input_files):
    tmp_conditions = []
    for input_file in input_files:
        ff = os.path.basename(input_file)
        #userid = mapping[ff.split('+',1)[0]]
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

        tab = ','
        for x in groupedbuf:
            for cond in x:
                if 'status' in cond:
                    continue
                condition = cond['type'] + '-' + cond['id']
                if condition not in tmp_conditions:
                    tmp_conditions.append(condition)

    for cd in tmp_conditions:
        if 'VALID_BLOCK' in cd:
            conditions.append(cd)
    conditions.append('BLOCK')
    for cd in tmp_conditions:
        if 'VALID_BLOCK' not in cd:
            conditions.append(cd)
    #print(conditions)


def parse_log(input_file):
    ff = os.path.basename(input_file)
    #userid = mapping[ff.split('+',1)[0]]
    count = 0
    tmpbuf = []
    csvbuf = ''
    with bzopen(input_file, "rb") as bzfin:
        """ Handle lines here """

        starttime = -1

        for i, line in enumerate(bzfin):
            ln = line.decode().rstrip()
            lsplits = ln.split(',',2)
            try:
                ema_json = json.loads(lsplits[2][1:-1])
                tmpbuf.append(ema_json)

                #['status'hp, 'current_time', 'timestamp', 'id', 'logSchedule', 'message', 'type', 'operation']
            except Exception as e:
                print(e)
                print(lsplits[2][1:-1])

            count += 1

    groupedbuf = []
    tmp = []
    tmptmp = []
    srtts = -1
    ema_started = False
    for l in tmpbuf:
        #"type": "PRIVACY", "id": "PRIVACY"}
        #print(l)
        if 'type' in l and l['type'] == 'PRIVACY' and l['id'] == 'PRIVACY':
            #print('EMA_STARTED')
            ema_started = True
            if len(tmp):
                for aa in tmp:
                    tmptmp.append(aa)
                tmp = []
            #continue

        if ema_started:
            #if 'message' in l and l['message'] != 'false: some conditions are failed':
            #    if l['message'] != 'true: all conditions okay':
            tmp.append(l)

        if 'status' in l and (l['status'] == 'COMPLETED' or l['status'] == 'MISSED' or l['status'] == 'ABANDONED_BY_TIMEOUT'):
            #print('EMA_ENDED')
            ema_started = False
            if len(tmp):
                groupedbuf.append(tmp)
                tmp = []
            if len(tmptmp):
                tmptmp.append(l)
                groupedbuf.append(tmptmp)
                #print(tmptmp)
                tmptmp = []

        #if 'message' in l and l['message'] == 'false: some conditions are failed':
        if 'message' in l and 'false:' in l['message']:
            #print('EMA_ENDED_AAAAAAAAAAAAAAAAAAAA')
            ema_started = False
            if len(tmp):
                groupedbuf.append(tmp)
            tmp = []

    '''
    for x in groupedbuf:
        for y in x:
            print(y)
    '''

    tab = ','
    dup_list = []
    for x in groupedbuf:
        if not len(x): 
            continue
        
        csv_entry = ''#userid + tab
        if 'status' in x[-1]:
            csv_entry += x[0]['current_time'] + tab+ x[-1]['id'] + tab + x[-1]['status']
        else:
            tmpid = x[-1]['id']
            if 'EMA' not in tmpid:
                for y in x:
                    if 'EMA' in y['id']:
                        tmpid = y['id']
                        if 'VALID_BLOCK_' in tmpid:
                            tmpid = tmpid.split('VALID_BLOCK_')[1]
                        break
            else:
                if 'VALID_BLOCK_' in tmpid:
                    tmpid = tmpid.split('VALID_BLOCK_')[1]


            csv_entry += x[-1]['current_time'] + tab+ tmpid + tab + 'NOT_DELIVERED'
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
        if csv_entry not in dup_list:
            csvbuf += csv_entry
            dup_list.append(csv_entry)
        else:
            print('D'*50)

        #exit(1)

    return csvbuf


def get_study_day(current, start):
   d1 = datetime.strptime(start, "%Y/%m/%d")
   d2 = datetime.strptime(current, "%Y/%m/%d")
   return abs((d2 - d1).days)

def date_of_month(intime):
   return (datetime.fromtimestamp(int(intime)).strftime('%Y/%m/%d'))

def insert_unixtime(csvbuf):
    lines = csvbuf.split('\n')
    to_return = ''
    start_timestamp = -1
    for l in lines:
        if not len(l) : continue

        splits = l.split(',')
        timestamp = datetime.strptime(splits[0], '%Y/%m/%d %I:%M:%S %p').timestamp()
        if start_timestamp == -1:
            start_timestamp = timestamp
        study_day = get_study_day(date_of_month(timestamp), date_of_month(start_timestamp)) + 1
        new_l = str(timestamp) + ',' + str(study_day) + ',' + l + '\n'
        to_return += new_l
    return to_return

if __name__ == '__main__':
    gen_usermapping()

    base_dir = '/smb/md2k_lab/Master_Data/Rice'
    find_files(base_dir)
    
    #for f in [log_files[13]]:
    for f in log_files:
        generate_header_row([f])

        expanded_conditions = []
        for cd in conditions:
            if cd == 'BLOCK' : 
                expanded_conditions.append(cd)
                continue
            expanded_conditions.append(cd+'--STATUS')
            expanded_conditions.append(cd+'--CONDITION')
            

        csvbuf = ''
        tab = ','
        #csvbuf = 'userid' + tab + 'current_time' + tab + 'id' + tab + 'status'
        csvbuf = 'unixtime' + tab + 'study_day' + tab + 'current_time' + tab + 'id' + tab + 'status'
        for c in expanded_conditions:
            csvbuf += tab + c

        csvbuf += '\n'

        out_file_name = f.split('.csv.bz2')[0] + '+analysis.csv'
        fo = open(out_file_name,'w')
        fo.write(csvbuf)
    
        print('parsing',f, 'writing', out_file_name)
        csvbuf = parse_log(f)
        csvbuf = insert_unixtime(csvbuf)
        fo.write(csvbuf)

        fo.close()
        conditions = []
        #break
    
    print('DONE')
