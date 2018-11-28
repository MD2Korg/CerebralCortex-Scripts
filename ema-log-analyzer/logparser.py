from bz2 import BZ2File as bzopen
import json
import os
from datetime import datetime
import sys

log_files = []
conditions = []

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


def generate_header_row(input_files):
    tmp_conditions = []
    for input_file in input_files:
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
    userid = mapping[ff.split('+',1)[0]]
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

    tab = '\t'
    dup_list = []
    for x in groupedbuf:
        if not len(x): continue
        csv_entry = userid + tab
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

            if 'message' not in cond:
                #print('message missing:', cond)
                allconds[condition] = cond['emiInfo']
            else:
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
                    try:
                        splits = tmpstr.split(':',1)
                        csv_entry += tab + splits[0].strip() + tab + splits[1].strip()  
                    except:
                        csv_entry += tab + "" + tab + str(tmpstr)
                        
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
            #print('D'*50)
            pass

        #exit(1)

    return csvbuf

if __name__ == '__main__':
    gen_usermapping()

    base_dir = sys.argv[1]
    find_files(base_dir)
    #log_files = ['/smb/md2k_lab/Data/Rice/1f879d60-3ccc-3b7a-b522-cbfbea277a9d/1f879d60-3ccc-3b7a-b522-cbfbea277a9d+10505+org.md2k.ema_scheduler+LOG+PHONE.csv.bz2']
    #log_files = ['1f879d60-3ccc-3b7a-b522-cbfbea277a9d+10505+org.md2k.ema_scheduler+LOG+PHONE.csv.bz2']
    generate_header_row(log_files)

    expanded_conditions = []
    for cd in conditions:
        if cd == 'BLOCK' : 
            expanded_conditions.append(cd)
            continue
        expanded_conditions.append(cd+'--STATUS')
        expanded_conditions.append(cd+'--CONDITION')
        

    csvbuf = ''
    tab = '\t'
    csvbuf = 'userid' + tab + 'current_time' + tab + 'id' + tab + 'status'
    for c in expanded_conditions:
        csvbuf += tab + c

    csvbuf += '\n'

    fo = open('all_users_parsed.csv','w')
    fo.write(csvbuf)
    
    for f in log_files:
        print('parsing',f)
        csvbuf = parse_log(f)
        fo.write(csvbuf)

    fo.close()
    print('DONE')
