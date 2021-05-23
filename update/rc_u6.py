import os
import multiprocessing
import time
from collections import defaultdict
import logging
import resource
from math import ceil
import re
import traceback

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = os.path.abspath(os.path.join('../log',f'log_lit_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)
patternv4=re.compile('^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)/(3[0-2]|[1-2][0-9]|[0-9])$')
def use(args):
    func = args[0]
    name = args[1]
    return func(name)




args=[]
res=[]
rcs=defaultdict(list)

def checkdir(dname,func,select):
    if type(dname)=='str':
        dname=os.path.join(os.path.abspath('..'),dname)
    fnames = os.listdir(dname)
    for fname in fnames:
        full_name = os.path.join(dname,fname)
        org = full_name.split('/')[-3]
        rc = full_name.split('/')[-2]
        tid = org+'/'+rc
        if os.path.isdir(full_name):
            checkdir(full_name,func,select) 
        else:
            if select(full_name):
                rcs[tid].append(full_name)
                # args.append([func,full_name])
                # res.append(func(full_name))

tt = 0
def at(full_name):
    global tt
    if full_name.endswith('dump'):
        tt+=1
        return True
    else:
        return False

time_bin=[
    '0[0-1]\d\d',
    '0[2-3]\d\d',
    '0[4-5]\d\d',
    '0[6-7]\d\d',
    '0[8-9]\d\d',
    '1[0-1]\d\d',
    '1[2-3]\d\d',
    '1[4-5]\d\d',
    '1[6-7]\d\d',
    '1[8-9]\d\d',
    '2[0-1]\d\d',
    '2[2-3]\d\d',
]

time_bin = [re.compile(tpattern) for tpattern in time_bin]

time_name = [
    '0001',
    '0203',
    '0405',
    '0607',
    '0809',
    '1011',
    '1213',
    '1415',
    '1617',
    '1819',
    '2021',
    '2223',
]

# store the event from last time interval
buf_time=None
buf_step=None
evetn_ori_buf = {}
evetn_pfx_buf = {}

def isRecent(timea,timeb):
    if abs(int(timea[2:]) - int(timeb[2:])) == 5 and int(timea[0:2]) == int(timeb[0:2])  :
        return True
    if int(timea[2:]) == 0 and int(timeb[2:]) == 55 and abs(int(timea[0:2]) - int(timeb[0:2])) ==1: 
        return True
    if int(timea[2:]) == 55 and int(timeb[2:]) == 00 and abs(int(timea[0:2]) - int(timeb[0:2])) ==1: 
        return True
    print(f'not recent {timea} {timeb}')
    return False
 
def lookevent(full_names):
    pfx_result=defaultdict(int)
    as_result=defaultdict(int)
    for full_name in full_names:
        last_name = full_name.split('/')[-1]
        cur_time = last_name.split('.')[2]
        org = full_name.split('/')[-3]
        rc = full_name.split('/')[-2]
        recent = False
        # if buf_time == None:
        #     buf_time = cur_time
        # else:
        #     recent =  isRecent(buf_time,cur_time) 
        f = open(full_name,'r')
        for line in f:
            # basic parse
            wh = line
            # print(wh)
            line = line.strip().split('|')
            pfx = line[5]
            if patternv4.match(pfx) is None:
                #go v6
                pass
            else:
                #go v4
                continue
            timestep = line[1]
            if len(line) > 6:
                asp = line[6].strip()
            else:
                continue
            ori = asp.split(' ')[-1]
            if '{' in asp:
                continue
            if '{' in ori:
                continue
            if '}' in ori:
                continue
            utype = line[2]

            # prefix update3127425640
            # /home/lwd/Update/2020-09-01/ripe/rrc22/updates.20200901.0310.dump
            pfx_result[pfx]+=1
            as_result[ori]+=1
        print(f'looked event {full_name} time{cur_time}')
    onename = full_names[0]
    org = onename.split('/')[-3]
    rc = onename.split('/')[-2]    
    f = open(f'/home/lwd/Result/update/pfx_up_brc_{org}_{rc}_v6.txt','w')
    for pfx,u_num in pfx_result.items():
        f.write(f'{pfx}|{u_num}')
        f.write('\n')
    f.close()
    f = open(f'/home/lwd/Result/update/as_up_brc_{org}_{rc}_v6.txt','w')
    for ori,u_num in as_result.items():
        if '{' in ori:
            continue
        f.write(f'{ori}|{u_num}')
        f.write('\n')
    f.close()

    return [pfx_result,as_result]



p1 = time.time()
upds = os.path.abspath('/home/lwd/Update/')
checkdir(upds,lookevent,at)
print('all append')
# num select
ns = 0
num = 300
args.sort(key= lambda path: path[1].split('/')[-1] )
total_args = args

def use(args):
    func = args[0]
    files = args[1]
    return func(files)

# for a,b in rcs.items():
#     lookevent(b)
# quit()
try:
    go = []
    for addr,file_list in rcs.items():
        go.append([lookevent,file_list])
    apool = multiprocessing.Pool(96)
    res = apool.map(use,go)
    # for hi in go:
    #     use(hi)
    pfxs = []
    ass = []
    for a in res:
        pfxs.append(a[0])
        ass.append(a[1])
    if True:
        pfx_r = dict()
        for p in pfxs:
            pfx_r.update(p)
        f = open(f'/home/lwd/Result/update/pfx_up_tt_v4.txt','w')
        for pfx,u_num in pfx_r.items():
            f.write(f'{pfx}|{u_num}')
            f.write('\n')
        f.close()
    if True:
        as_r = dict()
        for p in ass:
            as_r.update(p)
        f = open(f'/home/lwd/Result/update/as_up_tt_v4.txt','w')
        for ori,u_num in as_r.items():
            if '{' in ori:
                continue

            f.write(f'{ori}|{u_num}')
            f.write('\n')
        f.close()

except Exception as e:
    print(repr(e))
    traceback.print_exc()
finally:
    p2 = time.time()
    print(f'tt done, takes {p2-p1}s')