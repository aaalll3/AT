import os
import multiprocessing
import time
from collections import defaultdict
import logging
import resource
from math import ceil
import re


resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = os.path.abspath(os.path.join('../log',f'log_lit_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)

def use(args):
    func = args[0]
    name = args[1]
    return func(name)




args=[]
res=[]

def checkdir(dname,func,select):
    if type(dname)=='str':
        dname=os.path.join(os.path.abspath('..'),dname)
    fnames = os.listdir(dname)
    for fname in fnames:
        full_name = os.path.join(dname,fname)
        if os.path.isdir(full_name):
            checkdir(full_name,func,select) 
        else:
            if select(full_name):
                args.append([func,full_name])
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

def look(full_name):
    total=defaultdict(int)
    a2x=defaultdict(set)
    f = open(full_name,'r')
    for line in f:
        wh = line
        line = line.strip().split('|')
        pfx = line[5]
        timestep = line[2]
        if len(line) > 6:
            asp = line[6]
        else:
            continue
        ori = asp.split(' ')[-1]
        total[pfx]+=1
        a2x[ori].add(pfx)
    print(f'looked {full_name}')
    return [total,a2x]

# store the event from last time interval
buf_time=None
buf_step=None
evetn_ori_buf = {}
evetn_pfx_buf = {}

def isRecent(timea,timeb):
    if int(timea[2:3]) - int(timeb[2:3]) <7 and int(timea[0:1]) == int(timeb[0:1])  :
        return True
    if int(timea[2:3]) == 0 and int(timeb[2:3]) == 55 and int(timea[0:1]) - int(timeb[0:1]) <2: 
        return True
    if int(timea[2:3]) == 55 and int(timeb[2:3]) == 00 and int(timea[0:1]) - int(timeb[0:1]) <2: 
        return True
    return False
 
def lookevent(full_name):
    pfx_result=defaultdict(int)
    as_result=defaultdict(int)
    pfx_table=dict()
    as_table=dict()
    cur_time = full_name.split('.')[2]
    recent = False
    if buf_time == None:
        buf_time = cur_time
    else:
        recent =  isRecent(buf_time,cur_time) 
    f = open(full_name,'r')
    for line in f:
        wh = line
        line = line.strip().split('|')
        pfx = line[5]
        timestep = line[2]
        if len(line) > 6:
            asp = line[6]
        else:
            continue
        ori = asp.split(' ')[-1]
        last_up_pfx = pfx_table.get(pfx)
        last_up_as = as_table.get(ori)
        if last_up_pfx:
            if abs(int(last_up_pfx) - int(timestep)) <60:
                pfx_result[pfx]+=1
        if last_up_as:
            if abs(int(last_up_as) - int(timestep)) <60:
                pfx_result[ori]+=1
    print(f'looked event {full_name}')
    return [total,a2x]

p1 = time.time()
upds = os.path.abspath('/home/lwd/Update/')
checkdir(upds,look,at)
print('all append')
# num select
ns = 0
num = 300
args.sort(key= lambda path: path[1].split('/')[-1] )
total_args = args

for idx in range(len(time_bin)):
    try:
        bin = time_bin[idx]
        time_id = time_name[idx]
        go = []
        for name in total_args:
            filename = name[1].split('/')[-1]
            # print(filename)
            attime = filename.split('.')[2]
            if bin.fullmatch(attime):
                # print(f'matched {filename} at bin {attime}')
                go.append(name)

        print(go)
        res = []
        # aend = min(ns+num,len(total_args))
        # args = total_args[ns:aend]
        apool = multiprocessing.Pool(96)
        res+=apool.map(use,go)
        # for i in range(ceil(update_num/10)):
        #     start = i*10
        #     end = min((i+1)*10,update_num)
        #     go = args[start:end]
        #     apool = multiprocessing.Pool(96)
        #     res+=apool.map(use,go)
        print('returned')
    
        print(f'deal with result of {len(res)} files')
    
        f = open(f'/home/lwd/Result/update/pfxNum_mm{time_id}.txt','w')
        xnum = defaultdict(int)
        a2x_total = defaultdict(set)
        for ss in res:
            pfx_set = ss[0]
            a2x = ss[1]
            for pfx,u_num in pfx_set.items():
                xnum[pfx]+=u_num
            for ori,pfx in a2x.items():
                a2x_total[ori] = a2x_total[ori].union(pfx)
        for pfx,u_num in xnum.items():
            f.write(f'{pfx}:{u_num}')
            f.write('\n')
        f.close()
        f = open(f'/home/lwd/Result/update/ori2pfx_mm{time_id}.txt','w')
        for ori,pfx in a2x_total.items():
            # print(ori,end='@')
            f.write(f'{ori}@')
            for p in list(pfx):
                # print(p,end=' ')
                f.write(f'{p} ')
            f.write('\n')
        f.close()
        p2 = time.time()
        print(f'takes {p2-p1}s')
        ns+=num
    except Exception as e:
        print(repr(e))
        logging.exception(f'check {tt}')
    finally:
        pass