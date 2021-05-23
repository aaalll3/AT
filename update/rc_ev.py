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
    prefile_pfx=dict()
    prefile_pfx_path=dict()
    prefile_pfx_type=dict()
    prefile_as=dict()
    prefile_as_path=dict()
    prefile_as_type=dict()
    pfx_result=defaultdict(int)
    pfx_ww_res=defaultdict(int)
    pfx_aa_res=defaultdict(int)
    pfx_aw_res=defaultdict(int)
    pfx_wa_res=defaultdict(int)
    as_result=defaultdict(int)
    as_ww_res=defaultdict(int)
    as_aa_res=defaultdict(int)
    as_aw_res=defaultdict(int)
    as_wa_res=defaultdict(int)
    pfx_aa_res_diff= defaultdict(int)
    as_aa_res_diff= defaultdict(int)
    pfx_wa_res_diff= defaultdict(int)
    as_wa_res_diff= defaultdict(int)
    for full_name in full_names:
        curfile_front_pfx=set()
        curfile_front_as=set()
        pfx_early=dict()
        as_early=dict()
        pfx_table=dict()
        as_table=dict()
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
            timestep = line[1]
            if len(line) > 6:
                asp = line[6].strip()
            else:
                continue
            ori = asp.split(' ')[-1]

            utype = line[2]

            # prefix update
            if not pfx in curfile_front_pfx:
                curfile_front_pfx.add(pfx)
                pt = prefile_pfx.get(pfx)
                if pt:
                    if '.' in pt or '.' in timestep:
                        if abs(int(float(pt)) - int(float(timestep))) <60:
                            pfx_result[pfx]+=1
                    else:
                        if abs(int(pt) - int(timestep)) <60:
                            pfx_result[pfx]+=1
            first_up_pfx = pfx_early.get(pfx)
            if not first_up_pfx:
                pfx_early[pfx]=timestep
            last_up_pfx = pfx_table.get(pfx)
            if last_up_pfx:
                if '.' in last_up_pfx or '.' in timestep:
                    if abs(int(float(last_up_pfx)) - int(float(timestep))) <60:
                        pfx_result[pfx]+=1
                        if prefile_pfx_type[pfx]=='W' and utype=='W':
                            pfx_ww_res[pfx]+=1
                        if prefile_pfx_type[pfx]=='A' and utype=='W':
                            pfx_aw_res[pfx]+=1
                        if prefile_pfx_type[pfx]=='A' and utype=='A':
                            pfx_aa_res[pfx]+=1
                            if asp != prefile_pfx_path[pfx]:
                                pfx_aa_res_diff[pfx]+=1
                        if prefile_pfx_type[pfx]=='W' and utype=='A':
                            pfx_wa_res[pfx]+=1
                            if asp != prefile_pfx_path[pfx]:
                                pfx_wa_res_diff[pfx]+=1
                else:
                    if abs(int(last_up_pfx) - int(timestep)) <60:
                        pfx_result[pfx]+=1
                        if prefile_pfx_type[pfx]=='W' and utype=='W':
                            pfx_ww_res[pfx]+=1
                        if prefile_pfx_type[pfx]=='A' and utype=='W':
                            pfx_aw_res[pfx]+=1
                        if prefile_pfx_type[pfx]=='A' and utype=='A':
                            pfx_aa_res[pfx]+=1
                            if asp != prefile_pfx_path[pfx]:
                                pfx_aa_res_diff[pfx]+=1
                        if prefile_pfx_type[pfx]=='W' and utype=='A':
                            pfx_wa_res[pfx]+=1
                            if asp != prefile_pfx_path[pfx]:
                                pfx_wa_res_diff[pfx]+=1

            pfx_table[pfx]=str(int(float(timestep)))
            
            # origin as update
            if not ori in curfile_front_as:
                curfile_front_as.add(ori)
                pt = prefile_as.get(ori)
                if pt:
                    if '.' in pt or '.' in timestep:
                        if abs(int(float(pt)) - int(float(timestep))) <60:
                            as_result[ori]+=1
                    else:
                        if abs(int(pt) - int(timestep)) <60:
                            as_result[ori]+=1
            first_up_as = as_early.get(ori)
            if not first_up_as:
                as_early[ori]=timestep
            last_up_as = as_table.get(ori)
            if last_up_as:
                if '.' in last_up_as or '.' in timestep:
                    if abs(int(float(last_up_as)) - int(float(timestep))) <60:
                        as_result[ori]+=1
                        if prefile_as_type[ori]=='W' and utype=='W':
                            as_ww_res[ori]+=1
                        if prefile_as_type[ori]=='A' and utype=='W':
                            as_aw_res[ori]+=1
                        if prefile_as_type[ori]=='A' and utype=='A':
                            as_aa_res[ori]+=1
                            if asp != prefile_as_path[ori]:
                                as_aa_res_diff[ori]+=1
                        if prefile_as_type[ori]=='W' and utype=='A':
                            as_wa_res[ori]+=1
                            if asp != prefile_as_path[ori]:
                                as_wa_res_diff[ori]+=1
                else:
                    if abs(int(last_up_as) - int(timestep)) <60:
                        as_result[ori]+=1
                        if prefile_as_type[ori]=='W' and utype=='W':
                            as_ww_res[ori]+=1
                        if prefile_as_type[ori]=='A' and utype=='W':
                            as_aw_res[ori]+=1
                        if prefile_as_type[ori]=='A' and utype=='A':
                            as_aa_res[ori]+=1
                            if asp != prefile_as_path[ori]:
                                as_aa_res_diff[ori]+=1
                        if prefile_as_type[ori]=='W' and utype=='A':
                            as_wa_res[ori]+=1
                            if asp != prefile_as_path[ori]:
                                as_wa_res_diff[ori]+=1
            as_table[ori]=str(int(float(timestep)))
            prefile_pfx_path[pfx] = asp
            prefile_as_path[ori] = asp
            prefile_pfx_type[pfx] = utype
            prefile_as_type[ori] = utype
        prefile_pfx = pfx_table
        prefile_as = as_table
        print(f'looked event {full_name} time{cur_time}')

    onename = full_names[0]
    org = onename.split('/')[-3]
    rc = onename.split('/')[-2]
    
    f = open(f'/home/lwd/Result/update/pfx_event_brc_{org}_{rc}.txt','w')
    for pfx,u_num in pfx_result.items():
        f.write(f'{pfx}|{u_num}|aa_{pfx_aa_res[pfx]}_diff_{pfx_aa_res_diff[pfx]}|aw_{pfx_aw_res[pfx]}|ww_{pfx_ww_res[pfx]}|wa_{pfx_wa_res[pfx]}_diff_{pfx_wa_res_diff[pfx]}')
        f.write('\n')
    f.close()
    f = open(f'/home/lwd/Result/update/as_event_brc_{org}_{rc}.txt','w')
    for ori,u_num in as_result.items():
        f.write(f'{ori}|{u_num}|aa_{as_aa_res[ori]}_diff_{as_aa_res_diff[ori]}|aw_{as_aw_res[ori]}|ww_{as_ww_res[ori]}|wa_{as_wa_res[ori]}_diff_{as_wa_res_diff[ori]}')
        f.write('\n')
    f.close()



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
    apool.map(use,go)
except Exception as e:
    print(repr(e))
    traceback.print_exc()
finally:
    p2 = time.time()
    print(f'tt done, takes {p2-p1}s')