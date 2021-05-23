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
    if abs(int(timea[2:]) - int(timeb[2:])) == 5 and int(timea[0:2]) == int(timeb[0:2])  :
        return True
    if int(timea[2:]) == 0 and int(timeb[2:]) == 55 and abs(int(timea[0:2]) - int(timeb[0:2])) ==1: 
        return True
    if int(timea[2:]) == 55 and int(timeb[2:]) == 00 and abs(int(timea[0:2]) - int(timeb[0:2])) ==1: 
        return True
    print(f'not recent {timea} {timeb}')
    return False
 
def lookevent(full_name):
    pfx_result=defaultdict(int)
    as_result=defaultdict(int)
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
            asp = line[6]
        else:
            continue
        ori = asp.split(' ')[-1]

        # prefix update
        first_up_pfx = pfx_early.get(pfx)
        if not first_up_pfx:
            pfx_early[pfx]=timestep
        last_up_pfx = pfx_table.get(pfx)
        if last_up_pfx:
            if '.' in last_up_pfx or '.' in timestep:
                if abs(int(float(last_up_pfx)) - int(float(timestep))) <60:
                    pfx_result[pfx]+=1
                    pfx_table[pfx]=str(int(float(pfx_table[pfx])))
            else:
                if abs(int(last_up_pfx) - int(timestep)) <60:
                    pfx_result[pfx]+=1
        pfx_table[pfx]=str(int(float(timestep)))

        # origin as update
        first_up_as = as_early.get(ori)
        if not first_up_as:
            as_early[ori]=timestep
        last_up_as = as_table.get(ori)
        if last_up_as:
            if '.' in last_up_as or '.' in timestep:
                if abs(int(float(last_up_as)) - int(float(timestep))) <60:
                    as_result[ori]+=1
                    as_table[ori]=str(int(float(pfx_table[pfx])))
            else:
                if abs(int(last_up_as) - int(timestep)) <60:
                    as_result[ori]+=1
        as_table[ori]=str(int(float(timestep)))

    print(f'looked event {full_name} time{cur_time}')
    if len(cur_time)>5:
        print("alarm")
        print(cur_time)
        print(full_name)
    return [pfx_result,as_result,pfx_early,as_early,pfx_table,as_table,org,rc,cur_time]

p1 = time.time()
upds = os.path.abspath('/home/lwd/Update/')
checkdir(upds,lookevent,at)
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
        # print(go)
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
    
        xnum = defaultdict(int)
        as_xnum = defaultdict(int)

        pfx_tt_ear = defaultdict(dict)
        pfx_tt_lat = defaultdict(dict)
        as_tt_ear =defaultdict(dict)
        as_tt_lat =defaultdict(dict)
        for ss in res:
            org = ss[-3]
            rc = ss[-2]
            cur_time = ss[-1]
            tid = org+'/'+ rc +'/'+ cur_time
            pfx_ev = ss[0]
            for pfx,u_num in pfx_ev.items():
                xnum[pfx]+=u_num
            pfx_ear = ss[2]
            pfx_lat = ss[4]
            pfx_tt_ear[tid].update(pfx_ear)
            pfx_tt_lat[tid].update(pfx_lat)
        
            as_ev = ss[1]
            for ori,u_num in as_ev.items():
                as_xnum[ori] +=u_num
            as_ear = ss[3]
            as_lat = ss[5]
            as_tt_ear[tid].update(as_ear)
            as_tt_lat[tid].update(as_lat)
        def rcRecent(tid1,tid2):
            tid1 = tid1.split('/')
            tid2 = tid2.split('/')
            if tid1[0] == tid2[0] and tid1[1] == tid2[1] and isRecent(tid1[2],tid2[2]):
                return True
            else:
                return False
        # p1 = time.time()
        # for eid,edd in pfx_tt_ear.items():
        #     for lid,ldd in pfx_tt_lat.items():
        #         if not rcRecent(eid,lid):
        #             continue
        #         else:
        #             print(f'watching {eid} {lid}')
        #             print(f'pending {len(edd)}x{len(ldd)}')
        #         for epfx,estep in edd.items():
        #             for lpfx,lstep in ldd.items():
        #                 if epfx==lpfx and abs(int(float(estep))-int(float(lstep))) < 60:
        #                     xnum[epfx]+=1
        # for epfx,estep in pfx_tt_ear.items():
        #     for lpfx,lstep in pfx_tt_lat.items():
        #         if epfx==lpfx and abs(int(estep)-int(lstep)) < 60:
        #             xnum[epfx]+=1
        # p2 = time.time()
        # print(f'pfx event seam takes {p2-p1}s ')
        # for eid,edd in as_tt_ear.items():
        #     for lid,ldd in as_tt_lat.items():
        #         if not rcRecent(eid,lid):
        #             continue
        #         else:
        #             print(f'watching {eid} {lid}')
        #             print(f'pending {len(edd)}x{len(ldd)}')
        #         for eas,estep in edd.items():
        #             for las,lstep in ldd.items():
        #                 if eas==las and abs(int(float(estep))-int(float(lstep))) < 60:
        #                     as_xnum[eas]+=1
        # for eas,estep in as_tt_ear.items():
        #     for las,lstep in as_tt_lat.items():
        #         if eas==las and abs(int(estep)-int(lstep)) < 60:
        #             as_xnum[eas]+=1
        # p3 = time.time()
        # print(f'pfx event seam takes {p3-p2}s ')
            
        f = open(f'/home/lwd/Result/update/pfx_event_mm{time_id}.txt','w')
        for pfx,u_num in xnum.items():
            f.write(f'{pfx}|{u_num}')
            f.write('\n')
        f.close()
        f = open(f'/home/lwd/Result/update/as_event_mm{time_id}.txt','w')
        for ori,u_num in as_xnum.items():
            f.write(f'{ori}|{u_num}')
            f.write('\n')
        f.close()

        # f = open(f'/home/lwd/Result/update/pfx_tt_ear_mm{time_id}.txt','w')
        # for pfx,tstep in pfx_tt_ear.items():
        #     f.write(f'{pfx}>{tstep}')
        #     f.write('\n')
        # f.close()
        # f = open(f'/home/lwd/Result/update/pfx_tt_lat_mm{time_id}.txt','w')
        # for pfx,tstep in pfx_tt_lat.items():
        #     f.write(f'{pfx}>{tstep}')
        #     f.write('\n')
        # f.close()

        # f = open(f'/home/lwd/Result/update/as_tt_ear_mm{time_id}.txt','w')
        # for ori,tstep in as_tt_ear.items():
        #     f.write(f'{ori}>{tstep}')
        #     f.write('\n')
        # f.close()
        # f = open(f'/home/lwd/Result/update/as_tt_lat_mm{time_id}.txt','w')
        # for ori,tstep in as_tt_lat.items():
        #     f.write(f'{ori}>{tstep}')
        #     f.write('\n')
        # f.close()


        p2 = time.time()
        print(f'takes {p2-p1}s')
        ns+=num
    except Exception as e:
        print(repr(e))
        traceback.print_exc()
        logging.exception(f'check {tt}')
    finally:
        pass
quit()
update_dir = os.path.abspath('/home/lwd/Result/update/')
files = os.listdir(update_dir)
early_file=[]
late_file=[]
event_file=[]
for name in files:
    find_e = re.match(r'pfx_tt_ear_mm.*',name)
    find_l = re.match(r'pfx_tt_lat_mm.*',name)
    find_ev = re.match(r'pfx_event_mm.*',name)
    if find_e:
        early_file.append(os.path.join(update_dir,name))
    if find_l:
        late_file.append(os.path.join(update_dir,name))
    if find_ev:
        event_file.append(os.path.join(update_dir,name))
f = open(os.path.join(update_dir,'pfx_event_total.txt'),'w')
total_dict = defaultdict(int)
for name in event_file:
    rrf = open(os.path.join(update_dir,name))
    for line in rrf:
        line = line.strip().split('>')
        pfx = line[0]
        num = int(line[1])
        total_dict[pfx]+=num
