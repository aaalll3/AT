# this file check the change over other attr in updates
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
    prefile_pfx=dict()
    prefile_pfx_path=dict()
    prefile_pfx_comm=dict()
    prefile_pfx_loc=dict()
    prefile_pfx_med=dict()
    prefile_pfx_aag=dict()
    prefile_pfx_ag=dict()
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
    pfx_aa_res_diff_comm= defaultdict(int)
    as_aa_res_diff_comm= defaultdict(int)
    pfx_wa_res_diff_comm= defaultdict(int)
    as_wa_res_diff_comm= defaultdict(int)
    pfx_aa_res_diff_loc= defaultdict(int)
    as_aa_res_diff_loc= defaultdict(int)
    pfx_wa_res_diff_loc= defaultdict(int)
    as_wa_res_diff_loc= defaultdict(int)
    pfx_aa_res_diff_med= defaultdict(int)
    as_aa_res_diff_med= defaultdict(int)
    pfx_wa_res_diff_med= defaultdict(int)
    as_wa_res_diff_med= defaultdict(int)
    pfx_aa_res_diff_aag= defaultdict(int)
    as_aa_res_diff_aag= defaultdict(int)
    pfx_wa_res_diff_aag= defaultdict(int)
    as_wa_res_diff_aag= defaultdict(int)
    pfx_aa_res_diff_ag= defaultdict(int)
    as_aa_res_diff_ag= defaultdict(int)
    pfx_wa_res_diff_ag= defaultdict(int)
    as_wa_res_diff_ag= defaultdict(int)
    idd2pfx=dict()
    idd2as=dict()
    idd2nh=dict()
    direction = defaultdict(set)
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
            idd = ''
            pfx = line[5]
            #vv6
            if patternv4.match(pfx) is None:
                #go v6
                continue
            else:
                #go v4
                pass
            timestep = line[1]
            asp=''
            if len(line) >13:
                asp = line[6].strip()
                nexthop = line[8].strip()
                localpref = line[9].strip()
                MED = line[10].strip()
                comm = line[11].strip()
                aag = line[12].strip()
                ag = line[13].strip()
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

            
            idd = pfx+nexthop+ori
            idd2as[idd]=ori
            idd2pfx[idd]=pfx
            idd2nh[idd]=nexthop
            direction[pfx].add(nexthop)
            # prefix update3127425640
            # /home/lwd/Update/2020-09-01/ripe/rrc22/updates.20200901.0310.dump
            if not idd in curfile_front_pfx:
                curfile_front_pfx.add(idd)
                pt = prefile_pfx.get(idd)
                if pt:
                    if '.' in pt or '.' in timestep:
                        if abs(int(float(pt)) - int(float(timestep))) <120:
                            pfx_result[idd]+=1
                    else:
                        # if abs(int(pt) - int(timestep)) <120:
                            pfx_result[idd]+=1
            first_up_pfx = pfx_early.get(idd)
            if not first_up_pfx:
                pfx_early[idd]=timestep
            last_up_pfx = pfx_table.get(idd)
            if last_up_pfx:
                if '.' in last_up_pfx or '.' in timestep:
                    if abs(int(float(last_up_pfx)) - int(float(timestep))) <120:
                        pfx_result[idd]+=1
                        if prefile_pfx_type[idd]=='W' and utype=='W':
                            pfx_ww_res[idd]+=1
                        if prefile_pfx_type[idd]=='A' and utype=='W':
                            pfx_aw_res[idd]+=1
                        if prefile_pfx_type[idd]=='A' and utype=='A':
                            pfx_aa_res[idd]+=1
                            if localpref != prefile_pfx_loc[idd]:
                                pfx_aa_res_diff_loc[idd]+=1
                            if localpref != prefile_pfx_loc[idd]:
                                pfx_aa_res_diff_loc[idd]+=1
                            if MED != prefile_pfx_med[idd]:
                                pfx_aa_res_diff_med[idd]+=1
                            if comm != prefile_pfx_comm[idd]:
                                pfx_aa_res_diff_comm[idd]+=1
                            if aag != prefile_pfx_aag[idd]:
                                pfx_aa_res_diff_aag[idd]+=1
                            if ag != prefile_pfx_ag[idd]:
                                pfx_aa_res_diff_aag[idd]+=1
                        if prefile_pfx_type[idd]=='W' and utype=='A':
                            pfx_wa_res[idd]+=1
                            if localpref != prefile_pfx_loc[idd]:
                                pfx_wa_res_diff_loc[idd]+=1
                            if localpref != prefile_pfx_loc[idd]:
                                pfx_wa_res_diff_loc[idd]+=1
                            if MED != prefile_pfx_med[idd]:
                                pfx_wa_res_diff_med[idd]+=1
                            if comm != prefile_pfx_comm[idd]:
                                pfx_wa_res_diff_comm[idd]+=1
                            if aag != prefile_pfx_aag[idd]:
                                pfx_wa_res_diff_aag[idd]+=1
                            if ag != prefile_pfx_ag[idd]:
                                pfx_wa_res_diff_aag[idd]+=1
                else:
                    if abs(int(last_up_pfx) - int(timestep)) <120:
                        pfx_result[idd]+=1
                        if prefile_pfx_type[idd]=='W' and utype=='W':
                            pfx_ww_res[idd]+=1
                        if prefile_pfx_type[idd]=='A' and utype=='W':
                            pfx_aw_res[idd]+=1
                        if prefile_pfx_type[idd]=='A' and utype=='A':
                            pfx_aa_res[idd]+=1
                            if localpref != prefile_pfx_loc[idd]:
                                pfx_aa_res_diff_loc[idd]+=1
                            if localpref != prefile_pfx_loc[idd]:
                                pfx_aa_res_diff_loc[idd]+=1
                            if MED != prefile_pfx_med[idd]:
                                pfx_aa_res_diff_med[idd]+=1
                            if comm != prefile_pfx_comm[idd]:
                                pfx_aa_res_diff_comm[idd]+=1
                            if aag != prefile_pfx_aag[idd]:
                                pfx_aa_res_diff_aag[idd]+=1
                            if ag != prefile_pfx_ag[idd]:
                                pfx_aa_res_diff_aag[idd]+=1
                        if prefile_pfx_type[idd]=='W' and utype=='A':
                            pfx_wa_res[idd]+=1
                            if localpref != prefile_pfx_loc[idd]:
                                pfx_wa_res_diff_loc[idd]+=1
                            if localpref != prefile_pfx_loc[idd]:
                                pfx_wa_res_diff_loc[idd]+=1
                            if MED != prefile_pfx_med[idd]:
                                pfx_wa_res_diff_med[idd]+=1
                            if comm != prefile_pfx_comm[idd]:
                                pfx_wa_res_diff_comm[idd]+=1
                            if aag != prefile_pfx_aag[idd]:
                                pfx_wa_res_diff_aag[idd]+=1
                            if ag != prefile_pfx_ag[idd]:
                                pfx_wa_res_diff_aag[idd]+=1

            pfx_table[idd]=str(int(float(timestep)))
            prefile_pfx_path[idd] = asp
            prefile_pfx_comm[idd]=comm
            prefile_pfx_loc[idd]=localpref
            prefile_pfx_med[idd]=MED
            prefile_pfx_aag[idd]=aag
            prefile_pfx_ag[idd]=ag
            prefile_pfx_type[idd] = utype
            # origin as update
            # if not ori in curfile_front_as:
            #     curfile_front_as.add(ori)
            #     pt = prefile_as.get(ori)
            #     if pt:
            #         if '.' in pt or '.' in timestep:
            #             if abs(int(float(pt)) - int(float(timestep))) <120:
            #                 as_result[ori]+=1
            #         else:
            #             if abs(int(pt) - int(timestep)) <120:
            #                 as_result[ori]+=1
            # first_up_as = as_early.get(ori)
            # if not first_up_as:
            #     as_early[ori]=timestep
            # last_up_as = as_table.get(ori)
            # if last_up_as:
            #     if '.' in last_up_as or '.' in timestep:
            #         if abs(int(float(last_up_as)) - int(float(timestep))) <120:
            #             as_result[ori]+=1
            #             if prefile_as_type[ori]=='W' and utype=='W':
            #                 as_ww_res[ori]+=1
            #             if prefile_as_type[ori]=='A' and utype=='W':
            #                 as_aw_res[ori]+=1
            #             if prefile_as_type[ori]=='A' and utype=='A':
            #                 as_aa_res[ori]+=1
            #                 if asp != prefile_as_path[ori]:
            #                     as_aa_res_diff[ori]+=1
            #             if prefile_as_type[ori]=='W' and utype=='A':
            #                 as_wa_res[ori]+=1
            #                 if asp != prefile_as_path[ori]:
            #                     as_wa_res_diff[ori]+=1
            #     else:
            #         if abs(int(last_up_as) - int(timestep)) <120:
            #             as_result[ori]+=1
            #             if prefile_as_type[ori]=='W' and utype=='W':
            #                 as_ww_res[ori]+=1
            #             if prefile_as_type[ori]=='A' and utype=='W':
            #                 as_aw_res[ori]+=1
            #             if prefile_as_type[ori]=='A' and utype=='A':
            #                 as_aa_res[ori]+=1
            #                 if asp != prefile_as_path[ori]:
            #                     as_aa_res_diff[ori]+=1
            #             if prefile_as_type[ori]=='W' and utype=='A':
            #                 as_wa_res[ori]+=1
            #                 if asp != prefile_as_path[ori]:
            #                     as_wa_res_diff[ori]+=1

            # as_table[ori]=str(int(float(timestep)))
            # prefile_as_path[ori] = asp
            # prefile_as_type[ori] = utype
        
        

        prefile_pfx = pfx_table
        # prefile_as = as_table
        print(f'looked event {full_name} time{cur_time}')

    onename = full_names[0]
    org = onename.split('/')[-3]
    rc = onename.split('/')[-2]    
    #vv6
    f = open(f'/home/lwd/Result/update/pfx_event_attr_brc_{org}_{rc}_v4.txt','w')
    as_result = defaultdict(int)
    wh_pfx_result=defaultdict(int)
    wh_pfx_ww_res=defaultdict(int)
    wh_pfx_aw_res=defaultdict(int)
    wh_pfx_aa_res=defaultdict(int)
    wh_pfx_aa_res_diff_loc=defaultdict(int)
    wh_pfx_aa_res_diff_med=defaultdict(int)
    wh_pfx_aa_res_diff_comm=defaultdict(int)
    wh_pfx_aa_res_diff_aag=defaultdict(int)
    wh_pfx_aa_res_diff_ag=defaultdict(int)
    wh_pfx_wa_res=defaultdict(int)
    wh_pfx_wa_res_diff_loc=defaultdict(int)
    wh_pfx_wa_res_diff_med=defaultdict(int)
    wh_pfx_wa_res_diff_comm=defaultdict(int)
    wh_pfx_wa_res_diff_aag=defaultdict(int)
    wh_pfx_wa_res_diff_ag=defaultdict(int)
    for idd,u_num in pfx_result.items():   
        pfx = idd2pfx[idd]
        wh_pfx_result[pfx]+=u_num
        wh_pfx_ww_res[pfx]+=pfx_ww_res[idd]
        wh_pfx_aw_res[pfx]+=pfx_aw_res[idd]
        wh_pfx_aa_res[pfx]+=pfx_aa_res[idd]
        wh_pfx_aa_res_diff_loc[pfx]+=pfx_aa_res_diff_loc[idd]
        wh_pfx_aa_res_diff_med[pfx]+=pfx_aa_res_diff_med[idd]
        wh_pfx_aa_res_diff_comm[pfx]+=pfx_aa_res_diff_comm[idd]
        wh_pfx_aa_res_diff_aag[pfx]+=pfx_aa_res_diff_aag[idd]
        wh_pfx_aa_res_diff_ag[pfx]+=pfx_aa_res_diff_ag[idd]
        wh_pfx_wa_res[pfx]+=pfx_wa_res[idd]
        wh_pfx_wa_res_diff_loc[pfx]+=pfx_wa_res_diff_loc[idd]
        wh_pfx_wa_res_diff_med[pfx]+=pfx_wa_res_diff_med[idd]
        wh_pfx_wa_res_diff_comm[pfx]+=pfx_wa_res_diff_comm[idd]
        wh_pfx_wa_res_diff_aag[pfx]+=pfx_wa_res_diff_aag[idd]
        wh_pfx_wa_res_diff_ag[pfx]+=pfx_wa_res_diff_ag[idd]
    for pfx,u_num in wh_pfx_result.items():
        wh_pfx_result[pfx]/=len(direction[pfx])
        wh_pfx_ww_res[pfx]/=len(direction[pfx])
        wh_pfx_aw_res[pfx]/=len(direction[pfx])
        wh_pfx_aa_res[pfx]/=len(direction[pfx])
        wh_pfx_aa_res_diff_loc[pfx]/=len(direction[pfx])
        wh_pfx_aa_res_diff_med[pfx]/=len(direction[pfx])
        wh_pfx_aa_res_diff_comm[pfx]/=len(direction[pfx])
        wh_pfx_aa_res_diff_aag[pfx]/=len(direction[pfx])
        wh_pfx_aa_res_diff_ag[pfx]/=len(direction[pfx])
        wh_pfx_wa_res[pfx]/=len(direction[pfx])
        wh_pfx_wa_res_diff_loc[pfx]/=len(direction[pfx])
        wh_pfx_wa_res_diff_med[pfx]/=len(direction[pfx])
        wh_pfx_wa_res_diff_comm[pfx]/=len(direction[pfx])
        wh_pfx_wa_res_diff_aag[pfx]/=len(direction[pfx])
        wh_pfx_wa_res_diff_ag[pfx]/=len(direction[pfx])

    for idd,u_num in pfx_result.items():   
        pfx = idd2pfx[idd]
        ori = idd2as[idd]     
        as_result[ori]+=wh_pfx_result[pfx]
        as_ww_res[ori]+=wh_pfx_ww_res[pfx]
        as_aw_res[ori]+=wh_pfx_aw_res[pfx]
        as_aa_res[ori]+=wh_pfx_aa_res[pfx]
        as_aa_res_diff_loc[ori]+=wh_pfx_aa_res_diff_loc[pfx]
        as_aa_res_diff_med[ori]+=wh_pfx_aa_res_diff_med[pfx]
        as_aa_res_diff_comm[ori]+=wh_pfx_aa_res_diff_comm[pfx]
        as_aa_res_diff_aag[ori]+=wh_pfx_aa_res_diff_aag[pfx]
        as_aa_res_diff_ag[ori]+=wh_pfx_aa_res_diff_ag[pfx]
        as_wa_res[ori]+=wh_pfx_wa_res[pfx]
        as_wa_res_diff_loc[ori]+=wh_pfx_wa_res_diff_loc[pfx]
        as_wa_res_diff_med[ori]+=wh_pfx_wa_res_diff_med[pfx]
        as_wa_res_diff_comm[ori]+=wh_pfx_wa_res_diff_comm[pfx]
        as_wa_res_diff_aag[ori]+=wh_pfx_wa_res_diff_aag[pfx]
        as_wa_res_diff_ag[ori]+=wh_pfx_wa_res_diff_ag[pfx]
        f.write(f'{pfx}|{wh_pfx_result[pfx]}|aa_{wh_pfx_aa_res[pfx]}_diffa_{wh_pfx_aa_res_diff_loc[pfx]}_{wh_pfx_aa_res_diff_med[pfx]}_{wh_pfx_aa_res_diff_comm[pfx]}_{wh_pfx_aa_res_diff_aag[pfx]}_{wh_pfx_aa_res_diff_ag[pfx]}|aw_{wh_pfx_aw_res[pfx]}|ww_{wh_pfx_ww_res[pfx]}|wa_{wh_pfx_wa_res[pfx]}_diffa_{wh_pfx_wa_res_diff_loc[pfx]}_{wh_pfx_wa_res_diff_med[pfx]}_{wh_pfx_wa_res_diff_comm[pfx]}_{wh_pfx_wa_res_diff_aag[pfx]}_{wh_pfx_wa_res_diff_ag[pfx]}')
        f.write('\n')
    f.close()
    #vv6
    f = open(f'/home/lwd/Result/update/as_event_attr_brc_{org}_{rc}_v4.txt','w')
    for ori,u_num in as_result.items():
        if '{' in ori:
            continue
        f.write(f'{ori}|{u_num}|aa_{as_aa_res[ori]}_diffa_{as_aa_res_diff_loc[ori]}_{as_aa_res_diff_med[ori]}_{as_aa_res_diff_comm[ori]}_{as_aa_res_diff_aag[ori]}_{as_aa_res_diff_ag[ori]}|aw_{as_aw_res[ori]}|ww_{as_ww_res[ori]}|wa_{as_wa_res[ori]}_diffa_{as_wa_res_diff_loc[ori]}_{as_wa_res_diff_med[ori]}_{as_wa_res_diff_comm[ori]}_{as_wa_res_diff_aag[ori]}_{as_wa_res_diff_ag[ori]}')
        f.write('\n')
    f.close()
    wh_pfx_aa_res_diff=[wh_pfx_aa_res_diff_loc,wh_pfx_aa_res_diff_med,wh_pfx_aa_res_diff_comm,wh_pfx_aa_res_diff_aag,wh_pfx_aa_res_diff_ag]
    wh_pfx_wa_res_diff=[wh_pfx_wa_res_diff_loc,wh_pfx_wa_res_diff_med,wh_pfx_wa_res_diff_comm,wh_pfx_wa_res_diff_aag,wh_pfx_wa_res_diff_ag]
    as_aa_res_diff=[as_aa_res_diff_loc,as_aa_res_diff_med,as_aa_res_diff_comm,as_aa_res_diff_aag,as_aa_res_diff_ag]
    as_wa_res_diff=[as_wa_res_diff_loc,as_wa_res_diff_med,as_wa_res_diff_comm,as_wa_res_diff_aag,as_wa_res_diff_ag]
    return [wh_pfx_result,wh_pfx_aa_res,wh_pfx_aa_res_diff,wh_pfx_wa_res,wh_pfx_wa_res_diff,wh_pfx_aw_res,wh_pfx_ww_res,
            as_result, as_aa_res, as_aa_res_diff, as_wa_res, as_wa_res_diff, as_aw_res, as_ww_res]



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
        pfxs.append(a[0:7])
        ass.append(a[7:])
    if True:
        pfx_r = dict()
        pfx_aa= defaultdict(int)
        pfx_aa_d_loc= defaultdict(int)
        pfx_aa_d_med= defaultdict(int)
        pfx_aa_d_comm= defaultdict(int)
        pfx_aa_d_aag= defaultdict(int)
        pfx_aa_d_ag= defaultdict(int)
        pfx_wa= defaultdict(int)
        pfx_wa_d_loc= defaultdict(int)
        pfx_wa_d_med= defaultdict(int)
        pfx_wa_d_comm= defaultdict(int)
        pfx_wa_d_aag= defaultdict(int)
        pfx_wa_d_ag= defaultdict(int)
        pfx_aw= defaultdict(int)
        pfx_ww= defaultdict(int)
        for p in pfxs:
            pfx_r.update(p[0])
            pfx_aa.update(p[1])
            pfx_aa_d_loc.update(p[2][0])
            pfx_aa_d_med.update(p[2][1])
            pfx_aa_d_comm.update(p[2][2])
            pfx_aa_d_aag.update(p[2][3])
            pfx_aa_d_ag.update(p[2][4])
            pfx_wa.update(p[3])
            pfx_wa_d_loc.update(p[4][0])
            pfx_wa_d_med.update(p[4][1])
            pfx_wa_d_comm.update(p[4][2])
            pfx_wa_d_aag.update(p[4][3])
            pfx_wa_d_ag.update(p[4][4])
            pfx_aw.update(p[5])
            pfx_ww.update(p[6])
            #vv6
        f = open(f'/home/lwd/Result/update/pfx_event_attr_av_v4.txt','w')
        for pfx,u_num in pfx_r.items():
            f.write(f'{pfx}|{u_num}|aa_{pfx_aa[pfx]}_diffa_{pfx_aa_d_loc[pfx]}_{pfx_aa_d_med[pfx]}_{pfx_aa_d_comm[pfx]}_{pfx_aa_d_aag[pfx]}_{pfx_aa_d_ag[pfx]}|aw_{pfx_aw[pfx]}|ww_{pfx_ww[pfx]}|wa_{pfx_wa[pfx]}_diffa_{pfx_wa_d_loc[pfx]}_{pfx_wa_d_med[pfx]}_{pfx_wa_d_comm[pfx]}_{pfx_wa_d_aag[pfx]}_{pfx_wa_d_ag[pfx]}')
            f.write('\n')
        f.close()

    if True:
        as_r = dict()
        as_aa= defaultdict(int)
        as_aa_d_loc= defaultdict(int)
        as_aa_d_med= defaultdict(int)
        as_aa_d_comm= defaultdict(int)
        as_aa_d_aag= defaultdict(int)
        as_aa_d_ag= defaultdict(int)
        as_wa= defaultdict(int)
        as_wa_d_loc= defaultdict(int)
        as_wa_d_med= defaultdict(int)
        as_wa_d_comm= defaultdict(int)
        as_wa_d_aag= defaultdict(int)
        as_wa_d_ag= defaultdict(int)
        as_aw= defaultdict(int)
        as_ww= defaultdict(int)
        for p in ass:
            as_r.update(p[0])
            as_aa.update(p[1])
            as_aa_d_loc.update(p[2][0])
            as_aa_d_med.update(p[2][1])
            as_aa_d_comm.update(p[2][2])
            as_aa_d_aag.update(p[2][3])
            as_aa_d_ag.update(p[2][4])
            as_wa.update(p[3])
            as_wa_d_loc.update(p[4][0])
            as_wa_d_med.update(p[4][1])
            as_wa_d_comm.update(p[4][2])
            as_wa_d_aag.update(p[4][3])
            as_wa_d_ag.update(p[4][4])
            as_aw.update(p[5])
            as_ww.update(p[6])
            #vv6
        f = open(f'/home/lwd/Result/update/as_event_attr_av_v4.txt','w')
        for ori,u_num in as_r.items():
            if '{' in ori:
                continue

            f.write(f'{ori}|{u_num}|aa_{as_aa[ori]}_diffa_{as_aa_d_loc[pfx]}_{as_aa_d_med[pfx]}_{as_aa_d_comm[pfx]}_{as_aa_d_aag[pfx]}_{as_aa_d_ag[pfx]}|aw_{as_aw[ori]}|ww_{as_ww[ori]}|wa_{as_wa[ori]}_diffa_{as_wa_d_loc[pfx]}_{as_wa_d_med[pfx]}_{as_wa_d_comm[pfx]}_{as_wa_d_aag[pfx]}_{as_wa_d_ag[pfx]}')
            f.write('\n')
        f.close()

except Exception as e:
    print(repr(e))
    traceback.print_exc()
finally:
    p2 = time.time()
    print(f'tt done, takes {p2-p1}s')