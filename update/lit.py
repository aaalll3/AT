import os
import multiprocessing
import time
from collections import defaultdict
import logging
import resource
from math import ceil


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

p1 = time.time()
upds = os.path.abspath('/home/lwd/Update/')
checkdir(upds,look,at)
print('all append')
# num select
ns = 0
num = 300
args.sort()
total_args = args
for i in range(ceil(len(total_args)/num)):
    try:
        res = []
        aend = min(ns+num,len(total_args))
        args = total_args[ns:aend]
        apool = multiprocessing.Pool(96)
        res+=apool.map(use,args)
        # for i in range(ceil(update_num/10)):
        #     start = i*10
        #     end = min((i+1)*10,update_num)
        #     go = args[start:end]
        #     apool = multiprocessing.Pool(96)
        #     res+=apool.map(use,go)
        print('returned')
    
        print(f'deal with result of {len(res)} files')
    
        f = open(f'/home/lwd/Result/update/pfx_n{aend}.txt','w')
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
        f = open(f'/home/lwd/Result/update/ori2pfx_n{aend}.txt','w')
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