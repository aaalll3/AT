import os
import multiprocessing
import logging
import resource
import time

from math import ceil

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = os.path.abspath(os.path.join('../log',f'log_lit_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)


def use(args):
    func = args[0]
    name = args[1]
    return func(name)


args=[]


update_loc=os.path.abspath('/home/xb/data/update_decompress/2020-09-01')

def checkdir(dname,func,select):
    if type(dname)=='str':
        dname=os.path.join(os.path.abspath('..'),dname)
    new_name = dname.split('/')
    new_name[2]='lwd'
    new_name[4]='Update'
    new_name.remove(new_name[3])
    target = '/'
    for name in new_name:
        target = os.path.join(target,name)
    if not os.path.exists(target):
        os.system(f'mkdir {target}')

    fnames = os.listdir(dname)
    for fname in fnames:
        full_name = os.path.join(dname,fname)
        if os.path.isdir(full_name):
            checkdir(full_name,func,select) 
        else:
            if select(full_name):
                args.append([func,full_name])
def at (_):
    return True

def deco(full_name):
    global num
    temp = full_name.split('/')[-1]
    new_name = full_name.split('/')
    new_name[2]='lwd'
    new_name[4]='Update'
    new_name.remove(new_name[3])
    target = '/'
    for name in new_name:
        target = os.path.join(target,name)
    os.system("bgpdump -m " + full_name + " > " + target + ".dump")
    os.system(f"echo \"bgpdump done\"")


checkdir(update_loc, deco, at)
# print(args)
updates_num = len(args)
p1 = time.time()
num=96
apool = multiprocessing.Pool(96)
for i in range(ceil(updates_num/num)):
    print(f"round{i}")
    start = i*num
    end = min((i+1)*num,updates_num)
    go = args[start:end]
    apool.map(use,go)

p2 = time.time()

print(f'takes {p2-p1}s')
