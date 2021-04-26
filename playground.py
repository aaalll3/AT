from collections import defaultdict
import os
import multiprocessing
import time
import logging
log_location = os.path.abspath(os.path.join('./log',f'log_playground_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)

def use(args):
    a=set()
    a.add('1')
    return [a,{'a':a}]



args=[1,2,3]

start = time.time()
upds = os.path.abspath('/home/lwd/Update/')
apool = multiprocessing.Pool(96)
res = apool.map(use,args)
see = set()

asdf = defaultdict(set)
print(res)
for ss in res:  
    see = see.union(ss[0])
    for k,v in ss[1].items():
        print(v)
        print(asdf[k])
        asdf[k] = asdf[k].union(v)
print(see)

for k,v in asdf.items():
    print(k)
    print(v)
end = time.time()
1/0
print(f'takes {end-start}s')