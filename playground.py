from collections import defaultdict
import os
import multiprocessing
import time
import logging
log_location = os.path.abspath(os.path.join('./log',f'log_playground_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)

# def use(args):
#     a=set()
#     a.add('1')
#     return [a,{'a':a}]



# args=[1,2,3]

# start = time.time()
# upds = os.path.abspath('/home/lwd/Update/')
# apool = multiprocessing.Pool(96)
# res = apool.map(use,args)
# see = set()

# asdf = defaultdict(set)
# print(res)
# for ss in res:  
#     see = see.union(ss[0])
#     for k,v in ss[1].items():
#         print(v)
#         print(asdf[k])
#         asdf[k] = asdf[k].union(v)
# print(see)

# for k,v in asdf.items():
#     print(k)
#     print(v)
# end = time.time()
# 1/0
# print(f'takes {end-start}s')

def isRecent(timea,timeb):
    if abs(int(timea[2:3]) - int(timeb[2:3])) == 5 and int(timea[0:1]) == int(timeb[0:1]):
        print('1')
        print(abs(int(timea[2:3]) - int(timeb[2:3])))
        print(int(timea[0:1]))
        print(int(timeb[2:3]))
        return True
    if int(timea[2:3]) == 0 and int(timeb[2:3]) == 55 and abs(int(timea[0:1]) - int(timeb[0:1])) ==1: 
        print('2')
        return True
    if int(timea[2:3]) == 55 and int(timeb[2:3]) == 00 and abs(int(timea[0:1]) - int(timeb[0:1])) ==1: 
        print('3')
        return True
    print(f'not recent {timea} {timeb}')
    return False
a = '0000'
b = '0050'
print(int(a[0:1]) - int(b[0:1]))

print(int(a[2:3]) - int(b[2:3]))

print(isRecent(a,b))