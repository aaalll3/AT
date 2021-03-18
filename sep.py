import os 
import sys
import time
import logging
import multiprocessing
from os.path import abspath,join

ts=time.time()
log_path=join(abspath('./log'),f'split_{ts}.log')
logging.basicConfig(filename='',level=logging.DEBUG)

def mkfile(path):
    logging.debug(f'[mkfile]make file {path}')
    if os.path.exists(path):
        logging.debug(f'[mkfile]file exists. removing {path}')
        command = f'rm {path}'
        os.system(command)
    return open(path,'w')

def work(path):
    out_path = mkfile(path+'.path')
    out_community = mkfile(path+'.com')
    with open(path,'r') as file:
        for idx, line in enumerate(file):
            line = line.strip()
            parts = line.split('**')
            if len(parts) !=2:
                continue
            path = parts[0]
            community = parts[1]
            out_path.write(path)
            out_path.write('\n')
            out_community.write(community)
            out_community.write('\n')


if __name__=='__main__':
    uniq = True
    sep = True
    debug = False
    if uniq:
        names = os.listdir('../RIB.test/path/')
        names=[join(abspath('../RIB.test/path/'),name) for name in names if name.endswith('.v4') or name.endswith('v6')]
        for name in names:
            new_name = name+'.u'
            command = f'sort {name} | uniq >> {new_name}'
            os.system(command)
    if sep:
        names = os.listdir('../RIB.test/path/')
        arg_list=[join('../RIB.test/path/',name) for name in names if name.endswith('.u')]
        # print(arg_list)
        # with multiprocessing.Pool(4) as pool:
        #     pool.map(work, arg_list)
        for name in arg_list:
            work(name)
        # work('./testfolder/exfile.txt.v4')
    if debug:
        names = os.listdir('../RIB.test/path/')
        names=[join('../RIB.test/path/',name) for name in names if name.endswith('.v4') or name.endswith('v6')]
        print(names)

