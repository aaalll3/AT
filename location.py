import os
from os.path import join, abspath, exists


def checke(path):
    if exists(path):
        print(f'ready:{path}')
    else:
        print(f'not exists:{path}')

raw_path_dir = abspath('/home/lwd/RIB.test/path')
pure_path_dir = abspath('/home/lwd/RIB.test/path.test')
apwd = abspath('/home/lwd/Result/AP_working')
tswd = abspath('/home/lwd/Result/TS_working')
auxiliary = abspath('/home/lwd/Result/auxiliary')