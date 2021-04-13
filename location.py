import os
from os.path import join, abspath, exists

# nohup perl ./asrank_irr.pl --clique 174 209 286 701 1239 1299 2828 2914 3257 3320 3356 3491 5511 6453 6461 6762 6830 7018 12956 --filtered ~/RIB.test/path.test/pc20201201.v4.u.path.clean > /home/lwd/Result/auxiliary/pc20201201.v4.arout &


def checke(path):
    if exists(path):
        print(f'ready:{path}')
        return True
    else:
        print(f'not exists:{path}')
        return False


loc_raw_data_dir = abspath('/home/lwd/RIB.test/')
loc_result_dir = abspath('/home/lwd/Result/')

raw_path_dir = abspath('/home/lwd/RIB.test/path')
pure_path_dir = abspath('/home/lwd/RIB.test/path.test')
apwd = abspath('/home/lwd/Result/AP_working')
tswd = abspath('/home/lwd/Result/TS_working')
apvd = abspath('/home/lwd/Result/vote/apv')
tsvd = abspath('/home/lwd/Result/vote/tsv')
auxiliary = abspath('/home/lwd/Result/auxiliary')

v6vpd = abspath('/home/lwd/Result/v6vp')
v6vpg = abspath('/home/lwd/Result/vote/v6vpg')

irr_file='/home/lwd/Result/auxiliary/irr.txt'

s_dir='/home/lwd/RIB.test/path.test'  # source dir
r_dir='/home/lwd/Result'              # Result dir

rdir = '/home/lwd/Result'            # Result dir
vdir = "/home/lwd/Result/vote"