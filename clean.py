import os 
from os.path import join,abspath
import sqlite3



def mkfile(path):
    print(f'[mkfile]make file {path}')
    if os.path.exists(path):
        print(f'[mkfile]file exists. removing {path}')
        command = f'rm {path}'
        os.system(command)
    return open(path,'w')

def clean(path):
    out_path = mkfile(path+'.tmp')
    with open(path,'r') as file:
        for idx, line in enumerate(file):
            line = line.strip()
            parts = line.split('|')
            skip = False
            for part in parts:
                if '{' in part:
                    skip = True
                    break
                if not ASNAllocated(int(part)):
                    skip = True
                    break
                if checkIXP(part):
                    parts.remove(part)
            if skip:
                continue
            line = '|'.join(parts)
            out_path.write(line)
            out_path.write('\n')




ixp = set()
def readIXP(peeringdb_file) -> bool:
    if peeringdb_file.endswith('json'):
        with open(peeringdb_file) as f:
            data = json.load(f)
        for i in data['net']['data']:
            if i['info_type'] == 'Route Server':
                ixp.add(str(i['asn']))
        return True
    elif peeringdb_file.endswith('sqlite3'):
        conn = sqlite3.connect(peeringdb_file)
        c = conn.cursor()
        for row in c.execute("SELECT asn, info_type FROM 'peeringdb_network'"):
            asn, info_type = row
            if info_type == 'Route Server':
                ixp.add(str(asn))
        return True
    else:
        return False

def checkIXP(asn) -> bool:
    if asn in ixp:
        ixp_rule=True
        info=f'[checkIXP]asn {asn} is IXP and ruled out'
    else:
        info=f'[checkIXP]asn {asn} isn\'t IXP'
        ixp_rule=False
    return ixp_rule


def ASNAllocated(asn) -> bool:
    if asn == 23456 or asn == 0:
        proper_asn = False
    elif asn > 399260:
        proper_asn = False
    elif asn > 64495 and asn < 131072:
        proper_asn = False
    elif asn > 141625 and asn < 196608:
        proper_asn = False
    elif asn > 210331 and asn < 262144:
        proper_asn = False
    elif asn > 270748 and asn < 327680:
        proper_asn = False
    elif asn > 328703 and asn < 393216:
        proper_asn = False
    else:
        proper_asn = True
    return proper_asn

if __name__ == '__main__':
    readIXP('./peeringdb.sqlite3')
    names = os.listdir('../RIB.test/path.test/')
    names=[join(abspath('../RIB.test/path.test/'),name) for name in names if name.endswith('.path')]
    for name in names:
        tmp_name = name+'.tmp'
        new_name = name+'.clean'
        # if os.path.exists(new_name):
        #     continue
        clean(name)
        command = f'sort {tmp_name} | uniq > {new_name}'
        os.system(command)
        command = f'rm {tmp_name}'
        os.system(command)
        # os.system('perl ~/code/TopoScope.code/asrank.pl ~/RIB.test/path.test/pc20201201.v4.u.path.clean > ~/RIB.test/path.test/pc20201201.v4.arout')