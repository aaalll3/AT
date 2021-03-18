import os 
from os.path import join,abspath



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
            if skip:
                continue
            line = '|'.join(parts)
            out_path.write(line)
            out_path.write('\n')

if __name__ == '__main__':
    names = os.listdir('../RIB.test/path.test/')
    names=[join(abspath('../RIB.test/path.test/'),name) for name in names if name.endswith('.path')]
    for name in names:
        clean(name)
        tmp_name = name+'.tmp'
        new_name = name+'.clean'
        command = f'sort {tmp_name} | uniq >> {new_name}'
        os.system(command)
        command = f'rm {tmp_name}'
        os.system(command)
        os.system('perl ~/code/TopoScope.code/asrank.pl ~/RIB.test/path.test/pc20201201.v4.u.path.clean > ~/RIB.test/path.test/pc20201201.v4.arout')