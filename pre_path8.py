
import logging
import os
import argparse
import re
import sys
import json
import multiprocessing
import sqlite3
from os.path import join
import time

stamp = time.time()
                                            #DATE
logging.basicConfig(filename=f'./log/log_{20201208}_{stamp}.log',level=logging.DEBUG)

class Extor:
    def __init__(self,target_path = None,save_path = None) -> None:
        self.ready=False
        if target_path is None or save_path is None:
            logging.debug('[init]debugging')
            self.outTPath=os.path.abspath('./testfolder/ext/log')
            return
        self.target_dir=target_path
        self.outTPath = save_path+'.te'
        self.outTerminal = self.mkfile(save_path+'.te')
        self.outv4 = self.mkfile(save_path+'.v4')
        self.outv6 = self.mkfile(save_path+'.v6')
        self.patternv4=re.compile('^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)/(3[0-2]|[1-2][0-9]|[0-9])$')
        self.ixp=set()
        peeringdb=os.path.abspath(os.path.join(target_path,'peeringdb.sqlite3'))
        if self.readIXP(peeringdb):
            logging.debug('[init]IXP loaded')
            self.ready=True
        else:
            logging.debug('[init]IXP failed to load')

    def mkfile(self, path):
        logging.debug(f'[mkfile]make file {path}')
        if os.path.exists(path):
            logging.debug(f'[mkfile]file exists. removing {path}')
            command = f'rm {path} >> {self.outTPath}'
            os.system(command)
        return open(path,'w')

    def obtain_file(self, path_file):
        logging.debug(f'[obtain_file]obtain file {path}')
        if os.path.exists(path_file):
            logging.debug(f'[obtain_file]file exists. loading {path}')
            return open(path_file,'a')
        else:
            logging.debug(f'[obtain_file]making file {path}')
            return self.mkfile(path_file)

    def work(self):
        target = self.target_dir
        self.checkdir(target,self.extract,self.select)

    def select(self,name):
        name=name.split('/')
        name=name[-1]
        name=name.split('.')
        if len(name)!=4:
            return False
        date=name[1]
        time=name[2]
                                        #DATE
        if time =='0000' and date == '20201208':
            return True
        else: 
            return False

    def checkdir(self,dname,func,select):
        if type(dname)=='str':
            dname=os.path.join(os.path.abspath('..'),dname)
            logging.debug(f'[checkdir]in root dir {dname}')
        else:
            logging.debug(f'[checkdir]in dir {dname}')
    
        fnames = os.listdir(dname)
        for fname in fnames:
            name = os.path.join(dname,fname)
            if os.path.isdir(name):
                # logging.debug(f'[checkdir]into {name}')
                self.checkdir(name,func,select)
            else:
                if select(name):
                    logging.debug(f'[checkdir]select file {name}')
                    func(name)
                else:
                    logging.debug(f'[checkdir]skip file {name}')

    def extract(self,dump_file)->bool:
        '''
        To extract BGP table from dumped file
        '''
        if dump_file.endswith('.dump') or dump_file.endswith('.read'):
            # path_file=dump_file[0:-5]+'.path'
            logging.debug(f'[extractor]processing {dump_file}')
        else:
            logging.debug('[extractor]not target')
            return False
        # if os.path.exists(path_file):
        #     os.system(f'rm {path_file}')
        # pf = open(path_file,'w')
        with open(dump_file,'r') as df:
            for idx,line in enumerate(df):
                try:
                    if idx == 0:
                        continue
                    if line.strip()=='':
                        continue
                    # logging.debug(f'[extractor]line {idx} :{line}')
                    parts = line.strip().split('|')
                    prefix = parts[5]
                    aspath = parts[6]
                    community = parts[11]
                    output_line = ''
                    aspath = aspath.strip().split(' ')

                    # input(f'C1 prefix: {prefix} aspath: {aspath} community: {community}')

                    # ruled out duplicated asn
                    aspath = [v for i, v in enumerate(aspath)
                                if i == 0 or v != aspath[i-1]]
                    # ruled out circle
                    as_set = set(aspath)
                    if len(as_set) == 1 or not len(aspath) == len(as_set):
                        continue

                    # input(f'C2 prefix: {prefix} aspath: {aspath} community: {community}')

                    for asn in aspath:
                        output_line += asn
                        output_line += '|'
                    output_line = output_line.strip('|')
                    output_line += f'**{community}'
                    output_line += '\n'
                    if self.patternv4.match(prefix) is None:
                        self.outv6.write(output_line)
                    else:
                        self.outv4.write(output_line)
                except Exception as e:
                    logging.debug('**!Reading error')
                    logging.debug(f'line{idx}: {line}')
                    logging.debug(e)

    def readIXP(self,peeringdb_file) -> bool:
        logging.debug('[readIXP]loading IXP info from peeringdb')
        if peeringdb_file.endswith('json'):
            logging.debug('[readIXP]find json file')
            with open(peeringdb_file) as f:
                data = json.load(f)
            for i in data['net']['data']:
                if i['info_type'] == 'Route Server':
                    self.ixp.add(str(i['asn']))
            return True

        elif peeringdb_file.endswith('sqlite3'):
            logging.debug(f'[readIXP]find sqlite3 file {peeringdb_file}')

            conn = sqlite3.connect(peeringdb_file)
            c = conn.cursor()
            for row in c.execute("SELECT asn, info_type FROM 'peeringdb_network'"):
                asn, info_type = row
                if info_type == 'Route Server':
                    self.ixp.add(str(asn))
            return True

        else:
            logging.debug('[readIXP]find no file')
            return False

    def checkIXP(self,asn) -> bool:
        if asn in self.ixp:
            ixp_rule=True
            info=f'[checkIXP]asn {asn} is IXP and ruled out'
        else:
            info=f'[checkIXP]asn {asn} isn\'t IXP'
            ixp_rule=False
        logging.debug(info)
        return ixp_rule

    @staticmethod
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
        if proper_asn:
            info = f'[ASNAllocated]asn {asn} is proper'
        else:
            info = f'[ASNAllocated]asn {asn}\'s ruled out'
        logging.debug(info)
        return proper_asn

if __name__ == '__main__':
    # test_folder = os.path.abspath('./testfolder/ext')
    # logging.debug('test')
    # #test mkfile & rm file
    # ex = Extor()
    # test_mk=join(test_folder,'mkthis.test')
    # test_rm=join(test_folder,'mkanother.test')
    # try:
    #     os.system(f'rm {test_mk}')
    #     os.system(f'touch {test_rm}')
    # except Exception as e:
    #     logging.debug(e)
    # ex.mkfile(join(test_folder,'mkthis.test'))
    # ex.mkfile(join(test_folder,'mkanother.test'))
    # # (25[0-5]|((2[0-4]|1[0-9]|[1-9])?[0-9]).){3}(25[0-5]|((2[0-4]|1[0-9]|[1-9])?[0-9]))/3[0-2]|([1-2]?[0-9])   
    # #test regular expression
    # p = re.compile(r'^(25[0-5]|((2[0-4]|1[0-9]|[1-9])?[0-9]).){3}(25[0-5]|((2[0-4]|1[0-9]|[1-9])?[0-9]))/(3[0-2]|([1-2]?[0-9]))$')
    # result=p.match(r'1.0.0.0/24')
    # logging.debug('match result')
    # logging.debug(result)
    # input('')
    #test reading rib



    debug_target = os.path.abspath('../RIB.test/')
                                                        #DATE
    debug_saving = os.path.abspath('../RIB.test/path/pc20201208')
    ex = Extor(debug_target,debug_saving)
    ex.work()
