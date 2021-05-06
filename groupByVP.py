import os
import re
import logging
import resource
import networkx as nx
import time
import numpy as np

from itertools import permutations
from random import shuffle
from collections import defaultdict
from collections import Counter
from os.path import abspath, join, exists

from location import *

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = abspath(join('./log',f'log_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)


class groupByVP():
    def __init__(self,path_file=None,boost_file=None,irr_file=None) -> None:
        # base structure of graph
        self.whole_size=0
        self.clique = set(['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956'])
        self.tier_1 = ['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
        self.tier_1_v6 = ['174', '1299', '3356', '6057', '6939', '9002', '24482', '35280', '37468', '39533']
        self.clique_v6 = set(['174', '1299', '3356', '6057', '6939', '9002', '24482', '35280', '37468', '39533'])
        self.high = set()
        self.low = set()
        self.stub = set()
        self.provider = defaultdict(set)
        self.customer = defaultdict(set)
        self.peer = defaultdict(set)
        self.graph = nx.Graph()
        # Toposcope
        self.VP2AS = defaultdict(set)
        self.VP2path = defaultdict(set)
        self.fullVP = set()
        self.partialVP = set()
        self.pre_VP =set()
        self.sec_VP = set()
        self.VPGroup = list()
        self.file_num = None
        self.group_dir = None

        self.group_size=25
        # Apollo
        self.link_relation=dict()
        self.link_rel_c2f=dict()
        self.non_tier_1 =[]
        self.wrong_path = []
        # IRR
        self.irr_link = set()
        self.irr_c2p = set()
        self.irr_p2p = set()
        # File information
        self.boost_file=boost_file
        self.path_file=path_file
        self.irr_file=irr_file

    #boost file
    def get_relation(self, boost_file):
        with open(boost_file,'r') as file:
            for line in file:
                if not line.startswith('#'):
                    line=line.strip()
                    [asn1,asn2,rel]=line.split('|')[:3]
                    self.graph.add_edge(asn1,asn2)
                    if rel == '-1':
                        self.customer[asn1].add(asn2)
                        self.provider[asn2].add(asn1)
                    elif rel == '0' or rel == '1':
                        self.peer[asn1].add(asn2)
                        self.peer[asn2].add(asn1)

    def cal_hierarchy(self,version=4):
        allNodes = set()
        theclique=None
        if version == 4:
            theclique=self.clique
        else:
            theclique=self.clique_v6
        for node in theclique:
            for cus in self.customer[node]:
                if self.graph.degree(cus) > 100:
                    self.high.add(cus)
                    allNodes.add(cus)
            allNodes.add(node)
        for node in self.graph.nodes():
            if node in allNodes:
                continue
            if not self.customer[node]:
                self.stub.add(node)
            else:
                self.low.add(node)
            allNodes.add(node)
        self.whole_size=len(allNodes)
        print(f'whole:{self.whole_size}')
        print(f'clique:{len(theclique)}')
        print(f'high:{len(self.high)}')
        print(f'low:{len(self.low)}')
        print(f'stub:{len(self.stub)}')
    
    def check_hierarchy(self,asn,version=4):
        theclique=None
        if version == 4:
            theclique=self.clique
        else:
            theclique=self.clique_v6
        if asn in theclique:
            return 0
        elif asn in self.high:
            return 1
        elif asn in self.low:
            return 3
        else:
            return -1

    # path file, AP_out
    def set_VP_type(self,path_file,version=4):
        theclique=None
        if version == 4:
            theclique=self.clique
        else:
            theclique=self.clique_v6
        with open(path_file) as f:
            for line in f:
                ASes = line.strip().split('|')
                for AS in ASes:
                    self.VP2AS[ASes[0]].add(AS)
                self.VP2path[ASes[0]].add(line.strip())
                # self.process_line_AP(ASes) # Apollo
        for VP in self.VP2AS.keys():
            #V6SET
            if len(self.VP2AS[VP]) > self.whole_size*0.5:
                self.fullVP.add(VP)
                if VP in theclique or VP in self.high:
                    self.pre_VP.add(VP)
                else:
                    self.sec_VP.add(VP)
            else:
                self.partialVP.add(VP)
        print([len(self.pre_VP),len(self.sec_VP),len(self.partialVP)])

    # irr_file checked
    def read_irr(self,irr_path):
        with open(irr_path,'r') as f:
            lines = f.readlines()
        for line in lines:
            tmp = re.split(r'[\s]+',line)
            if tmp[2] == '1':
                self.irr_c2p.add((tmp[0],tmp[1]))
            if tmp[2] == '0':
                self.irr_p2p.add((tmp[0],tmp[1]))
            if tmp[2] == '-1':
                self.irr_c2p.add((tmp[1],tmp[0]))

    # path_file ar_version
    def boost(self,ar_version,path_file,dst = None):
        name = path_file.split('.')[0]
        if dst is None:
            dst = name+'.rel'
        command= f'perl {ar_version} {path_file} > {dst}'
        os.system(command)
        return dst

    def clean_vp(self,dir):
        os.system(f'rm {dir}/*')

    # TS divided file
    def divide_VP(self,group_size,dir,date):
        pre_VP = list(self.pre_VP)
        sec_VP = list(self.sec_VP)
        partial_VP = list(self.partialVP)
        # shuffle the premier VP, second VP, and the rest
        pre_num = len(pre_VP)
        sec_num = len(sec_VP)
        pre_order = [i for i in range(pre_num)]
        sec_order = [i for i in range(sec_num)]
        shuffle(pre_order)
        shuffle(sec_order)
        for i in range(int(pre_num/group_size)):
            self.VPGroup.append(pre_VP[i:i+group_size])
        for i in range(int(sec_num/group_size)):
            self.VPGroup.append(sec_VP[i:i+group_size])
        pre_rest = pre_num%group_size
        sec_rest = sec_num%group_size
        if pre_rest + sec_rest > group_size:
            w_partial = pre_rest + sec_rest - group_size
            if sec_rest < w_partial:
                self.VPGroup.append(pre_VP[-pre_rest:-w_partial] + sec_VP[-sec_rest:])
                self.VPGroup.append(pre_VP[-w_partial:] + partial_VP)
            else:
                self.VPGroup.append(pre_VP[-pre_rest:] + sec_VP[-sec_rest:-w_partial])
                self.VPGroup.append(sec_VP[-w_partial:] + partial_VP)

        else:
            self.VPGroup.append(pre_VP + sec_VP + partial_VP)
        #FULL
        for i in range(len(self.VPGroup)):
            wf_name = join(dir,f'path_{date}_vp{i}.path')
            wf = open(wf_name,'w')
            for VP in self.VPGroup[i]:
                for path in self.VP2path[VP]:
                    wf.write(path + '\n')
            wf.close()
  