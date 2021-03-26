import os
import sys
import re
import logging
import resource
import networkx as nx
import json
import pandas as pd
import time
import numpy as np
import copy, math, operator, random, scipy, sqlite3
import matplotlib.pyplot as plt
import lightgbm as lgb

from itertools import permutations
from random import shuffle
from collections import defaultdict
from collections import Counter
from os.path import abspath, join, exists
from networkx.algorithms.centrality import group
from TopoScope.topoFusion import TopoFusion
from logging import warn,debug,info
from sklearn import preprocessing
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neighbors import NearestNeighbors
from sklearn.model_selection import train_test_split

from rib_to_read import url_form, download_rib, worker, unzip_rib
from location import *


# TODO
# logging|done
# path of files
# debug

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = abspath(join('./log',f'log_{time.time()}'))
# log_location =''
logging.basicConfig(filename=log_location,level=logging.INFO)



# To make dataset
class Download():
    def __init__(self) -> None:
        pass

# To infer
class Run():
    def __init__(self, struc=None,infer=None,voter=None,prob=None) -> None:
        self.struc=struc
        self.infer=infer
        self.voter=voter
        self.prob=prob

    def run(self):
        if self.struc is None or \
            self.infer is None or \
            self.voter is None or \
            self.prob is None:
            logging.warn('Struc, Infer, Voter and Prob is needed')
            return

# TS: boost > get_relation > cal_hierarchy > set_VP_type > divide_TS > infer_TS > Vote_TS
# AP: read irr > process_line_AP > write_AP > vote_AP > infer_AP

# first stage: ASrank/C2L
#     TS_F:boost > get_realation > cal_hierarchy > set_VP_type > divide_TS > infer_TS
#     AP_F:read irr > process_line_AP > write_AP
# Second stage: vote in three way
#     TS_S:infer_TS > vote_TS
#     AP_S:vote_AP > infer_AP

# source dir: RIB.test/path
# source file: pc{date}.path.v4 or pc{date}.path.v6
### pc stanfs for path & community

# Apollo working dir: Result/AP_work
# Apollo working file: rel_{date}.st1
# Apollo working file for wrong path: rel_{date}.wrn

# TocoScope working dir: Result/TS_work
# TopoScope boost file: boost_{date}.ar
# TopoScope mid path: path_{date}_vp{idx}.path
# TopoScope working file: rel_{date}_vp{idx}.ar
# TopoScope 

class Struc():
    def __init__(self,path_file=None,boost_file=None,irr_file=None) -> None:
        # base structure of graph
        debug('[Struc.init]initializing',stack_info=True)
        self.clique = set(['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956'])
        self.tier_1 = ['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
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
        #TODO
        # self.dir=None

        info('[Struc.init]loading informations')

    #boost file
    def get_relation(self, boost_file):
        debug('[Struc.get_relation]',stack_info=True)
        info('[Struc.get_relation]TS: load initial infer for hierarchy')
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

    #
    def cal_hierarchy(self):
        debug('[Struc.cal_hierarchy]',stack_info=True)
        info('[Struc.cal_hieracrchy]TS: discrime different type of ASes')
        allNodes = set()
        for node in self.clique:
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
    
    def check_hierarchy(self,asn):
        info(f'[Struc.check_hierarchy]TS: find results of hierarchy for {asn}')
        if asn in self.clique:
            return 0
        elif asn in self.high:
            return 1
        elif asn in self.low:
            return 3
        else:
            return -1

    # path file, AP_out
    def set_VP_type(self,path_file):
        debug('[Struc.set_VP_type]',stack_info=True)
        info('[Struc.set_VP_type]set VP type for TS and run c2f for AP')
        with open(path_file) as f:
            for line in f:
                ASes = line.strip().split('|')
                for AS in ASes:
                    self.VP2AS[ASes[0]].add(AS)
                self.VP2path[ASes[0]].add(line.strip())
                # self.process_line_AP(ASes) # Apollo
        for VP in self.VP2AS.keys():
            if len(self.VP2AS[VP]) > 65000*0.8:
                self.fullVP.add(VP)
                if VP in self.clique or VP in self.high:
                    self.pre_VP.add(VP)
                else:
                    self.sec_VP.add(VP)
            else:
                self.partialVP.add(VP)
        # self.write_AP(AP_stage1_file)  # part1 over for Apollo


    def process_line_AP(self,ASes):
        # info(f'[Struc.process_line_AP]AP: process AS path {ASes}')
        for i in range(len(ASes)-1):
            if(ASes[i],ASes[i+1]) in self.irr_c2p:
                self.link_relation.setdefault((ASes[i],ASes[i+1]),set()).add(1)
            if(ASes[i+1],ASes[i]) in self.irr_c2p:
                self.link_relation.setdefault((ASes[i],ASes[i+1]),set()).add(-1)
            if(ASes[i],ASes[i+1]) in self.irr_p2p or (ASes[i+1],ASes[i]) in self.irr_p2p:
                self.link_relation.setdefault((ASes[i],ASes[i+1]),set()).add(0)
            if ASes[i] in self.tier_1 and ASes[i+1] in self.tier_1:
                self.link_relation.setdefault((ASes[i],ASes[i+1]),set()).add(0)
        idx = -1
        cnt = 0
        for i in range(len(ASes)):
            if ASes[i] in self.tier_1:
                idx = i
                cnt+=1
        if cnt>=2 and ASes[idx-1] not in self.tier_1:
            self.wrong_path.append(ASes)
            return
        if idx>=2:
            for i in range(idx-1):
                self.link_relation.setdefault((ASes[i],ASes[i+1]),set()).add(1)
        if idx<=len(ASes)-2 and cnt>0:
            for i in range(idx-1):
                self.link_relation.setdefault((ASes[i],ASes[i+1]),set()).add(1)
        if idx == -1:
            self.non_tier_1.append(ASes)


    # AP c2l out/w out
    def write_AP(self, AP_stage1_file,wp_file):
        debug('[Struc.write_AP]',stack_info=True)
        info('[Struc.write_AP]AP: iteration and output c2f result')
        for it in range(5):
            for asn in self.non_tier_1:
                idx_11 = 0
                idx_1 = 0
                idx_0 = 0
                for i in range(len(asn)-1):
                    if (asn[i],asn[i+1]) in self.link_relation.keys() \
                        and list(self.link_relation[(asn[i],asn[i+1])]) == [-1]:
                        idx_11 = i
                    if (asn[i],asn[i+1]) in self.link_relation.keys() \
                        and list(self.link_relation[(asn[i],asn[i+1])]) == [0]:
                        idx_0 = i
                    if (asn[i],asn[i+1]) in self.link_relation.keys() \
                        and list(self.link_relation[(asn[i],asn[i+1])]) == [1]:
                        idx_1 = i
                if idx_11 !=0:
                    for i in range(idx_1+1,len(asn)-1):
                        self.link_relation.setdefault((asn[i],asn[i+1]),set()).add(-1)
                if idx_1 !=0:
                    for i in range(idx_0-1):
                        self.link_relation.setdefault((asn[i],asn[i+1]),set()).add(1)
                if idx_0 !=0:
                    if idx_0>=2:
                        for i in range(idx_0-1):
                            self.link_relation.setdefault((asn[i],asn[i+1]),set()).add(1)
                    if idx_0<=len(asn)-2:
                        for i in range(idx_0+1,len(asn)-1):
                            self.link_relation.setdefault((asn[i],asn[i+1]),set()).add(-1)
        
            p2c_cnt = 0
            c2p_cnt = 0
            p2p_cnt = 0
            dulp = 0
            for k,v in self.link_relation.items():
                if len(v)>=2:
                    dulp +=1
                    continue
                if 1 in v:
                    c2p_cnt +=1
                if 0 in v:
                    p2p_cnt +=1
                if -1 in v:
                    p2c_cnt +=1
            print(p2c_cnt)
            print(c2p_cnt)
            print(p2p_cnt)
            print(dulp)
        result = dict()
        conflict = 0
        for k,v in self.link_relation.items():
            if len(v)>1 and 0 in v:
                result[k] = 0
            elif len(v) == 1:
                result[k] = list(v)[0]
            else:
                conflict +=1
        print('conflict: ' + str(conflict))
        #TODO
        #dst
        print('saving')
        # wp_file = join(AP_stage1_file,'wrong')
        f = open(AP_stage1_file,'w')
        f.write(str(result))
        f.close()
        f = open(wp_file,'w')
        f.write(str(self.wrong_path))
        f.close()

    def apollo(self,path_file,AP_stage1_file,wp_file):
        debug('[Struc.apollo]',stack_info=True)
        info('[Struc.apollo]run c2f for AP')
        with open(path_file) as f:
            for line in f:
                ASes = line.strip().split('|')
                self.process_line_AP(ASes) # Apollo
        self.write_AP(AP_stage1_file,wp_file)  # part1 over for Apollo


    def core2leaf(self, path_files, output_file):
        """
        A easy core to leaf infer with irr

        a|b|r 
        r has three values
        -1 for p2c
        1 for c2p
        0 for p2p
        4 for confilct link
        """
        link_rel_c2f = dict()
        for path_file in path_files:
            pf = open(path_file,'r')
            for line in pf:
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
                prime_t1 = 10000
                for i in range(len(ASes)-1):
                    if prime_t1 <= i-2:
                        rel = link_rel_c2f.setdefault((ASes[i],ASes[i+1]),-1)
                        if rel != -1:
                            link_rel_c2f[(ASes[i],ASes[i+1])] = 4
                        continue
                    if(ASes[i],ASes[i+1]) in self.irr_c2p:
                        link_rel_c2f.setdefault((ASes[i],ASes[i+1]),1)
                    if(ASes[i+1],ASes[i]) in self.irr_c2p:
                        link_rel_c2f.setdefault((ASes[i],ASes[i+1]),-1)
                    if(ASes[i],ASes[i+1]) in self.irr_p2p or (ASes[i+1],ASes[i]) in self.irr_p2p:
                        link_rel_c2f.setdefault((ASes[i],ASes[i+1]),0)
                    if ASes[i] in self.tier_1:
                        if prime_t1 == i-1:
                            link_rel_c2f.setdefault((ASes[i-1],ASes[i]),0)
                        prime_t1 = i
                    # if ASes[i] in self.tier_1 and ASes[i+1] in self.tier_1:
                    #     self.link_rel_c2f.setdefault((ASes[i],ASes[i+1]),0)
            pf.close()
        wf = open(output_file,'w')
        for link,rel in link_rel_c2f.items():
            if rel != 4:
                line = f'{link[0]}|{link[1]}|{rel}\n'
                wf.write(line)
        wf.close()

    #6125
    # notes for c2l+apollo vote
    # two of method is used, "Struc.apollo_it" and "Struc.vote_ap"
    # in parts "simple", firstly, runs the "apollo_it"
    # then, in parts "vote", runs the "vote_ap"
    # validation is made in file "./compare.py"
    #
    # used to give basic infer
    # first run the core2leaf for paths which contain the tier1 AS
    # then go through rest path for 5 times (following apollo) \
    # to infer paths that contain already infered link
    def apollo_it(self, path_files, output_file):
        """
        core to leaf followed by iterations
        """    
        link_rel_ap = dict()
        non_t1 =list()
        for path_file in path_files:
            pf = open(path_file,'r')
            for line in pf:
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
                prime_t1 = 10000
                for i in range(len(ASes)-1):
                    if prime_t1 <= i-2:
                        rel = link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                        if rel != -1:
                            link_rel_ap[(ASes[i],ASes[i+1])] = 4
                        continue
                    if(ASes[i],ASes[i+1]) in self.irr_c2p:
                        link_rel_ap.setdefault((ASes[i],ASes[i+1]),1)
                    if(ASes[i+1],ASes[i]) in self.irr_c2p:
                        link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                    if(ASes[i],ASes[i+1]) in self.irr_p2p or (ASes[i+1],ASes[i]) in self.irr_p2p:
                        link_rel_ap.setdefault((ASes[i],ASes[i+1]),0)
                    if ASes[i] in self.tier_1:
                        if prime_t1 == i-1:
                            link_rel_ap.setdefault((ASes[i-1],ASes[i]),0)
                        prime_t1 = i
                    if prime_t1 == 10000:
                        non_t1.append(ASes)
                    # if ASes[i] in self.tier_1 and ASes[i+1] in self.tier_1:
                    #     link_rel_ap.setdefault((ASes[i],ASes[i+1]),0)
            pf.close()
        for turn in range(5):
            for ASes in non_t1:
                idx_11 = 0
                idx_1 = 0
                idx_0 = 0
                for i in range(len(ASes)-1):
                    if (ASes[i],ASes[i+1]) in link_rel_ap.keys() \
                        and link_rel_ap[(ASes[i],ASes[i+1])] == -1:
                        idx_11 = i
                    if (ASes[i],ASes[i+1]) in link_rel_ap.keys() \
                        and link_rel_ap[(ASes[i],ASes[i+1])] == 0:
                        idx_0 = i
                    if (ASes[i],ASes[i+1]) in link_rel_ap.keys() \
                        and link_rel_ap[(ASes[i],ASes[i+1])] == 1:
                        idx_1 = i
                if idx_11 !=0:
                    for i in range(idx_1+1,len(ASes)-1):
                        rel = link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                        if rel != -1:
                            link_rel_ap[(ASes[i],ASes[i+1])]=4
                if idx_1 !=0:
                    for i in range(idx_0-1):
                        rel=link_rel_ap.setdefault((ASes[i],ASes[i+1]),1)
                        if rel != 1:
                            link_rel_ap[(ASes[i],ASes[i+1])]=4
                if idx_0 !=0:
                    if idx_0>=2:
                        for i in range(idx_0-1):
                            rel = link_rel_ap.setdefault((ASes[i],ASes[i+1]),1)
                            if rel != 1:
                                link_rel_ap[(ASes[i],ASes[i+1])]=4
                    if idx_0<=len(ASes)-2:
                        for i in range(idx_0+1,len(ASes)-1):
                            link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                            if rel != -1:
                                link_rel_ap[(ASes[i],ASes[i+1])]=4
        wf = open(output_file,'w')
        for link,rel in link_rel_ap.items():
            if rel != 4:
                line = f'{link[0]}|{link[1]}|{rel}\n'
                wf.write(line)
        wf.close()

    def clear(self,path,out):
        f = open(path,'r')
        links = []
        link = None
        for line in f:
            line = line.strip()
            parts = line.split('|')
            if len(parts)<3:
                if link is None:
                    link = [parts[0],parts[1]]
                else:
                    link.append(parts[1])
                    links.append(link)
                    link = None
            else:
                links.append(parts)
        f.close()
        o = open(out,'w')
        for link in links:
            o.write('|'.join(link)+'\n')

    # irr_file checked
    def read_irr(self,irr_path):
        debug('[Struc.read_irr]',stack_info=True)
        info('[Struc.read_irr]read irr info')
        with open(irr_path,'r') as f:
            lines = f.readlines()
        for line in lines:
            # tmp = line.strip().split('|')
            tmp = re.split(r'[\s]+',line)
            if tmp[2] == '1':
                self.irr_c2p.add((tmp[0],tmp[1]))
            if tmp[2] == '0':
                self.irr_p2p.add((tmp[0],tmp[1]))
            if tmp[2] == '-1':
                self.irr_c2p.add((tmp[1],tmp[0]))

    # path_file ar_version
    def boost(self,ar_version,path_file):
        debug('[Struc.boost]',stack_info=True)
        info('[Struc.boost]run initial asrank for TS')
        name = path_file.split('.')[0]
        dst = name+'.rel'
        command= f'perl {ar_version} {path_file} > {dst}'
        os.system(command)
        return dst

    # TS divided file
    def divide_TS(self,group_size,dir,date):
        debug('[Struc.divide_TS]',stack_info=True)
        info(f'[Struc.divide_TS]divide VP into {group_size} groups for voting')
        group_cnt = 0
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
            self.VPGroup.append(pre_VP[i:i+25])
        for i in range(int(sec_num/group_size)):
            self.VPGroup.append(sec_VP[i:i+25])
        pre_rest = pre_num%group_size
        sec_rest = sec_num&group_size
        if pre_rest + sec_rest > group_size:
            w_partial = pre_rest + sec_rest - group_size
            self.VPGroup.append(sec_VP[-sec_rest:] + pre_VP[-pre_rest:-w_partial])
            self.VPGroup.append(pre_VP[-w_partial:] + partial_VP)
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
    
    #FULL
    def infer_TS(self,dir,ar_version,date):
        debug('[Struc.infer_TS]',stack_info=True)
        info('[Struc.infer_TS]run asrank for seperated group')
        for i in range(len(self.VPGroup)):
            src_name = abspath(join(dir,f'path_{date}_vp{i}.path'))
            dst_name = abspath(join(dir,f'rel_{date}_vp{i}.ar'))
            command = f"perl {ar_version} {src_name} > {dst_name}"
            os.system(command)


    # path_file, peeringdb file, AP vote out
    def prepare_AP(self,path_file,peeringdb_file,AP_stage1_file):
        debug('[Struc.infer_AP]',stack_info=True)
        info('[Struc.infer_AP]infer AP')
        #TODO
        #need a normal path
        # paths = BgpPaths()
        # paths.extract_ixp(ixp_file)
        # paths.parse_bgp_paths('./sanitized_rib.txt')
        links = set()
        f = open(path_file,'r')
        for line in f:
            if '|' in line:
                ASes = line.split("|")
                for i in range(len(ASes)-1):
                    if (ASes[i], ASes[i+1]) not in links:
                        links.add((ASes[i], ASes[i+1]))
                for i in range(len(ASes),0,-1):
                    if (ASes[i], ASes[i-1]) not in links:
                        links.add((ASes[i], ASes[i-1]))
        tier1s = [ '174', '209', '286', '701', '1239', '1299', '2828', '2914', '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
        g = nx.Graph()
        for link in links:
            g.add_edge(link[0], link[1])
        shortest_distance = defaultdict(dict)
        shortest_distance_list = defaultdict(list)
        for tier1_asn in tier1s:
            if tier1_asn not in g:
                tier1s.remove(tier1_asn)
            else:
                p = nx.shortest_path_length(g, source=tier1_asn)
                for k, v in p.items():
                    if k not in shortest_distance or tier1_asn not in shortest_distance[k]:
                        shortest_distance[k][tier1_asn] = v
                        shortest_distance_list[k].append(v)
        node_neighbor_cnt = {}
        for k,v in g.degree():
            node_neighbor_cnt[k] = v
        node_fea = pd.DataFrame.from_dict(node_neighbor_cnt,orient= 'index',columns= ['degree'])
        for k,v in shortest_distance_list.items():
            node_fea.loc[k,'distance'] =int(sum(shortest_distance_list[k])/float(len(shortest_distance_list[k])))
        up_distance_cnt = {}
        down_distance_cnt = {}
        same_distance_cnt= {}
        for k in g.nodes():
            up_cnt = 0
            down_cnt = 0
            same_cnt = 0
            for i in g[k]:
                if i not in node_fea.index:
                    continue
                if node_fea.loc[k,'distance']-node_fea.loc[i,'distance'] == -1:
                    up_cnt +=1
                if node_fea.loc[k,'distance']-node_fea.loc[i,'distance'] == 1:
                    down_cnt +=1
                if node_fea.loc[k,'distance']-node_fea.loc[i,'distance'] == 0:
                    same_cnt +=1
            up_distance_cnt[k] = up_cnt
            down_distance_cnt[k] = down_cnt
            same_distance_cnt[k] = same_cnt
        for k,v in up_distance_cnt.items():
            node_fea.loc[k,'up-distance'] =v
        for k,v in down_distance_cnt.items():
            node_fea.loc[k,'down-distance'] =v
        for k,v in same_distance_cnt.items():
            node_fea.loc[k,'same-distance'] =v
        #每个link被多少个vp观测到
        vp_cnt = {}
        for line in f:
            if '|' in line:
                ASes = line.split("|")
                vp = ASes[0]
                for i in range(len(ASes)-1):
                    if (ASes[i], ASes[i+1]) not in vp_cnt:
                        vp_cnt[(ASes[i], ASes[i+1])] = set()
                        vp_cnt[(ASes[i+1], ASes[i])] = set()
                    vp_cnt[(ASes[i], ASes[i+1])].add(vp)
                    vp_cnt[(ASes[i+1], ASes[i])].add(vp)
        for link in vp_cnt:
            vp_cnt[link] = len(vp_cnt[link])

        #ixp
        ixp_dict = {}
        colocated_ixp = defaultdict(int)
        # PeeringDB json dump
        if peeringdb_file.endswith('json'):
            with open(peeringdb_file) as f:
                data = json.load(f)
            for i in data['netixlan']['data']:
                AS, ixp = i['asn'], i['ixlan_id']
                if ixp not in ixp_dict:
                    ixp_dict[ixp] = [AS]
                else:
                    ixp_dict[ixp].append(AS)
        for k, v in ixp_dict.items():
            as_pairs = [(str(p1), str(p2)) for p1 in v for p2 in v if p1 != p2]
            for pair in as_pairs:
                colocated_ixp[(pair[0], pair[1])] += 1
        for link in links:
            if link not in colocated_ixp:
                colocated_ixp[link] = 0
        #facility
        facility_dict = {}
        colocated_facility = defaultdict(int)
        # PeeringDB json dump
        if peeringdb_file.endswith('json'):
            with open(peeringdb_file) as f:
                data = json.load(f)
            for i in data['netfac']['data']:
                AS, facility = i['local_asn'], i['fac_id']
                if facility not in facility_dict:
                    facility_dict[facility] = [AS]
                else:
                    facility_dict[facility].append(AS)

        for k, v in facility_dict.items():
            as_pairs = [(str(p1), str(p2)) for p1 in v for p2 in v if p1 != p2]
            for pair in as_pairs:
                colocated_facility[(pair[0], pair[1])] += 1
        for link in links:
                if link not in colocated_facility:
                    colocated_facility[link] = 0

        #label
        with open('./stage1_final.txt','r') as f:
            result = eval(f.read())
        label = defaultdict(int)
        for link in links:
            if link not in result:
                label[link] = 3
            else:
                label[link] = result[link]+1
        link_arr = []
        for link in links:
            tmp = []
            tmp.append(link)
            tmp.append(node_fea.loc[link[0],'degree'])
            tmp.append(node_fea.loc[link[0],'distance'])
            tmp.append(node_fea.loc[link[0],'up-distance'])
            tmp.append(node_fea.loc[link[0],'down-distance'])
            tmp.append(node_fea.loc[link[0],'same-distance'])
            tmp.append(node_fea.loc[link[1],'degree'])
            tmp.append(node_fea.loc[link[1],'distance'])
            tmp.append(node_fea.loc[link[1],'up-distance'])
            tmp.append(node_fea.loc[link[1],'down-distance'])
            tmp.append(node_fea.loc[link[1],'same-distance'])
            tmp.append(vp_cnt[link])
            tmp.append(colocated_ixp[link])
            tmp.append(colocated_facility[link])
            tmp.append(label[link])
            link_arr.append(tmp)
            if label[link] == 1:
                tmp = []
                tmp.append((link[1],link[0]))
                tmp.append(node_fea.loc[link[1],'degree'])
                tmp.append(node_fea.loc[link[1],'distance'])
                tmp.append(node_fea.loc[link[1],'up-distance'])
                tmp.append(node_fea.loc[link[1],'down-distance'])
                tmp.append(node_fea.loc[link[1],'same-distance'])
                tmp.append(node_fea.loc[link[0],'degree'])
                tmp.append(node_fea.loc[link[0],'distance'])
                tmp.append(node_fea.loc[link[0],'up-distance'])
                tmp.append(node_fea.loc[link[0],'down-distance'])
                tmp.append(node_fea.loc[link[0],'same-distance'])
                tmp.append(vp_cnt[link])
                tmp.append(colocated_ixp[link])
                tmp.append(colocated_facility[link])
                tmp.append(label[link])
                link_arr.append(tmp)
        link_fea = pd.DataFrame(link_arr,columns=['link','degree1','distance1','up-distance1','down-distance1','same-distance1',
                                                  'degree2','distance2','up-distance2','down-distance2','same-distance2','vp_cnt',
                                                 'colocated_ixp','colocated_facility','label'])
        link_fea.to_csv('fea.csv',index=False)
    
    def vote_TS(self,dir,date):
        debug('[Struc.vote_TS]',stack_info=True)
        info('[Struc.vote_TS]')
        # vote
        if self.file_num == None:
            self.topoFusion = TopoFusion(self.file_num,dir,date)
        else:
            file_num=0
            names = os.listdir(tswd)
            for name in names:
                if 'path_' in name:
                    file_num+=1
            self.topoFusion = TopoFusion(file_num,dir,date)
        self.topoFusion.getTopoProb()

    def vote_simple_ts(self,dir,date,file_list,output_file):
        debug('[Struc.vote_TS]',stack_info=True)
        info('[Struc.vote_TS]')
        self.topoFusion = TopoFusion(10,dir,date)
        self.topoFusion.vote_among(file_list,output_file)

    #6125
    # the method is used for vote among files in varible "file_list"
    # following method in Apollo/Stage_1.py
    # most parts are copied from apollo and basiclly just the same
    def vote_ap(self,file_list,filename):
        """
        vote from all files
        file_list: containing all files that give their votes
        filename: where the result puts
        """
        links=dict()
        for file in file_list:
            with open(file,'r') as ff:
                for line in ff:
                    if line.startswith('#'):
                        continue
                    line=line.strip().split('|')
                    asn1=line[0]
                    asn2=line[1]
                    rel = line[2]
                    links.setdefault((asn1,asn2),set()).add(rel)
        result=dict()
        for link,rel in links.items():
            if len(rel)>1 and 0 in rel:
                result[link] = 0
            elif len(rel) == 1:
                result[link] = list(rel)[0]

        w = open(filename,'w')
        for link,rel in result.items():
            w.write(f'{link[0]}|{link[1]}|{rel}\n')

    @staticmethod
    def AP_to_read(read_from,wrtie_to):
        w = open(wrtie_to,'w')
        with open(read_from,'r') as f:
            res = eval(f.read())
            for link,rel in res.items():
                w.write(f'{link[0]}|{link[1]}|{rel}\n')

    @staticmethod
    def cross_ts(dir,ar_version,files):
        for file in files:
            dst_name = 'cross_'+file +'.ar'
            src_name = join(dir,file)
            dst_name = join(dir,dst_name)
            command = f"perl {ar_version} {src_name} > {dst_name}"
            os.system(command)

    def cross_ap(self,dir,files):
        for file in files:
            path_file = join(dir,file)
            AP_stage1_file = join(dir,'cross_'+file +'.st1')
            wp_file = join(dir,'cross_'+file +'.wrn')
            self.apollo(path_file,AP_stage1_file,wp_file)
            Struc.AP_to_read(AP_stage1_file,AP_stage1_file.replace('st1','apr'))

class Links(object):
    def __init__(self, dir_name, org_name, peering_name):
        self.dir = dir_name
        self.org_name = org_name
        self.peering_name = peering_name
        self.prob = dict()
        self.init_prob = dict()
        self.siblings = set()
        self.edge_infer = set()
        self.edge_finish = set()

        self.provider = defaultdict(set)
        self.peer = defaultdict(set)
        self.customer = defaultdict(set)

        self.tripletRel = dict()
        self.nonpath = dict()
        self.distance2Clique = dict()
        self.vp = dict()
        self.ixp = defaultdict(int)
        self.facility = defaultdict(int)
        self.adjanceTypeRatio = dict()
        self.degreeRatio = dict()
        self.vppos = dict()
        self.tier = Hierarchy(self.dir + 'asrel_prime_prob.txt')

        self.ingestProb()
        self.extractSiblings()

    def ingestProb(self):
        self.edge_finish = set()
        #TODO
        with open(self.dir + 'asrel_prime_prime.txt') as f:
            for line in f:
                if not line.startswith('#'):
                    [asn1, asn2, rel] = line.strip().split('|')
                    self.edge_finish.add((asn1, asn2))
                    self.edge_finish.add((asn2, asn1))
        #TODO
        with open(self.dir + 'asrel_prime_prob.txt') as f:
            for line in f:
                if not line.startswith('#'):
                    [asn1, asn2, rel, p2p, p2c, c2p] = line.strip().split('|')[:6]
                    a, b, c = random.uniform(1e-5, 1e-3), random.uniform(1e-5, 1e-3), random.uniform(1e-5, 1e-3)
                    self.init_prob[(asn1, asn2)] = (float(p2p)+a, float(p2c)+b, float(c2p)+c)
                    self.init_prob[(asn2, asn1)] = (float(p2p)+a, float(c2p)+c, float(p2c)+b)
                    if (asn1, asn2) not in self.edge_finish:
                        self.edge_infer.add((asn1, asn2))
                        self.edge_infer.add((asn2, asn1))
                    if rel == '0':
                        self.prob[(asn1, asn2)] = (1.0, 0.0, 0.0)
                        self.prob[(asn2, asn1)] = (1.0, 0.0, 0.0)
                        self.peer[asn1].add(asn2)
                        self.peer[asn2].add(asn1)
                    elif rel == '-1':
                        self.prob[(asn1, asn2)] = (0.0, 1.0, 0.0)
                        self.prob[(asn2, asn1)] = (0.0, 0.0, 1.0)
                        self.customer[asn1].add(asn2)
                        self.provider[asn2].add(asn1)
    
    def parseBGPPaths(self):
        forwardPath, reversePath = set(), set()
        #TODO
        with open('aspaths.txt') as f:
            for line in f:
                if line.strip() == '':
                    continue
                path = line.strip().split("|")
                forwardPath.add("|".join(path))
                reversePath.add("|".join(path[::-1]))
        return forwardPath, reversePath

    def getEdgeClass(self):
        forwardPath, reversePath = self.parseBGPPaths()
        edge2VP = defaultdict(list)
        tripletRel = defaultdict(lambda: [[0 for x in range(4)] for y in range(4)])
        for path in (forwardPath | reversePath):
            flag = 1
            ASes = path.split('|')
            for i in range(len(ASes) - 1):
                if (ASes[i], ASes[i+1]) not in self.prob:
                    flag = 0
            if flag == 1:
                linkList = ['NULL']
                for i in range(len(ASes) - 1):
                    linkList.append((ASes[i], ASes[i+1]))
                linkList.append('NULL')
                
                for i in range(1, len(linkList)-1):
                    prevRel = self.getEdgeRelationship(linkList[i-1])
                    nextRel = self.getEdgeRelationship(linkList[i+1])
                    tripletRel[linkList[i]][prevRel][nextRel] += 1
                    if path in forwardPath:
                        edge2VP[linkList[i]].append(ASes[0])
                    
        for edge in self.prob:
            if edge in edge2VP:
                self.vp[edge] = len(set(edge2VP[edge]))
                tmp = [0, 0, 0, 0]
                for obvp in edge2VP[edge]:
                    tmp[self.tier.get_hierarchy(obvp)] += 1
                vps = sum(tmp)
                for i in range(4):
                    tmp[i] = round(tmp[i]/vps/0.1)
                self.vppos[edge] = copy.deepcopy(tmp)
            else:
                self.vp[edge] = 0
                self.vppos[edge] = [0, 0, 0, 0]
        
        for edge in self.prob:
            trs = float(np.array(tripletRel[edge]).sum())
            tmp = list()
            for i in range(4):
                for j in range(4):
                    tripletRel[edge][i][j] = round(tripletRel[edge][i][j]/trs/0.1)
                    tmp.append(tripletRel[edge][i][j])
            self.tripletRel[edge] = copy.deepcopy(tmp)
    
    def getEdgeRelationship(self, edge):
        if edge == 'NULL':
            return 0
        asn1, asn2 = edge
        if asn1 in self.customer[asn2]:
            return 1
        if asn1 in self.peer[asn2]:
            return 2
        if asn1 in self.provider[asn2]:
            return 3
    
    def extractSiblings(self):
        formatCounter = 0
        orgAsn = defaultdict(list)
        with open(self.org_name) as f:
            for line in f:
                if formatCounter == 2:
                    asn = line.split('|')[0]
                    orgId = line.split('|')[3]
                    orgAsn[orgId].append(asn)
                if line.startswith("# format"):
                    formatCounter += 1
        for _, v in orgAsn.items():
            siblingPerm = permutations(v, 2)
            for i in siblingPerm:
                self.siblings.add(i)

    def assignNonpath(self):
        for link in self.prob:
            PCPP = len(self.peer[link[0]]) + len(self.provider[link[0]])
            prevLink = 0
            for i in range(4):
                prevLink += self.tripletRel[link][8+i]
                prevLink += self.tripletRel[link][12+i]
            if PCPP > 0 and prevLink == 0:
                self.nonpath[link] = PCPP
            else:
                self.nonpath[link] = 0

    def assignIXPFacility(self):
        ixp_dict = {}
        facility_dict = {}

        if self.peering_name.endswith('json'):
            with open(self.peering_name) as f:
                data = json.load(f)
            for i in data['netixlan']['data']:
                AS, ixp = i['asn'], i['ixlan_id']
                if ixp not in ixp_dict:
                    ixp_dict[ixp] = [AS]
                else:
                    ixp_dict[ixp].append(AS)
            for i in data['netfac']['data']:
                AS, facility = i['local_asn'], i['fac_id']
                if facility not in facility_dict:
                    facility_dict[facility] = [AS]
                else:
                    facility_dict[facility].append(AS)

        elif self.peering_name.endswith('sqlite'):
            conn = sqlite3.connect(self.peering_name)
            c = conn.cursor()
            for row in c.execute("SELECT asn, ixlan_id FROM 'peeringdb_network_ixlan'"):
                AS, ixp = row[0], row[1]
                if ixp not in ixp_dict:
                    ixp_dict[ixp] = [AS]
                else:
                    ixp_dict[ixp].append(AS)
            for row in c.execute("SELECT local_asn, fac_id FROM 'peeringdb_network_facility'"):
                AS, facility = row[0], row[1]
                if facility not in facility_dict:
                    facility_dict[facility] = [AS]
                else:
                    facility_dict[facility].append(AS)
        
        else:
            raise TypeError('PeeringDB file must be either a json file or a sqlite file.')
                

        for _, v in ixp_dict.items():
            as_pairs = [(p1, p2) for p1 in v for p2 in v if p1 != p2]
            for pair in as_pairs:
                self.ixp[(pair[0], pair[1])] += 1
        for _, v in facility_dict.items():
            as_pairs = [(p1, p2) for p1 in v for p2 in v if p1 != p2]
            for pair in as_pairs:
                self.facility[(pair[0], pair[1])] += 1

        for link in self.prob:
            if link not in self.ixp:
                self.ixp[link] = 0
        for link in self.prob:
            if link not in self.facility:
                self.facility[link] = 0

    def assignDistance2Clique(self):
        shortestDistanceList = defaultdict(list)
        g = nx.Graph()
        for link in self.prob:
            g.add_edge(link[0], link[1])
        clique = ['174', '209', '286', '701', '1239', '1299', '2828', '2914', '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
        for c in clique:
            if c not in g:
                clique.remove(c)
            else:
                p = nx.shortest_path_length(g, source=c)
                for k, v in p.items():
                    shortestDistanceList[k].append(v)

        for link in self.prob:
            AS1, AS2 = link
            if len(shortestDistanceList[AS1]) == 0:
                disAS1 = 200
            else:
                disAS1 = int(sum(shortestDistanceList[AS1])/float(len(shortestDistanceList[AS1]))/0.1)
            if len(shortestDistanceList[AS2]) == 0:
                disAS2 = 200
            else:
                disAS2 = int(sum(shortestDistanceList[AS2])/float(len(shortestDistanceList[AS2]))/0.1)
            self.distance2Clique[link] = (disAS1, disAS2)

    def assignAdjanceTypeRatio(self):
        for link in self.prob:
            AS1, AS2 = link
            type1 = [len(self.provider[AS1]), len(self.peer[AS1]), len(self.customer[AS1])]
            deg1 = float(sum(type1))
            type2 = [len(self.provider[AS2]), len(self.peer[AS2]), len(self.customer[AS2])]
            deg2 = float(sum(type2))
            ratio1 = list(map(lambda x:round(x/deg1/0.1), type1))
            ratio2 = list(map(lambda x:round(x/deg2/0.1), type2))
            self.adjanceTypeRatio[link] = (ratio1[0], ratio1[1], ratio1[2], ratio2[0], ratio2[1], ratio2[2])
            self.degreeRatio[link] = round(deg1/deg2/0.1)
    
    def constructAttributes(self):
        self.getEdgeClass()
        self.assignNonpath()
        self.assignDistance2Clique()
        self.assignIXPFacility()
        self.assignAdjanceTypeRatio()

def output(final_prob, link):
    link_infer = list(link.edge_infer)
    link_finish = list(link.edge_finish)
    link_all = link_finish + link_infer
    
    outputRel = open('asrel_toposcope.txt', 'w')
    inferredLink = set()
    tier1s = ['174', '209', '286', '701', '1239', '1299', '2828', '2914', '3257', '3320', '3356', '4436', '5511', '6453', '6461', '6762', '7018', '12956', '3549']

    for edge in link_finish:
        final_prob[edge] = link.prob[edge]
    
    for edge in link_all:
        AS1, AS2 = edge
        if AS1 in tier1s and AS2 in tier1s:
            outputRel.write('|'.join((str(AS1), str(AS2), '0')) + '\n')
            continue
        reverseEdge = (AS2, AS1)

        if edge in inferredLink:
            continue
        if edge in link.siblings:
            outputRel.write('|'.join((str(AS1), str(AS2), '1')) + '\n')
            inferredLink.add(edge)
            inferredLink.add(reverseEdge)
            continue
        inferredLink.add(edge)
        inferredLink.add(reverseEdge)
        p2p, p2c, c2p = final_prob[edge]
        if p2p > p2c and p2p > c2p:
            if AS1 < AS2:
                outputRel.write('|'.join((str(AS1), str(AS2), '0')) + '\n')
            else:
                outputRel.write('|'.join((str(AS2), str(AS1), '0')) + '\n')
        elif p2c > p2p and p2c > c2p:
            outputRel.write('|'.join((str(AS1), str(AS2), '-1')) + '\n')
        elif c2p > p2p and c2p > p2c:
            outputRel.write('|'.join((str(AS2), str(AS1), '-1')) + '\n')

def BayesNetwork(link):
    link_infer = list(link.edge_infer)
    link_finish = list(link.edge_finish)
    link_all = link_finish + link_infer

    link_feature = [link.vp, link.nonpath, link.ixp, link.facility, link.degreeRatio, link.distance2Clique, link.tripletRel, link.adjanceTypeRatio, link.vppos]
    link_feature_order = [list() for _ in range(len(link_all))]
    
    for i in range(len(link_feature)):
        f_c = 0
        feature_dict = dict()
        for j in range(len(link_all)):
            if isinstance(link_feature[i][link_all[j]], int):
                f = tuple([link_feature[i][link_all[j]]])
            else:
                f = tuple(link_feature[i][link_all[j]])
            if f not in feature_dict:
                feature_dict[f] = f_c
                f_c += 1
            link_feature_order[j].append(feature_dict[f])
    link_feature_order = np.array(link_feature_order)

    parent = {1:[0, 8], 6:[7], 5:[6, 7]}
    final_prob = dict()
    for edge in link_all:
        final_prob[edge] = list(map(lambda x: math.log10(x), link.init_prob[edge]))

    for i in range(len(link_feature)):
        prob = defaultdict(lambda: [0.0, 0.0, 0.0])
        count_class = defaultdict(lambda: [0.0, 0.0, 0.0])
        for j in range(len(link_all)):
            x = link_feature_order[j][i]
            y = []
            if i in parent:
                for pa in parent[i]:
                    y.append(link_feature_order[j][pa])
            y = tuple(y)
            temp_prob = link.init_prob[link_all[j]]
            prob[(x, y)] = tuple([a + b for a, b in zip(prob[(x, y)], temp_prob)])
            count_class[y] = tuple([a + b for a, b in zip(count_class[y], temp_prob)])
        f_prob = dict()
        for key in prob:
            (x, y) = key
            f_prob[x] = tuple([(a + 1) / (b + len(prob)) for a, b in zip(prob[(x, y)], count_class[y])])
        for j in range(len(link_all)):
            edge = link_all[j]
            x = link_feature_order[j][i]
            temp_prob = (f_prob[x][0] + 1e-10, f_prob[x][1] + 1e-10, f_prob[x][2] + 1e-10)
            final_prob[edge] = tuple(map(lambda x, y: x + y, final_prob[edge], tuple(map(lambda x: math.log10(x), temp_prob))))
    output(final_prob, link)

def BN_go():
    link = Links(args.dir_name, args.org_name, args.peering_name)
    link.constructAttributes()
    BayesNetwork(link)

def GetCred(y_fraud_train,K,ind):
    N = ind.shape[0]
    Cred = [[0,0,0] for i in range(N)]
    for i in range(N):
        cnt0 = 0
        cnt1 = 0
        cnt2 = 0
        for x in ind[i][0:K]:
            if(y_fraud_train[x] == 0):
                cnt0 +=1
            if(y_fraud_train[x] == 1):
                cnt1 +=1
            if(y_fraud_train[x] == 2):
                cnt2 +=1
        Cred[i][0] = float(cnt0/K)
        Cred[i][1] = float(cnt1/K)
        Cred[i][2] = float(cnt2/K)
    Cred = np.array(Cred)
    return Cred

params = {
    'boosting_type': 'gbdt',  
    'objective': 'multiclass',  
    'num_class': 3,  
    'metric': 'multi_error',  
    'num_leaves': 300,  
    'min_data_in_leaf': 500,  
    'learning_rate': 0.01,  
    'feature_fraction': 0.8,  
    'bagging_fraction': 0.8,  
    'bagging_freq': 5,  
    'lambda_l1': 0.4,  
    'lambda_l2': 0.5,  
    'min_gain_to_split': 0.2,  
    'verbose': -1,
    'num_threads':4
}

def pro_knn_trains(X,Y,T,pred_X,ind,K,epoch):
    Cred = GetCred(Y,K,ind)
    print("cred done!")
    #model = lgb.LGBMClassifier(params)
    N = T.shape[0]
    y_final = np.zeros(N)
    infer_res = []
    print("start train")
    num_round = 1000
    for j in range(epoch):
        print("epoch:" + str(j))
        seed = j*1234
        rng = np.random.RandomState(seed)
        Pr = rng.rand(1,N)
        for i in range(N):
            left = Cred[i][0]
            right = Cred[i][0]+Cred[i][1]
            if(Pr[0][i] <= left ):
                y_final[i] = 0
            elif(Pr[0][i] <= right ):
                y_final[i] = 1
            if(Pr[0][i] > right):
                y_final[i] = 2
        X_data = np.concatenate((X,T),axis=0)
        Y_data = np.concatenate((Y,y_final),axis=0)
        X_train,X_test,y_train,y_test=train_test_split(X_data,Y_data,test_size=0.2)
        train_data = lgb.Dataset(X_train,label=y_train)
        validation_data = lgb.Dataset(X_test,label=y_test)
        model=lgb.train(params,train_data,num_round,valid_sets=[validation_data],early_stopping_rounds = 100)
        pred = model.predict(pred_X)
        infer_res.append(pred)
    return infer_res


def run():
    df = pd.read_csv('fea.csv')
    Y = df['label'].astype(int).values
    x = df.drop(['link','label'],axis = 1).astype(float)
    X = preprocessing.MinMaxScaler().fit_transform(x.values)
    neigh = NearestNeighbors(n_neighbors = 100, algorithm = 'kd_tree',n_jobs = 4)
    neigh.fit(X)
    dis,ind = neigh.kneighbors(T,100)

    k = 100
    epoch = 50
    y_prob1= pro_knn_trains(X,Y,T,pred_x,ind,k,epoch)

    res = []
    for epoch_i in y_prob1:
        infer = []
        for i in epoch_i:
            infer.append(np.argmax(i))
        res.append(infer)

clear = False

test = False

vote = True

cross = False

simple = False



if __name__=='__main__' and clear:
    irr_file='/home/lwd/Result/auxiliary/irr.txt'
    struc = Struc()
    struc.read_irr(irr_file)
    # struc.core2leaf(['/home/lwd/Result/TS_working/path_20201208_vp1.path'],'/home/lwd/Result/TS_working/rel_20201208_vp1.cf')
    tsnames = os.listdir('/home/lwd/Result/TS_working')
    apnames = os.listdir('/home/lwd/Result/AP_working')
    for name in tsnames:
        if name.endswith('.cf') or name.endswith('.apr'):
            name = join(tswd,name)
            struc.clear(name,name)
    for name in apnames:
        if name.endswith('.cf') or name.endswith('.apr'):
            name = join(apwd,name)
            struc.clear(name,name)
    quit()

if __name__=='__main__' and test:
    read_dir='/home/lwd/Result/AP_working/'
    # read='/home/lwd/Result/AP_working/rel_20201222.st1'
    names = os.listdir(read_dir)
    for name in names:
        if name.endswith('.st1'):
            newname = name.replace('st1','apr')
            read = join(read_dir,name)
            write= join(read_dir,newname)
            print(read)
            print(write)
            print(f'working on {read}')
            Struc.AP_to_read(read,write)
    quit()

if __name__=='__main__' and vote:
    irr_file='/home/lwd/Result/auxiliary/irr.txt'
    boost_file='/home/lwd/Result/auxiliary/pc20201201.v4.arout'

    # nohup perl ./asrank_irr.pl --clique 174 209 286 701 1239 1299 2828 2914 3257 3320 3356 3491 5511 6453 6461 6762 6830 7018 12956 --filtered ~/RIB.test/path.test/pc20201201.v4.u.path.clean > /home/lwd/Result/auxiliary/pc20201201.v4.arout &

    s_dir='/home/lwd/RIB.test/path.test'
    r_dir='/home/lwd/Result'

    ts_working_dir='TS_working'
    ap_working_dir='AP_working'

    auxd = join(r_dir,'auxiliary')
    tswd = join(r_dir,ts_working_dir)
    apwd = join(r_dir,ap_working_dir)

    votd = join(r_dir,'vote')

    tsfiles = os.listdir(tswd)
    apfiles = os.listdir(apwd)

    _tsfiles=dict()
    _apfiles=[]
    # for n in tsfiles:
    #     if n.endswith('.ar'):
    #         res = re.match(r'^rel_([0-9]+)',n)
    #         today=None
    #         if res is not None:
    #             date = res.group(1)
    #             name = join(tswd,n)
    #             today = _tsfiles.setdefault(date,[]).append(name)
    #         else:
    #             continue
    for n in tsfiles:
        if n.endswith('.apr2'):
            res = re.match(r'^rel_([0-9]+)',n)
            today=None
            if res is not None:
                date = res.group(1)
                name = join(tswd,n)
                today = _tsfiles.setdefault(date,[]).append(name)
            else:
                continue

    for n in apfiles:
        if n.endswith('.apr'):
            name = join(apwd,n)
            _apfiles.append(name)

    struc = Struc()
    struc.read_irr(irr_file)


    # quit()


    for date,files in _tsfiles.items():
        print(date,files)
        outf = join(votd,'tsv',f'ap2_bv_{date}.rel')
        struc.vote_simple_ts(tswd,date, files,outf)

    #6125
    _apfiles = ['/home/lwd/Result/vote/tsv/ap2_bv_20201201.rel',
    '/home/lwd/Result/vote/tsv/ap2_bv_20201208.rel',
    '/home/lwd/Result/vote/tsv/ap2_bv_20201215.rel',
    '/home/lwd/Result/vote/tsv/ap2_bv_20201222.rel',]
    outf = join(votd,'apv','ap2_bv.rel')
    struc.vote_ap(_apfiles,outf)    

    # tsvote 

    # file_list=[f'/home/lwd/Result/TS_working/rel_wholemonth_vp{i}.ar' for i in range(0,14)]
    # # input(file_list)
    # output_file='/home/lwd/Result/vote/tsv/ar_tsv_month.rel'
    # struc_ar_tsv = Struc()
    # struc_ar_tsv.read_irr(irr_file)
    # struc_ar_tsv.topoFusion = TopoFusion(14,dir,date)
    # for file in file_list:
    #     checke(file)
    # struc_ar_tsv.topoFusion.vote_among(file_list,output_file)
    # struc_ar_tsv=None

    file_list=[f'/home/lwd/Result/TS_working/rel_wholemonth_vp{i}.apr2' for i in range(0,14)]
    # input(file_list)
    output_file='/home/lwd/Result/vote/tsv/ap2_tsv_month.rel'
    struc_ap_tsv = Struc()
    struc_ap_tsv.read_irr(irr_file)
    struc_ap_tsv.topoFusion = TopoFusion(14,dir,date)
    for file in file_list:
        checke(file)
    struc_ap_tsv.topoFusion.vote_among(file_list,output_file)
    struc_ap_tsv=None

    _apfiles = ['/home/lwd/Result/AP_working/rel_20201201.apr2',
    '/home/lwd/Result/AP_working/rel_20201208.apr2',
    '/home/lwd/Result/AP_working/rel_20201215.apr2',
    '/home/lwd/Result/AP_working/rel_20201222.apr2',]
    outf = join(votd,'apv','ap2_apv.rel')
    struc.vote_ap(_apfiles,outf)    

    quit()

    # ts file, ts vote
    for date,files in _tsfiles.items():
        print(date,files)
        outf = join(votd,'tsv',f'tsf_{date}.rel')
        struc.vote_simple_ts(tswd,date, files,outf)

    # ap file, ap vote


    quit()

if __name__=='__main__' and cross:

    irr_file='/home/lwd/Result/auxiliary/irr.txt'
    boost_file='/home/lwd/Result/auxiliary/pc20201201.v4.arout'

    # nohup perl ./asrank_irr.pl --clique 174 209 286 701 1239 1299 2828 2914 3257 3320 3356 3491 5511 6453 6461 6762 6830 7018 12956 --filtered ~/RIB.test/path.test/pc20201201.v4.u.path.clean > /home/lwd/Result/auxiliary/pc20201201.v4.arout &

    s_dir='/home/lwd/RIB.test/path.test'
    r_dir='/home/lwd/Result'

    ts_working_dir='TS_working'
    ap_working_dir='AP_working'

    auxd = join(r_dir,'auxiliary')
    tswd = join(r_dir,ts_working_dir)
    apwd = join(r_dir,ap_working_dir)

    votd = join(r_dir,'vote')

    tsfiles = os.listdir(tswd)
    apfiles = os.listdir(apwd)

    _tsfiles=[]
    _apfiles=[]
    for n in tsfiles:
        if n.endswith('.path'):
            _tsfiles.append(n)

    ar_version='/home/lwd/AT/TopoScope/asrank_irr.pl'
    struc = Struc()
    struc.read_irr(irr_file)

    struc.cross_ap(tswd,_tsfiles)

    quit()

if __name__=='__main__' and simple:
    print('start')

    group_size=25
    irr_file='/home/lwd/Result/auxiliary/irr.txt'
    boost_file='/home/lwd/Result/auxiliary/pc202012.v4.arout'

    # nohup perl ./asrank_irr.pl --clique 174 209 286 701 1239 1299 2828 2914 3257 3320 3356 3491 5511 6453 6461 6762 6830 7018 12956 --filtered ~/RIB.test/path.test/pc20201201.v4.u.path.clean > /home/lwd/Result/auxiliary/pc20201201.v4.arout &

    s_dir='/home/lwd/RIB.test/path.test'
    r_dir='/home/lwd/Result'

    ts_working_dir='TS_working'
    ap_working_dir='AP_working'

    auxd = join(r_dir,'auxiliary')
    tswd = join(r_dir,ts_working_dir)
    apwd = join(r_dir,ap_working_dir)

    



    # TS simple infer 
    ar_version='/home/lwd/AT/TopoScope/asrank_irr.pl'

    boost_file = join(tswd,boost_file)
    path_dir = s_dir
    # def checke(path):
    #     if exists(path):
    #         print(f'ready:{path}')
    #     else:
    #         print(f'not exists:{path}')

    name = 'pc202012.v4.u.path.clean'
    path_file = join(s_dir,name)

    checke(irr_file)
    checke(boost_file)
    checke(path_dir)
    checke(path_file)
    checke(ar_version)
    checke(auxd)
    checke(tswd)
    checke(apwd)
    date='wholemonth'
    # print(f'date:{date}')
    while True:
        a = input('continue?')
        if a == 'y':
            break
        elif a =='n':
            quit()
    
    struc = Struc()
    struc.read_irr(irr_file)


    tsls = os.listdir(tswd)
    in_files=[]
    out_files=[]
    out_files_ap=[]
    first  = False
    for name in tsls:
        if name.endswith('path'):
            res = re.match(r'^path_(.+)\.path',name)
            if res is not None:
                tmp = res.group(1)
                nn = join(tswd,name)
                in_files.append([nn])
                nn = join(tswd,f'rel_{tmp}.cf')
                out_files.append(nn)
                nn = join(tswd,f'rel_{tmp}.apr2')
                out_files_ap.append(nn)
            else:
                continue
    
    # for in_file,out_file in zip(in_files,out_files):
    #     print(in_file)
    #     print(out_file)
    
    # while True:
    #     a = input('continue?')
    #     if a == 'y':
    #         break
    #     elif a =='n':
    #         quit()
    # vg vb
    for in_file,out_file,out_file_ap in zip(in_files,out_files,out_files_ap):
        print(in_file)
        print(out_file)
        print(out_file_ap)
        if first:
            start = time.time()
            struc.core2leaf(in_file,out_file)
            p1 = time.time()
            struc.apollo_it(in_file,out_file_ap)
            p2 = time.time()
            print(f'c2f:{p1-start}s\napollo:{p2-p1}s')
            first = False
        else:
            #6125
            # here, to infer groups of Toposcope vote and both methods vote
            # output name is in form of "rel_XXXXX.apr2"
            struc.apollo_it(in_file,out_file_ap)

            # pass
    # vd
    in_files=[
        '/home/lwd/RIB.test/path.test/pc20201201.v4.u.path.clean',
        '/home/lwd/RIB.test/path.test/pc20201208.v4.u.path.clean',
        '/home/lwd/RIB.test/path.test/pc20201215.v4.u.path.clean',
        '/home/lwd/RIB.test/path.test/pc20201222.v4.u.path.clean',
    ]
    out_files=[
        '/home/lwd/Result/AP_working/rel_20201201.cf',
        '/home/lwd/Result/AP_working/rel_20201208.cf',
        '/home/lwd/Result/AP_working/rel_20201215.cf',
        '/home/lwd/Result/AP_working/rel_20201222.cf',
        ]
    out_files_ap=[
        '/home/lwd/Result/AP_working/rel_20201201.apr2',
        '/home/lwd/Result/AP_working/rel_20201208.apr2',
        '/home/lwd/Result/AP_working/rel_20201215.apr2',
        '/home/lwd/Result/AP_working/rel_20201222.apr2',
        ]
    for in_file,out_file,out_file_ap in zip(in_files,out_files,out_files_ap):
        print(in_file)
        print(out_file)
        print(out_file_ap)
        if first:
            start = time.time()
            struc.core2leaf([in_file],out_file)
            p1 = time.time()
            struc.apollo_it(in_file,out_file_ap)
            p2 = time.time()
            print(f'c2f:{p1-start}s\napollo:{p2-p1}s')
            first = False
        else:
            #6125
            # here, to infer groups of apollo vote
            # output name is in form of "rel_DATE.apr2" (4 files in different days)
            struc.apollo_it([in_file],out_file_ap)
    #TODO
    # struc.get_relation(boost_file)
    # struc.cal_hierarchy()
    # struc.set_VP_type(path_file)
    # struc.divide_TS(group_size,tswd,date)
    # struc.infer_TS(tswd,ar_version,date)
    quit()

if __name__=='__main__':
    print('start')

    group_size=25
    irr_file='/home/lwd/Result/auxiliary/irr.txt'
    boost_file='/home/lwd/Result/auxiliary/pc20201201.v4.arout'

    # nohup perl ./asrank_irr.pl --clique 174 209 286 701 1239 1299 2828 2914 3257 3320 3356 3491 5511 6453 6461 6762 6830 7018 12956 --filtered ~/RIB.test/path.test/pc20201201.v4.u.path.clean > /home/lwd/Result/auxiliary/pc20201201.v4.arout &

    s_dir='/home/lwd/RIB.test/path.test'
    r_dir='/home/lwd/Result'

    ts_working_dir='TS_working'
    ap_working_dir='AP_working'

    auxd = join(r_dir,'auxiliary')
    tswd = join(r_dir,ts_working_dir)
    apwd = join(r_dir,ap_working_dir)

    



    # TS simple infer 
    ar_version='/home/lwd/AT/TopoScope/asrank_irr.pl'

    # boost_file = join(tswd,boost_file)
    path_dir = s_dir
    def checke(path):
        if exists(path):
            print(f'ready:{path}')
        else:
            print(f'not exists:{path}')

    names = os.listdir(path_dir)
    #TODO
    # names=[ 'pc20201201.v4.u.path.clean']
    names.sort()
    for idx,name in enumerate(names):
        if name.endswith('.clean'):
            if 'v6' in name:
                continue
            date=f'unknown{idx}'
            res = re.match(r'^pc([0-9]+)',name)
            if res is not None:
                date = res.group(1)
            boost_file=join(auxd,f'pc{date}.v4.arout')

            path_file = join(s_dir,name)

            checke(irr_file)
            checke(boost_file)
            checke(path_dir)
            checke(path_file)
            checke(ar_version)
            checke(auxd)
            checke(tswd)
            checke(apwd)
            
            print(f'date:{date}')
            # while True:
            #     a = input('continue?')
            #     if a == 'y':
            #         break
            #     elif a =='n':
            #         quit()
            
            struc = Struc()
            struc.read_irr(irr_file)
            #TODO
            struc.get_relation(boost_file)
            struc.cal_hierarchy()
            struc.set_VP_type(path_file)
            struc.divide_TS(group_size,tswd,date)
            struc.infer_TS(tswd,ar_version,date)

            # ap_file= join(apwd,f'rel_{date}.st1')
            # wp_file= join(apwd,f'rel_{date}.wrn')
            # struc.apollo(path_file,ap_file,wp_file) # AP simple infer