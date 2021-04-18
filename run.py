import os
from os import path
import sys
import re
import logging
import resource
import networkx as nx
import json
from networkx.algorithms.structuralholes import mutual_weight
import pandas as pd
import time
import numpy as np
import copy, math, operator, random, scipy, sqlite3
import matplotlib.pyplot as plt
import lightgbm as lgb
import xgboost as xgb
import multiprocessing

from itertools import permutations
from random import shuffle
from collections import defaultdict
from collections import Counter
from os.path import abspath, join, exists
from networkx.algorithms.centrality import group
from logging import warn,debug,info
from sklearn import preprocessing
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neighbors import NearestNeighbors
from sklearn.model_selection import train_test_split

from TopoScope.topoFusion import TopoFusion
from location import *
from hierarchy import Hierarchy


# TODO
# logging|done
# path of files
# debug

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = abspath(join('./log',f'log_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)



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

    def core2leaf(self, path_files, output_file,version=4):
        """
        A easy core to leaf infer with irr

        a|b|r 
        r has three values
        -1 for p2c
        1 for c2p
        0 for p2p
        4 for confilct link
        """
        thetier_1=None
        if version ==4 :
            thetier_1=self.tier_1
        else:
            thetier_1=self.tier_1_v6
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
                    if ASes[i] in thetier_1:
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

    def apollo_it(self, path_files, output_file, it = 5,version=4):
        """
        core to leaf followed by iterations
        """    
        thetier_1=None
        all_link=set()
        if version ==4 :
            thetier_1=self.tier_1
        else:
            thetier_1=self.tier_1_v6
        p1 = time.time()
        print('ap_it start')
        link_rel_ap = dict()
        non_t1 =list()
        if type(path_files) is not list:
            path_files = [path_files]
        for path_file in path_files:
            pf = open(path_file,'r')
            for line in pf:
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
                prime_t1 = 10000
                for i in range(len(ASes)-1):
                    all_link.add((ASes[i],ASes[i+1]))
                    all_link.add((ASes[i+1],ASes[i]))
                    if prime_t1 <= i-2:
                        rel = link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                        if rel != -1:
                            link_rel_ap[(ASes[i],ASes[i+1])] = 4
                        continue
                    # irr
                    if(ASes[i],ASes[i+1]) in self.irr_c2p:
                        link_rel_ap.setdefault((ASes[i],ASes[i+1]),1)
                    if(ASes[i+1],ASes[i]) in self.irr_c2p:
                        link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                    if(ASes[i],ASes[i+1]) in self.irr_p2p or (ASes[i+1],ASes[i]) in self.irr_p2p:
                        link_rel_ap.setdefault((ASes[i],ASes[i+1]),0)
                    if ASes[i] in thetier_1:
                        if prime_t1 == i-1:
                            link_rel_ap.setdefault((ASes[i-1],ASes[i]),0)
                        prime_t1 = i
                # if prime_t1 == 10000:
                non_t1.append(ASes)
            pf.close()
        p2 = time.time()
        print(f'done first time: {p2-p1}s')
        turn = 0
        while True:
            tmp = []
            convert = False
            turn += 1
            t1= time.time()
            print(f'start it{turn}, {len(non_t1)}')
            for ASes in non_t1:
                convert_sub = False
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
                    for i in range(idx_11+1,len(ASes)-1):
                        rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                        if rel is None:
                            convert = True
                            convert_sub = True
                            link_rel_ap[(ASes[i],ASes[i+1])] = -1
                        else:
                            if rel != -1:
                                link_rel_ap[(ASes[i],ASes[i+1])]=4
                if idx_1 !=0:
                    for i in range(idx_1-1):
                        rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                        if rel is None:
                            convert = True
                            convert_sub = True
                            link_rel_ap[(ASes[i],ASes[i+1])]=1 
                        else:
                            if rel != 1:
                                link_rel_ap[(ASes[i],ASes[i+1])]=4
                if idx_0 !=0:
                    if idx_0>=2:
                        for i in range(idx_0-1):
                            rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                            if rel is None:
                                convert = True
                                convert_sub = True
                                link_rel_ap[(ASes[i],ASes[i+1])]=1
                            else:
                                if rel != 1:
                                    link_rel_ap[(ASes[i],ASes[i+1])]=4
                    if idx_0<=len(ASes)-2:
                        for i in range(idx_0+1,len(ASes)-1):
                            rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                            if rel is None:
                                convert = True
                                convert_sub = True
                                link_rel_ap[(ASes[i],ASes[i+1])]=-1
                            else:
                                if rel != -1:
                                    link_rel_ap[(ASes[i],ASes[i+1])]=4
                if not convert_sub:
                    tmp.append(ASes)
            if not convert:
                break
            non_t1 = tmp
            t2= time.time()
            print(f'for it{turn}, takes {t2-t1}s')
        wf = open(output_file,'w')
        linkset = set()
        for link,rel in link_rel_ap.items():
            if link in linkset:
                continue
            rev = (link[1],link[0])
            asn1 = int(link[0])
            asn2 = int(link[1])
            if asn1 < asn2:
                line = f'{asn1}|{asn2}|{rel}\n'
            else:
                line = f'{asn2}|{asn1}|{-rel}\n'
            if rel != 4:
                wf.write(line)
            linkset.add(link)
            linkset.add(rev)
            # loose
        for link in all_link:
            if link in linkset:
                continue
            rev = (link[1],link[0])
            asn1 = int(link[0])
            asn2 = int(link[1])
            if asn1 < asn2:
                line = f'{asn1}|{asn2}|{0}\n'
            else:
                line = f'{asn2}|{asn1}|{0}\n'
            if rel != 4:
                wf.write(line)
            linkset.add(link)
            linkset.add(rev)
        wf.close()
        p3= time.time()
        print(f'iteration takes {p3-p2}s')
        print(f'ap_it takes {p3-p1}s')

    @staticmethod
    def apollo_copy(irr_file, filelist):
            # irr
        with open(irr_file,'r') as f:
            lines = f.readlines()
        irr_c2p = set()
        irr_p2p = set()
        for line in lines:
            if line.startswith('#'):
                continue
            tmp = re.split(r'[\s]+',line)
            # print(tmp)
            if tmp[2] == '1':
                irr_c2p.add((tmp[0],tmp[1]))
            if tmp[2] == '0':
                irr_p2p.add((tmp[0],tmp[1]))
        print('starting')
        tier_1 =['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
        link_relation = {}
        # src
        # filelist = os.listdir('.')
        non_tier_1 = []
        wrong_path = []
        for file in filelist:
            if file.startswith('as_path') or True:
                f = open(file,'r')
                lines = f.readlines()
                f.close()
                for line in lines:
                    asn = line.strip().split('|')
                    for i in range(len(asn)-1):
                        if (asn[i],asn[i+1]) in irr_c2p:
                            link_relation.setdefault((asn[i],asn[i+1]),set()).add(1)
                        if (asn[i+1],asn[i]) in irr_c2p:
                            link_relation.setdefault((asn[i],asn[i+1]),set()).add(-1)
                        if (asn[i],asn[i+1]) in irr_p2p or (asn[i+1],asn[i]) in irr_p2p:
                            link_relation.setdefault((asn[i],asn[i+1]),set()).add(0)
                        if asn[i] in tier_1 and asn[i+1] in tier_1 :
                            link_relation.setdefault((asn[i],asn[i+1]),set()).add(0)

                    idx = -1
                    cnt = 0
                    for i in range(len(asn)):
                        if asn[i] in tier_1:
                            idx = i
                            cnt+=1
                    if cnt>=2 and asn[idx-1] not in tier_1:
                        wrong_path.append(asn)
                        continue
                    if idx>=2:
                        for i in range(idx-1):
                            link_relation.setdefault((asn[i],asn[i+1]),set()).add(1)
                    if idx<=len(asn)-2 and cnt>0:
                        for i in range(idx+1,len(asn)-1):
                            link_relation.setdefault((asn[i],asn[i+1]),set()).add(-1)
                    if idx == -1:
                        non_tier_1.append(asn)
                #print(t1_link)
        print(len(non_tier_1))
        for it in range(5):
            for asn in non_tier_1:
                idx_11 = 0
                idx_1 = 0
                idx_0 = 0
                for i in range(len(asn)-1):
                    if (asn[i],asn[i+1]) in link_relation.keys() and list(link_relation[(asn[i],asn[i+1])]) == [-1]:
                        idx_11 = i
                    if (asn[i],asn[i+1]) in link_relation.keys() and list(link_relation[(asn[i],asn[i+1])]) == [0]:
                        idx_0 = i
                    if (asn[i],asn[i+1]) in link_relation.keys() and list(link_relation[(asn[i],asn[i+1])]) == [1]:
                        idx_1 = i
                if idx_11 !=0:
                    for i in range(idx_11+1,len(asn)-1):
                        link_relation.setdefault((asn[i],asn[i+1]),set()).add(-1)
                if idx_1 !=0:
                    for i in range(idx_1-1):
                        link_relation.setdefault((asn[i],asn[i+1]),set()).add(1)
                if idx_0 !=0:
                    if idx_0>=2:
                        for i in range(idx_0-1):
                            link_relation.setdefault((asn[i],asn[i+1]),set()).add(1)
                    if idx_0<=len(asn)-2:
                        for i in range(idx_0+1,len(asn)-1):
                            link_relation.setdefault((asn[i],asn[i+1]),set()).add(-1)
    
            p2c_cnt = 0
            c2p_cnt = 0
            p2p_cnt = 0
            dulp = 0
            for k,v in link_relation.items():
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
        for k,v in link_relation.items():
            if len(v)>1 and 0 in v:
                result[k] = 0
            elif len(v) == 1:
                result[k] = list(v)[0]
            else:
                conflict +=1
        print('conflict: ' + str(conflict))

        #dst
        print('saving')
        f = open('./stage1_res.txt','w')
        f.write(str(result))
        f.close()
        f = open('./wrong_path.txt','w')
        f.write(str(wrong_path))
        f.close()

    def c2f_strong(self,infile,outfile,it=1,version=4):
        thetier_1=None
        if version ==4 :
            thetier_1=self.tier_1
        else:
            thetier_1=self.tier_1_v6
        true_path=[]
        link2rel = {}
        clique1=None
        clique2=None
        with open(infile,'r') as ff:
            for line in ff:
                discard=False
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
                for i in range(len(ASes)-1):
                    if (ASes[i],ASes[i+1]) in self.irr_c2p:
                        link2rel[(ASes[i],ASes[i+1])]=1
                    if (ASes[i+1],ASes[i]) in self.irr_c2p:
                        link2rel[(ASes[i],ASes[i+1])]=-1
                    if (ASes[i],ASes[i+1]) in self.irr_p2p or (ASes[i+1],ASes[i]) in self.irr_p2p:
                        link2rel[(ASes[i],ASes[i+1])]=0
                    if ASes[i] in thetier_1 and ASes[i+1] in thetier_1:
                        link2rel[(ASes[i],ASes[i+1])]=0
                        link2rel[(ASes[i+1],ASes[i])]=0
                for i in range(len(ASes)):
                    if ASes[i] in thetier_1:
                        if clique1 is None:
                            clique1 = i
                        elif clique2 is None:
                            clique2 = i
                        else:
                            discard = True
                if clique1 and clique2:
                    if clique2 - clique1 != 1:
                        discard = True
                if discard:
                    continue
                true_path.append(ASes)
        ff.close()
        for turn in range(it):
            tmp_true_path=[]
            for ASes in true_path:
                descend=False
                discard = False
                for i in range(len(ASes)-1):
                    rel=  link2rel.get((ASes[i],ASes[i+1]))
                    if rel:
                        if rel == 1:
                            if descend:
                                discard = True
                                break
                        elif rel == 0:
                            if descend:
                                discard = True
                                break
                            descend = True
                        elif rel == -1:
                            descend = True
                        else:
                            discard= True
                            break
                if discard:
                    continue
                tmp_true_path.append(ASes)
            true_path=tmp_true_path
            added_rel={}
            for ASes in true_path:
                for i in range(len(ASes)-2):
                    rel =  link2rel.get((ASes[i],ASes[i+1]))
                    if rel:
                        if rel == 1:
                            pass
                        elif rel == 0:
                            added_rel.setdefault((ASes[i+1],ASes[i+2]),-1)
                        elif rel == -1:
                            added_rel.setdefault((ASes[i+1],ASes[i+2]),-1)
                        else:
                            pass
            for link,rel in added_rel.items():
                forward_rel = link2rel.get(link)
                backward_rel = link2rel.get((link[1],link[0]))
                if forward_rel is None and backward_rel is None:
                    link2rel[link]=rel
                    continue
                elif forward_rel and backward_rel is None:
                    if forward_rel == 1:
                        link2rel[link]=4
                elif forward_rel is None and backward_rel:
                    if backward_rel == -1:
                        link2rel[link]=4
                elif forward_rel and backward_rel:
                    if forward_rel == 1 and backward_rel == 1:
                        link2rel[link]=4
                    if forward_rel == -1 and backward_rel == -1:
                        link2rel[link]=4
        with open(outfile,'w') as of:
            for link,rel in link2rel.items():
                if rel ==4:
                    continue
                of.write(f'{link[0]}|{link[1]}|{rel}\n')

    def c2f_unary(self,infile, outfile,it):
        tier1s = [ '174', '209', '286', '701', '1239', '1299', '2828', '2914', '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
        untouched = []
        link2rel = {}
        last_tier1 = 10000
        print(f'start uny for {infile}')
        with open(infile,'r') as ff:
            for line in ff:
                last_tier1 = 10000
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
                for i in range(len(ASes)-1):
                    if (ASes[i],ASes[i+1]) in self.irr_c2p:
                        link2rel[(ASes[i],ASes[i+1])]=1
                    if (ASes[i+1],ASes[i]) in self.irr_c2p:
                        link2rel[(ASes[i],ASes[i+1])]=-1
                    if (ASes[i],ASes[i+1]) in self.irr_p2p or (ASes[i+1],ASes[i]) in self.irr_p2p:
                        link2rel[(ASes[i],ASes[i+1])]=0
                for i in range(len(ASes)):
                    if ASes[i] in tier1s:
                        if last_tier1 == i-1:
                            link2rel[(ASes[i-1],ASes[i])]=0
                        last_tier1 = i
                for i in range(last_tier1+1,len(ASes)-1):
                    link2rel.setdefault((ASes[i],ASes[i+1]),-1)
                    if link2rel.get((ASes[i+1],ASes[i])) == -1:
                        link2rel[(ASes[i],ASes[i+1])] = 4
                        link2rel[(ASes[i+1],ASes[i])] = 4
                if last_tier1 == 10000:
                    untouched.append(ASes)
        cnt = 0
        look = open('./lookit.txt','w')
        while(it):
            cnt +=1
            tmp_untouched=[]
            last_p2c = 10000
            convert = False
            print(f'it {cnt}: for {len(untouched)} paths')
            for ASes in untouched:
                pre={}
                post={}
                see = False
                last_p2c = 10000
                for i in range(len(ASes)-1):
                    rel = link2rel.get((ASes[i],ASes[i+1]))
                    if rel:
                        if rel == -1:
                            last_p2c = i
                    # pre[(i,i+1)]=rel
                if last_p2c == 10000:
                    tmp_untouched.append(ASes)
                    continue
                for i in range(last_p2c,len(ASes)-1):
                    link2rel.setdefault((ASes[i],ASes[i+1]),-1) 
                    if link2rel.get((ASes[i+1],ASes[i])) == -1:
                        link2rel[(ASes[i],ASes[i+1])] = 4
                        link2rel[(ASes[i+1],ASes[i])] = 4
                        see = True
                    
                    convert = True
                # for i in range(len(ASes)-1):
                #     post[(i,i+1)]= link2rel.get((ASes[i],ASes[i+1]))
                # if see == True:
                #     look.write(f'it{cnt}# pre: ')
                #     for k,v in pre.items():
                #         look.write(f'{k}-{v} ')
                #     look.write(f'\nit{cnt}#post: ')
                #     for k,v in post.items():
                #         look.write(f'{k}-{v} ')
                #     look.write(f'\nit{cnt}#lastp2c:{last_p2c}\n')
            untouched = tmp_untouched
            if not convert:
                break                
        with open(outfile,'w') as of:
            for link,rel in link2rel.items():
                if rel ==4:
                    continue
                of.write(f'{link[0]}|{link[1]}|{rel}\n')

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
    def boost(self,ar_version,path_file,dst = None):
        debug('[Struc.boost]',stack_info=True)
        info('[Struc.boost]run initial asrank for TS')
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

    def infer_ar(self,ar_version,inf,outf):
        command = f"perl {ar_version} {inf} > {outf}"
        os.system(command)   

    def vote_simple_vp(self,file_num,in_files,out_files,path=None):
        self.topoFusion = TopoFusion(file_num,'placeholder','placeholder',path_file=path)
        self.topoFusion.vote_among(in_files,out_files)

    # path_file, peeringdb file, AP vote out
    def prepare_AP(self,path_file,peeringdb_file,ap_res_file,output_file):
        '''
        prepare knn features
        '''
        print(f'prepare for {ap_res_file}')
        start = time.time()
        #TODO
        #need a normal path
        # paths = BgpPaths()
        # paths.extract_ixp(ixp_file)
        # paths.parse_bgp_paths('./sanitized_rib.txt')
        links = set()
        f = open(path_file,'r')
        lines = f.readlines()
        for line in lines:
            if '|' in line:
                ASes = line.strip().split("|")
                for i in range(len(ASes)-1):
                    if (ASes[i], ASes[i+1]) not in links:
                        links.add((ASes[i], ASes[i+1]))
                        links.add((ASes[i+1], ASes[i]))
                        # if ASes[i] == '271069' or ASes[i]=='267094':
                        #     print('fuck u 271069,267094')
                # for i in range(len(ASes)-1,0,-1):
                #     if (ASes[i], ASes[i-1]) not in links:
                #         links.add((ASes[i], ASes[i-1]))
        p1 = time.time()
        print(f'got link: {p1-start}s')
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
            node_fea.loc[k,'distance'] = int(sum(shortest_distance_list[k])/float(len(shortest_distance_list[k])))
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
        p2 = time.time()
        print(f'got distance: {p2-p1}s')
        #每个link被多少个vp观测到
        vp_cnt = defaultdict(set)
        for line in lines:
            if '|' in line:
                ASes = line.strip().split("|")
                vp = ASes[0]
                for i in range(len(ASes)-1):
                    vp_cnt[(ASes[i], ASes[i+1])].add(vp)
                    vp_cnt[(ASes[i+1], ASes[i])].add(vp)
        for link in vp_cnt:
            vp_cnt[link] = len(vp_cnt[link])
        p3 = time.time() 
        print(f'got vp: {p3-p1}s')
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

        elif peeringdb_file.endswith('sqlite3'):
            conn = sqlite3.connect(peeringdb_file)
            c = conn.cursor()
            for row in c.execute("SELECT asn, ixlan_id FROM 'peeringdb_network_ixlan'"):
                AS, ixp = row[0], row[1]
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
        elif peeringdb_file.endswith('sqlite3'):
            conn = sqlite3.connect(peeringdb_file)
            c = conn.cursor()
            for row in c.execute("SELECT local_asn, fac_id FROM 'peeringdb_network_facility'"):
                AS, facility = row[0], row[1]
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
        p4 = time.time()
        print(f'got peer: {p4-p3}s')

        #label
        with open(ap_res_file,'r') as f:
            result=dict()
            for line in f:
                if line.startswith('#'):
                    continue
                line = line.strip().split('|')
                link=(line[0],line[1])
                rel = int(line[2])
                result[link]=rel
        label = defaultdict(int)
        for link in links:
            if link not in result:
                label[link] = 3
            else:
                label[link] = result[link]+1
        link_arr = []
        for link in links:
            skip = False
            for i in list(node_fea.loc[link[0]]):
                if np.isnan(i):
                    skip = True
                    break
            for i in list(node_fea.loc[link[1]]):
                if np.isnan(i):
                    skip = True
                    break
            if skip:
                continue
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
        link_fea.to_csv(output_file,index=False)
        end  = time.time() 
        print(f'done: {end-p4}s,{end-start}s')


class Links(object):
    def __init__(self, org_name, peering_name,rel_file,prob_file,path_file,version=4):
        self.rel_file=rel_file
        self.prob_file=prob_file
        self.path_file=path_file
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
        self.tier = Hierarchy(self.prob_file,version)

        self.version=version
        self.clique = set(['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956'])
        self.clique_v6 = ['174', '1299', '3356', '6057', '6939', '9002', '24482', '35280', '37468', '39533']

        self.ingestProb()
        self.extractSiblings()

    def ingestProb(self):
        self.edge_finish = set()
        #TODO
        with open(self.rel_file) as f:
            for line in f:
                if not line.startswith('#'):
                    [asn1, asn2, rel] = line.strip().split('|')
                    self.edge_finish.add((asn1, asn2))
                    self.edge_finish.add((asn2, asn1))
        #TODO
        with open(self.prob_file) as f:
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
        with open(self.path_file) as f:
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
                    if trs ==0:
                        tripletRel[edge][i][j] = 0
                    else:
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

        elif self.peering_name.endswith('sqlite3'):
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
        theclique=None
        if self.version == 4:
            theclique=self.clique
        else:
            theclique=self.clique_v6
        theclique = ['174', '209', '286', '701', '1239', '1299', '2828', '2914', '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
        for c in theclique:
            if c not in g:
                theclique.remove(c)
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


class Stage2():
    def __init__(self):
        self.params = {
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
            

    def output(self,final_prob, link,output_path):
        link_infer = list(link.edge_infer)
        link_finish = list(link.edge_finish)
        link_all = link_finish + link_infer

        outputRel = open(output_path, 'w')
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

    def BayesNetwork(self,link,output_file):
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
        self.output(final_prob, link,output_file)

    def BN_go(self,org_name,peering_name,rel_file,prob_file,path_file,output_file,version=4):
        link = Links(org_name, peering_name,rel_file,prob_file,path_file,version)
        link.constructAttributes()
        self.BayesNetwork(link,output_file)

    def GetCred(self,y_fraud_train,K,ind):
        y_fraud_train = np.array(y_fraud_train)
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

    def pro_knn_trains(self,X,Y,T,pred_X,ind,K,epoch):
        Cred = self.GetCred(Y,K,ind)
        print("cred done!")
        #model = lgb.LGBMClassifier(params)
        N = len(T)
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
            model=lgb.train(self.params,train_data,num_round,valid_sets=[validation_data],early_stopping_rounds = 100)

            # dtrain=xgb.DMatrix(X_train,label=y_train)
            # dtest=xgb.DMatrix(X_test,label=y_test)
            # evallist = [(dtest, 'eval'), (dtrain, 'train')]
            # model=xgb.train(parms,dtrain,num_round,evals=evallist,early_stopping_rounds=100)
            pred = model.predict(pred_X)
            infer_res.append(pred)
        return infer_res

    def NN_go(self,input_file,output):
        p1 = time.time()
        df = pd.read_csv(input_file)

        trust = df.loc[df['label']!=3]
        trust_Y=trust['label'].astype(int).values
        # trust_x=trust.drop(['link','label'],axis = 1).astype(float)
        # trust_X = preprocessing.MinMaxScaler().fit_transform(trust_x.values)

        ud = df.loc[df['label']==3]
        ud_Y=ud['label'].astype(int).values

        trust_X=[]
        ud_X=[]
        def safe_drop(df,key):
            ch = np.array(df[key].astype(float))
            if np.isnan(ch).any():
                for i in np.nditer(np.where(np.isnan(ch))):
                    df = df.drop([i],axis=0)
            return df

        Y = df['label'].astype(int).values
        x = df.drop(['link','label'],axis = 1).astype(float)
        X = preprocessing.MinMaxScaler().fit_transform(x.values)
        ud_loc2idx=[]
        for loc,idx in enumerate(trust.index):
            trust_X.append(X[idx])
        for loc, idx in enumerate(ud.index):
            ud_X.append(X[idx])
            ud_loc2idx.append(idx)
        print(len(ud_X))
        print('read labels')
        print('constructing kdtree')
        neigh = NearestNeighbors(n_neighbors = 100, algorithm = 'kd_tree',n_jobs = 4)
        neigh.fit(trust_X)
        print('construction complete')
        print('searching knn')
        dis,ind = neigh.kneighbors(ud_X,100)
        p2 = time.time()
        print(f'searched knn: {p2-p1}s')
        k = 50
        epoch = 10
        y_prob1= self.pro_knn_trains(trust_X,trust_Y,ud_X,ud_X,ind,k,epoch)
        p3 = time.time()
        print(f'outputing: {p3-p2}')
        res = np.array(y_prob1)
        res = np.mean(res,axis=0)
        res = np.argmax(res,axis=1)
        final = df[['link','label']]
        for loc, idx in enumerate(ud_loc2idx):
            final.at[idx,'label']=res[loc]

        f = open(output,'w')
        linkset=set()
        for index,row in final.iterrows():
            link = row['link']
            rel = row['label']
            a1=link[0].strip()
            a2=link[1].strip()
            a1,a2 = link.split(',')
            a1= a1.replace('(','')
            a1=a1.replace(')','')
            a1=a1.replace('\'','')
            a1=a1.replace('\\n','')
            a2=a2.replace('(','')
            a2=a2.replace(')','')
            a2=a2.replace('\'','')
            a2=a2.replace('\\n','')
            a1=a1.strip()
            a2=a2.strip()
            rel-=1
            if (a1,a2) in linkset:
                continue
            f.write(f'{a1}|{a2}|{rel}\n')
            linkset.add((a1,a2))
            linkset.add((a2,a1))
        f.close()
        p4 = time.time()
        print(f'output time: {p4-p3}s')
        print(f'finished computation {input_file}: {p4-p1}s')



def slow_start():
    while True:
        a = input('continue?')
        if a == 'y':
            break
        elif a =='n':
            quit()


if __name__=='__main__':
    print('start preview')
    struc = Struc()
    struc.read_irr(irr_file)
    path_file='/home/lwd/RIB.test/path.test/pc20201201.v4.u.path.clean'
    peeringdb_file='/home/lwd/Result/auxiliary/peeringdb.sqlite3'
    name = '/home/lwd/Result/vote/apv/ap2_apv.rel'
    tmp = name.split('/')[-1]
    outname=tmp.replace('.rel','.fea.csv')
    outname=join('/home/lwd/Result/AP_working',outname)

    inf = '/home/lwd/RIB.test/path.test/pc20201201.v4.u.path.clean'
    outf = '/home/lwd/Result/auxiliary/pc20201201.v4.half.sap2out'
    # inf = '/home/lwd/Result/TS_working/path_20201201_vp0.path'
    # outf = '/home/lwd/Result/TS_working/rel_20201201_vp0.ap2.half'
    struc.apollo_it(inf,outf)

    # path_file='/home/lwd/RIB.test/path.test/pc20201201.v4.u.path.clean'
    # oneday = set()
    # pf = open(path_file,'r')
    # for line in pf:
    #     if line.startswith('#'):
    #         continue
    #     ASes = line.strip().split('|')
    #     for i in range(len(ASes)-1):
    #         oneday.add((ASes[i],ASes[i+1]))
    #         oneday.add((ASes[i+1],ASes[i]))
    # print('one',len(oneday))
    # path_file='/home/lwd/RIB.test/path.test/pc202012.v4.u.path.clean'
    # allday = set()
    # pf = open(path_file,'r')
    # for line in pf:
    #     if line.startswith('#'):
    #         continue
    #     ASes = line.strip().split('|')
    #     for i in range(len(ASes)-1):
    #         allday.add((ASes[i],ASes[i+1]))
    #         allday.add((ASes[i+1],ASes[i]))
    # print('all',len(allday))

    # outname= join('/home/lwd/Result/NN','ap2_apv_nn_pv.rel')
    # checke('/home/lwd/Result/AP_working/ap2_apv.fea.csv')
    # NN_go('/home/lwd/Result/AP_working/ap2_apv.fea.csv',outname)


if __name__=='__main__':
    quit()
    print('start v6')

    v6infer=True
    v6vote=True
    v6prob=False
    v6bn=True

    print(f'v6 infer {v6infer}')
    print(f'v6 vote {v6vote}')
    print(f'v6 prob {v6prob}')
    print(f'v6 bn {v6bn}')

    irr_file='/home/lwd/Result/auxiliary/irr.txt'
    boost_file='/home/lwd/Result/auxiliary/pc20201201.v4.arout'
    s_dir='/home/lwd/RIB.test/path.test'
    r_dir='/home/lwd/Result'


    boost_file=join(auxiliary,f'pc20201201.v6.arout')
    path_file =join(pure_path_dir,'pc20201201.v6.u.path.clean')
    ar_version=join(abspath('./TopoScope'),'asrank_irr.pl')
    thisround = v6vpd.split('/')[-1]
        #V6SET
    thisround = 'tmp_d5_30'
    fulld = join('/home/lwd/Result',thisround)
    fullv = join('/home/lwd/Result/vote',thisround)
    if thisround!=v6vpd.split('/')[-1]:
        print(f'special run at {thisround}')
        os.system(f'mkdir /home/lwd/Result/{thisround}')
        os.system(f'mkdir /home/lwd/Result/vote/{thisround}')
    else:
        print(f'normal run at {thisround}')
    checke(irr_file)
    checke(boost_file)
    checke(v6vpd)
    checke(path_file)
    checke(ar_version)
    checke(tswd)
    checke(apwd)
    date = '20201201'
    print(f'date:{date}')
    struc = Struc()
    struc.read_irr(irr_file)
#infer
    if v6infer:
        print('NOW infer')
        if not checke(boost_file):
            struc.boost(ar_version,path_file,boost_file)
        struc.get_relation(boost_file)
        struc.cal_hierarchy(6)
        struc.set_VP_type(path_file,6)
        struc.clean_vp(fulld)
        if len(os.listdir(fulld)):
            print('unclean')
        print(os.listdir(fulld))
                #V6SET
        struc.divide_VP(30,fulld,date)
        struc.infer_ar(fulld,ar_version,date)
        files = os.listdir(fulld)
        in_files = []
        out_files = []
        for file in files:
            if file.startswith('path'):
                in_files.append(join(fulld,file))
                oname= file.split('.')[0]
                oname = oname.replace('path','rel')
                out_files.append(join(fulld,oname))
    
        def use(args):
            args[0](args[1],args[2],args[3],args[4])
    
        args = []
        for ii,oo in zip(in_files,out_files):
            # checke(ii)
            # checke(oo)
            args.append([struc.apollo_it,ii,oo+'.ap2',5,6])
    
        args.append([struc.c2f_strong,join(pure_path_dir,'pc20201201.v6.u.path.clean'),join(fulld,'rel_20201201.stg'),1,6])
        with multiprocessing.Pool(96) as pool:
            pool.map(use,args)
#vote
    if v6vote:
        print('NOW vote')
        files = os.listdir(fulld)
        file_num = 0
        for file  in files:
            if file.endswith('.path'):
                file_num+=1

        file_list=[f'/home/lwd/Result/{thisround}/rel_20201201_vp{i}.ap2' for i in range(0,file_num)]
        output_file=f'/home/lwd/Result/vote/{thisround}/ap2_vpg_v6.rel'
        struc_ap_tsv = Struc()
        struc_ap_tsv.read_irr(irr_file)
        struc_ap_tsv.topoFusion = TopoFusion(14,dir,'wholemonth',path_file)
        for file in file_list:
            checke(file)
        struc_ap_tsv.topoFusion.vote_among(file_list,output_file)

        file_list=[f'/home/lwd/Result/{thisround}/rel_20201201_vp{i}.ar' for i in range(0,file_num)]
        output_file=f'/home/lwd/Result/vote/{thisround}/ar_vpg_v6.rel'
        struc_ap_tsv = Struc()
        struc_ap_tsv.read_irr(irr_file)
        struc_ap_tsv.topoFusion = TopoFusion(14,dir,'wholemonth',path_file)
        for file in file_list:
            checke(file)
        struc_ap_tsv.topoFusion.vote_among(file_list,output_file)
#prob
    if v6prob:
        print('NOW prob')
        stg_file = join(fulld,'rel_20201201.stg')
        outf = join(fullv,'stg.rel')
        os.system(f'cp {stg_file} {outf}')
        stg_file=[outf]
        struc.topoFusion = TopoFusion(1,dir,'some',path_file=path_file)
        struc.topoFusion.prob_among(stg_file,outf)
#bn
    if v6bn:
        print('NOW bn')
        org_name= join(auxiliary,'20201001.as-org2info.txt')
        peering_name= join(auxiliary,'peeringdb.sqlite3')

        tsfiles=[
        # f'/home/lwd/Result/vote/{thisround}/ap2_vpg_v6.rel',
        f'/home/lwd/Result/vote/{thisround}/ar_vpg_v6.rel',
        # f'/home/lwd/Result/vote/{thisround}/stg.rel',
        ]

        rels = tsfiles 
        checke(org_name)
        checke(peering_name)
        path_file='/home/lwd/RIB.test/path.test/pc202012.v4.u.path.clean'

        def use_bn_go(args):
            start = time.time()
            func = args[0]
            org = args[1]
            peer = args[2]
            rel = args[3]
            prob = args[4]
            path = args[5]
            out = args[6]
            func(org,peer,rel,prob,path,out)
            end = time.time()
            print(f'done compute {rel}, takes {end-start}s')

        mp_args=[]
        for name in rels:
            print(f'adding {name}')
            checke(name)
            outname = name.strip().split('/')[-1]
            #V6SET
            outname = outname+'_ad72.bn_0.5_30'
            # outname = outname+'.bn_ad85'
            outname = join('/home/lwd/Result/BN',outname)
            checke(name)
            checke(name+'.prob')
            mp_args.append([BN_go,org_name,peering_name,name,name+'.prob',path_file,outname,6])
            # BN_go(org_name,peering_name,name,name+'.prob',path_file,outname)

        with multiprocessing.Pool(50) as pool:
            pool.map(use_bn_go,mp_args)
    if thisround!=v6vpd.split('/')[-1]:
        os.system(f'rm -r /home/lwd/Result/{thisround}')
        os.system(f'rm -r /home/lwd/Result/vote/{thisround}')

