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

# from TopoScope.topoFusion import TopoFusion
from location import *
from hierarchy import Hierarchy

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = abspath(join('./log',f'log_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)

from collections import defaultdict
import numpy as np
import os

class TopoFusion(object):
    def __init__(self, fileNum, dir_name,date, path_file=None):
        self.fileNum = fileNum
        self.dir = dir_name
        self.date = date
        self.prob = defaultdict(lambda: np.array([0.0, 0.0, 0.0]))
        self.linknum = defaultdict(int)

        self.bg= False
        self.bg_link=set()
        if path_file:
            self.background_link(path_file)

    def background_link(self, path_file):
        with open(path_file,'r') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                
                ASes = line.strip().split('|')
                for i in range(len(ASes)-1):
                    asn1 = int(ASes[i])
                    asn2 = int(ASes[i+1])
                    self.bg_link.add((asn1,asn2))
                    self.bg_link.add((asn2,asn1))
        f.close()
        self.bg = True

    def peek(self):
        print(len(self.bg_link))

    def prob_among(self,file_list,output_file):
        for file in file_list:
            with open(file) as f:
                for line in f:
                    if line.startswith('#'):
                        continue
                    [asn1, asn2, rel] = line.strip().split('|')
                    asn1, asn2 = int(asn1), int(asn2)
                    if rel == '0':
                        self.prob[(asn1, asn2)] += np.array([1.0, 0.0, 0.0])
                        self.prob[(asn2, asn1)] += np.array([1.0, 0.0, 0.0])
                    elif rel == '1':
                        self.prob[(asn1, asn2)] += np.array([0.0, 0.0, 1.0])
                        self.prob[(asn2, asn1)] += np.array([0.0, 1.0, 0.0])
                    elif rel == '-1':
                        self.prob[(asn1, asn2)] += np.array([0.0, 1.0, 0.0])
                        self.prob[(asn2, asn1)] += np.array([0.0, 0.0, 1.0])
                    
                    self.linknum[(asn1, asn2)] += 1
                    self.linknum[(asn2, asn1)] += 1
        self.writeProbf(output_file)

    def vote_among(self,file_list,output_file):
        for file in file_list:
            with open(file) as f:
                for line in f:
                    if line.startswith('#'):
                        continue
                    [asn1, asn2, rel] = line.strip().split('|')
                    asn1, asn2 = int(asn1), int(asn2)
                    if rel == '0':
                        self.prob[(asn1, asn2)] += np.array([1.0, 0.0, 0.0])
                        self.prob[(asn2, asn1)] += np.array([1.0, 0.0, 0.0])
                    elif rel == '1':
                        self.prob[(asn1, asn2)] += np.array([0.0, 0.0, 1.0])
                        self.prob[(asn2, asn1)] += np.array([0.0, 1.0, 0.0])
                    elif rel == '-1':
                        self.prob[(asn1, asn2)] += np.array([0.0, 1.0, 0.0])
                        self.prob[(asn2, asn1)] += np.array([0.0, 0.0, 1.0])
                    
                    self.linknum[(asn1, asn2)] += 1
                    self.linknum[(asn2, asn1)] += 1
        self.writeProbf(output_file)
        #V6SET
        self.writeResultf(output_file,0.8, self.fileNum * 0.7, self.fileNum * 0.2)

    def getTopoProb(self):
        for i in range(self.fileNum):
            _filename = os.path.join(self.dir, f'rel_{self.date}_vp{i}.path')
            # _filename = self.dir + 'fullVPRel' + str(i) + '.txt'
            with open(_filename) as f:
                for line in f:
                    if line.startswith('#'):
                        continue
                    [asn1, asn2, rel] = line.strip().split('|')
                    asn1, asn2 = int(asn1), int(asn2)
                    #TODO
                    # apollo modify
                    if rel == '0':
                        self.prob[(asn1, asn2)] += np.array([1.0, 0.0, 0.0])
                        self.prob[(asn2, asn1)] += np.array([1.0, 0.0, 0.0])
                    elif rel == '1':
                        self.prob[(asn1, asn2)] += np.array([0.0, 0.0, 1.0])
                        self.prob[(asn2, asn1)] += np.array([0.0, 1.0, 0.0])
                    elif rel == '-1':
                        self.prob[(asn1, asn2)] += np.array([0.0, 1.0, 0.0])
                        self.prob[(asn2, asn1)] += np.array([0.0, 0.0, 1.0])
                    
                    self.linknum[(asn1, asn2)] += 1
                    self.linknum[(asn2, asn1)] += 1
        self.writeProb()
        self.writeResult(0.8, self.fileNum * 0.8, self.fileNum * 0.4)

    def writeProbf(self,file_name):
        alllink = set()
        fout = open(file_name+'.prob', 'w')
        for link in self.prob:
            if link in alllink:
                continue
            prob = self.prob[link]
            reverse_link = (link[1], link[0])
            seenNum = prob.sum()
            p2p, p2c, c2p = prob / seenNum

            if (p2p == p2c and p2p > c2p):
                if self.linknum[link] > self.fileNum / 2:
                    p2c += 0.001
                else:
                    p2p += 0.001
            if (p2p == c2p and p2p > p2c):
                if self.linknum[link] > self.fileNum / 2:
                    c2p += 0.001
                else:
                    p2p += 0.001
            if (p2c == c2p and p2c > p2p):
                p2c += 0.001
            if (p2c == c2p and p2c == p2p):
                if self.linknum[link] > self.fileNum / 2:
                    p2c += 0.001
                else:
                    p2p += 0.001
            
            if p2c > p2p and p2c > c2p:
                fout.write(str(link[0]) + '|' + str(link[1]) + '|-1|' + str(p2p) + '|' + str(p2c) + '|' + str(c2p) + '|' + str(self.linknum[link]) + '\n')
            elif c2p > p2p and c2p > p2c:
                fout.write(str(link[1]) + '|' + str(link[0]) + '|-1|' + str(p2p) + '|' + str(c2p) + '|' + str(p2c) + '|' + str(self.linknum[link]) + '\n')
            elif p2p > p2c and p2p > c2p:
                if link[0] < link[1]:
                    fout.write(str(link[0]) + '|' + str(link[1]) + '|0|' + str(p2p) + '|' + str(p2c) + '|' + str(c2p) + '|' + str(self.linknum[link]) + '\n')
                else:
                    fout.write(str(link[1]) + '|' + str(link[0]) + '|0|' + str(p2p) + '|' + str(c2p) + '|' + str(p2c) + '|' + str(self.linknum[link]) + '\n')

            alllink.add(link)
            alllink.add(reverse_link)
        if self.bg:
            for link in self.bg_link:
                reverse_link = (link[1], link[0])
                if link in alllink:
                    continue
                p2p = 0.0001
                p2c = 0.0001
                c2p = 0.0001
                if link[0] < link[1]:
                    fout.write(str(link[0]) + '|' + str(link[1]) + '|0|' + str(p2p) + '|' + str(p2c) + '|' + str(c2p) + '|' + str(1) + '\n')
                else:
                    fout.write(str(link[1]) + '|' + str(link[0]) + '|0|' + str(p2p) + '|' + str(c2p) + '|' + str(p2c) + '|' + str(1) + '\n')
                alllink.add(link)
                alllink.add(reverse_link)
        fout.close()

    def writeProb(self):
        file_name = os.path.join(self.dir,f'asrel_prime_prob_{self.date}.txt')
        self.writeProbf(file_name)

    def writeResultf(self, file_name, lowprob = 0.8, maxseen = 10, minseen = 4):
        alllink = set()
        fout = open(file_name, 'w')
        for link in self.prob:
            if link in alllink:
                continue
            prob = self.prob[link]
            reverse_link = (link[1], link[0])
            seenNum = prob.sum()
            p2p, p2c, c2p = prob / seenNum
            if not (p2p > lowprob or p2c > lowprob or c2p > lowprob):
                continue
            if seenNum >= maxseen:
                if p2c > p2p and p2c > c2p:
                    fout.write(str(link[0]) + '|' + str(link[1]) + '|-1\n')
                elif c2p > p2p and c2p > p2c:
                    fout.write(str(link[1]) + '|' + str(link[0]) + '|-1\n')
            if seenNum <= minseen:
                if p2p > p2c and p2p > c2p:
                    if link[0] < link[1]:
                        fout.write(str(link[0]) + '|' + str(link[1]) + '|0\n')
                    else:
                        fout.write(str(link[1]) + '|' + str(link[0]) + '|0\n')

            alllink.add(link)
            alllink.add(reverse_link)
        fout.close()

    def writeResult(self, lowprob = 0.8, maxseen = 10, minseen = 4):
        file_name = os.path.join(self.dir,f'asrel_prime_prime_{self.date}.txt')
        self.writeResultf(file_name,lowprob,maxseen,minseen)
        

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

    def c2f_loose(self, path_files, output_file, it = 5,version=4):
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

            if not convert or turn >= it:
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

    def c2f_strict(self, path_files, output_file, it = 5,version=4):
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

            if not convert or turn >= it:
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
        wf.close()
        p3= time.time()
        print(f'iteration takes {p3-p2}s')
        print(f'ap_it takes {p3-p1}s')

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

    def vote_simple_vp(self,file_num,in_files,out_file,path_file=None):
        self.topoFusion = TopoFusion(file_num,'placeholder','placeholder',path_file=path_file)
        self.topoFusion.vote_among(in_files,out_file)

    def prob_simple_vp(self,file_num,in_files,out_file,path_file=None):
        self.topoFusion = TopoFusion(file_num,'placeholder','placeholder',path_file=path_file)
        self.topoFusion.prob_among(in_files,out_file)

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

