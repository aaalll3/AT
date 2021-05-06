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


from location import *
from hierarchy import Hierarchy
from vpVoter import TopoFusion
from originVoter import groupVoter
from core2leaf_loose import core2leaf_loose
from core2leaf_strict import core2leaf_strict,c2f_strict_mp
from core2leaf_strong import core2leaf_strong

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = abspath(join('./log',f'log_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)

from collections import defaultdict
import numpy as np
import os

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

    def boost(self,ar_version,inf,outf):
        self.infer_ar(ar_version,inf,outf)

    def c2f_loose(self,in_file,out_file, irr_file):
        my = core2leaf_loose(in_file,out_file,irr_file)
        my.run()

    def c2f_strict(sefl,in_file,out_file,irr_file):
        my = core2leaf_strict(in_file,out_file,irr_file)
        my.run()

    def c2f_strict_mp(sefl,args):
        c2f_strict_mp(args)

    def c2f_strong(self,in_file,out_file,irr_file,it=1):
        my = core2leaf_strong(in_file,out_file,irr_file,it)
        my.run()

    def infer_ar(self,ar_version,inf,outf):
        command = f"perl {ar_version} {inf} > {outf}"
        os.system(command)   

    def vote_simple_vp(self,file_num,in_files,out_file,path_file=None):
        self.topoFusion = TopoFusion(file_num,'placeholder','placeholder',path_file=path_file)
        self.topoFusion.vote_among(in_files,out_file)

    def prob_simple_vp(self,file_num,in_files,out_file,path_file=None):
        self.topoFusion = TopoFusion(file_num,'placeholder','placeholder',path_file=path_file)
        self.topoFusion.prob_among(in_files,out_file)

    def vote_simple_ori(self,file_num,in_files,out_file,path_file=None):
        self.og = groupVoter(file_num,'palceholder',path_file=path_file)
        self.og.vote_among(in_files,out_file)

    #TODO
    def vote_simple_ori(self,file_num,in_files,out_file,path_file=None):
        self.og = groupVoter(file_num,'palceholder',path_file=path_file)
        self.og.prob_among(in_files,out_file)

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

