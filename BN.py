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

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = abspath(join('./log',f'log_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)

from link import Links

class BN():
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
