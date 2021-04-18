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
        pass
            
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

