import os
import re
import logging
import resource
import networkx as nx
import json
import pandas as pd
import time

from random import shuffle
from collections import defaultdict
from os.path import abspath, join, exists
from networkx.algorithms.centrality import group
from TopoScope.topoFusion import TopoFusion
from rib_to_read import url_form, download_rib, worker, unzip_rib
from logging import warn,debug,info
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
            pf = open(path_file)
            for line in pf:
                if line.startswith('#'):
                    continue
                ASes = line.split('|')
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
        wf = open(output_file)
        for link,rel in link_rel_c2f.items():
            if rel != 4:
                line = f'{link[0]}|{link[1]}|{rel}\n'
                wf.write(line)
        wf.close()

    def apollo_it(self, path_files, output_file):
        """
        core to leaf followed by apollo iteration
        """    
        link_rel_ap = dict()
        non_t1 =list()
        for path_file in path_files:
            pf = open(path_file)
            for line in pf:
                if line.startswith('#'):
                    continue
                ASes = line.split('|')
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
                    #     self.link_rel_ap\.setdefault((ASes[i],ASes[i+1]),0)
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
                        link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                if idx_1 !=0:
                    for i in range(idx_0-1):
                        link_rel_ap.setdefault((ASes[i],ASes[i+1]),1)
                if idx_0 !=0:
                    if idx_0>=2:
                        for i in range(idx_0-1):
                            link_rel_ap.setdefault((ASes[i],ASes[i+1]),1)
                    if idx_0<=len(ASes)-2:
                        for i in range(idx_0+1,len(ASes)-1):
                            link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
        wf = open(output_file)
        for link,rel in self.link_rel_ap.items():
            if rel != 4:
                line = f'{link[0]}|{link[1]}|{rel}\n'
                wf.write(line)
        wf.close()


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
    def infer_AP(self,path_file,peeringdb_file,AP_stage1_file):
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

    def vote_ap(self,file_list,filename):
        # vote from all files
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


class Infer():
    def __init__(self) -> None:
        pass

test = False

vote = True

cross = False

simple = False

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
    for n in tsfiles:
        if n.endswith('.ar'):
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


    # tsvote 
    struc.topoFusion = TopoFusion(4,dir,date)

    file_list=[f'/home/lwd/Result/TS_working/rel_wholemonth_vp{i}.ar' for i in range(0,14)]
    # input(file_list)
    output_file='/home/lwd/Result/vote/tsv/tsf_month.rel'
    for file in file_list:
        checke(file)
    struc.topoFusion.vote_among(file_list,output_file)

    quit()

    # ts file, ts vote
    for date,files in _tsfiles.items():
        print(date,files)
        outf = join(votd,'tsv',f'tsf_{date}.rel')
        struc.vote_simple_ts(tswd,date, files,outf)

    # ap file, ap vote

    # _apfiles = ['/home/lwd/Result/vote/tsv/tsf_20201201.rel',
    # '/home/lwd/Result/vote/tsv/tsf_20201208.rel',
    # '/home/lwd/Result/vote/tsv/tsf_20201215.rel',
    # '/home/lwd/Result/vote/tsv/tsf_20201222.rel',]
    # outf = join(votd,'apv','tsf_apf.rel')
    # struc.vote_ap(_apfiles,outf)

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
    for name in tsls:
        if name.endswith('path'):
            res = re.match(r'^path_(*).path',n)
            if res is not None:
                tmp = res.group(1)
                nn = join(tswd,name)
                in_files.append([nn])
                nn = join(tswd,f'rel_{tmp}.cf')
                out_files.append(nn)
                nn = join(tswd,f'rel_{tmp}.apr')
                out_files_ap.append(nn)
            else:
                continue
    # vg vb
    for in_file,out_file in zip(in_files,out_files):
        struc.core2leaf(in_file,out_file)
        struc.apollo_it(in_file,out_file)

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
        '/home/lwd/Result/AP_working/rel_20201201.apr',
        '/home/lwd/Result/AP_working/rel_20201208.apr',
        '/home/lwd/Result/AP_working/rel_20201215.apr',
        '/home/lwd/Result/AP_working/rel_20201222.apr',
        ]
    for in_file,out_file in zip(in_files,out_files):
        struc.core2leaf([in_file],out_file)
        struc.apollo_it(in_file,out_file)

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