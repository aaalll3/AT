import os
import logging
import resource
import networkx as nx
import json
import pandas as pd

from random import shuffle
from collections import defaultdict
from os.path import abspath, join
from networkx.algorithms.centrality import group
from TopoScope.topoFusion import TopoFusion
from rib_to_read import url_form, download_rib, worker, unzip_rib
from logging import warn,debug,info

# TODO
# logging|done
# path of files
# debug

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = abspath(join('.','log'))
logging.basicConfig(filename=log_location,level=logging.INFO)


# TODO
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

class Struc():
    def __init__(self,path_file=None,boost_file=None,irr_file=None) -> None:
        # base structure of graph
        debug('[Struc.init]initializing',stack_info=True)
        self.clique = set(['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956'])
        self.tier_1 = ['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '4436', '5511', '6453', '6461', '6762', '7018', '12956', '3549']
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
    
    def process_line_AP(self,ASes):
        info(f'[Struc.process_line_AP]AP: process AS path {ASes}')
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
    def write_AP(self, AP_stage1_file ):
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
        wp_file = join(AP_stage1_file,'wrong')
        f = open(AP_stage1_file,'w')
        f.write(str(result))
        f.close()
        f = open(wp_file,'w')
        f.write(str(self.wrong_path))
        f.close()

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

    def apollo(self,path_file,AP_stage1_file):
        debug('[Struc.apollo]',stack_info=True)
        info('[Struc.apollo]run c2f for AP')
        with open(path_file) as f:
            for line in f:
                ASes = line.strip().split('|')
                self.process_line_AP(ASes) # Apollo
        self.write_AP(AP_stage1_file)  # part1 over for Apollo

    # irr_file
    def read_irr(self,irr_path):
        debug('[Struc.read_irr]',stack_info=True)
        info('[Struc.read_irr]read irr info')
        with open(irr_path,'r') as f:
            lines = f.readlines()
        for line in lines:
            tmp = line.strip().split('|')
            if tmp[2] == '1':
                self.irr_c2p.add((tmp[0],tmp[1]))
            if tmp[2] == '0':
                self.irr_p2p.add((tmp[0],tmp[1]))

    def boost(self,path_file):
        debug('[Struc.boost]',stack_info=True)
        info('[Struc.boost]run initial asrank for TS')
        name = path_file.split('.')[0]
        dst = name+'.rel'
        command= f'perl asrank.pl {path_file} > {dst}'
        os.system(command)
        return dst

    # TS divided file
    def divide_TS(self,group_size,dir):
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
        for i in range(pre_num/group_size):
            self.VPGroup.append(pre_VP[i:i+25])
        for i in range(sec_num/group_size):
            self.VPGroup.append(sec_VP[i:i+25])
        pre_rest = pre_num%group_size
        sec_rest = sec_num&group_size
        if pre_rest + sec_rest > group_size:
            w_partial = pre_rest + sec_rest - group_size
            self.VPGroup.append(sec_VP[-sec_rest:] + pre_VP[-pre_rest:-w_partial])
            self.VPGroup.append(pre_VP[-w_partial:] + partial_VP)
        else:
            self.VPGroup.append(pre_VP + sec_VP + partial_VP)

        for i in range(len(self.VPGroup)):
            wf = open(dir + 'fullVPPath' + str(i) + '.txt','w')
            for VP in self.VPGroup[i]:
                for path in self.VP2path:
                    wf.write(path + '\n')
            wf.close()

    def infer_TS(self,dir):
        debug('[Struc.infer_TS]',stack_info=True)
        info('[Struc.infer_TS]run asrank for seperated group')
        for i in range(len(self.VPGroup)):
            os.system("perl asrank.pl " + dir + "fullVPPath" + str(i) + ".txt > " 
            + dir + "fullVPRel" + str(i) + ".txt")

    def vote_TS(self,dir):
        debug('[Struc.vote_TS]',stack_info=True)
        info('[Struc.vote_TS]')
        # vote 
        self.topoFusion= TopoFusion(self.file_num,dir)
        self.topoFusion.getTopoProb()

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
        tier1s = ['174', '209', '286', '701', '1239', '1299', '2828', '2914', '3257', '3320', '3356', '4436', '5511', '6453', '6461', '6762', '7018', '12956', '3549']
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

    def vote_ap(self):
        # vote from all files
        pass

class Infer():
    def __init__(self) -> None:
        pass

debugging= True

if __name__=='__main__' and debugging:

if __name__=='__main__' and not debugging:

    group_size=25
    irr_file=''
    boost_file=''
    path_file=''
    path_dir=''

    dir='data'
    ts_working_dir='TS_working'
    ap_working_dir='AP_working'

    tswd = join(dir,ts_working_dir)
    apwd = join(dir,ap_working_dir)

    # TS simple infer 

    struc =Struc()
    struc.read_irr(irr_file)
    struc.get_relation(boost_file)
    struc.cal_hierarchy()
    names = os.listdir(path_dir)
    for name in names:
        if name.endswith('.v4'):
            struc.set_VP_type(name)
            struc.divide_TS(group_size,tswd)
            struc.infer_TS(tswd)

            struc.apollo(name,apwd) # AP simple infer