import os
import multiprocessing
import time
from collections import defaultdict
import logging
import resource
from math import ceil
import re
import traceback
from numpy import array,argsort,intersect1d,take,nonzero,sum
# from scipy.stats import entropy
import networkx as nx


resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = os.path.abspath(os.path.join('../log',f'log_lit_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)



def ltop(num,idxs,cont):
    print(f'look top {cont}')
    cnt = 0
    for idx in reversed(idxs):
        print(f'{asns[idx]}:{cont[idx]}')
        cnt+=1
        if cnt >num:
            break



class Hierarchy():
    def __init__(self,relfile,version):
        self.peer=defaultdict(set)
        self.provider=defaultdict(set)
        self.customer=defaultdict(set)
        self.graph = nx.Graph()
        with open(relfile) as f:
            for line in f:
                if '#' in line:
                    continue
                tmp=line.strip().split('|')
                asn1 = int(tmp[0])
                asn2 = int(tmp[1])
                rel = tmp[2]
                self.graph.add_edge(asn1, asn2)
                if rel == '0':
                    self.peer[asn1].add(asn2)
                    self.peer[asn2].add(asn1)
                elif rel == '1':
                    self.customer[asn2].add(asn1)
                    self.provider[asn1].add(asn2)
                elif rel == '-1':
                    self.customer[asn1].add(asn2)
                    self.provider[asn2].add(asn1)
            if version == 4:
                self.clique = set([174, 209, 286, 701, 1239, 1299, 2828, 2914, 3257, 3320, 3356, 3491, 5511, 6453, 6461, 6762, 6830, 7018, 12956])
            else:   
                self.clique = set([174, 3356, 3549, 6939, 24482, 35280, 39533])
            self.allNodes = set()
            self.high = set()#1
            self.mid = set()
            self.mid1 = set()
            self.mid2 = set()
            self.mid3 = set()
            self.mid4 = set()
            self.low = set()
            self.stub = set()#8
            for node in self.clique:
                for cus in self.customer[node]:
                    self.high.add(cus)
                    self.allNodes.add(cus)
                    # if self.graph.degree(cus) > 0:
                    #     self.high.add(cus)
                    #     self.allNodes.add(cus)
                    # else:
                    #     self.mid.add(cus)
                    #     self.allNodes.add(cus)
                self.allNodes.add(node)
            for node in self.high:
                for cus in self.customer[node]:
                    if cus in self.allNodes:
                        continue
                    self.mid.add(cus)
                    self.allNodes.add(cus)
            for node in self.mid:
                for cus in self.customer[node]:
                    if cus in self.allNodes:
                        continue
                    self.mid1.add(cus)
                    self.allNodes.add(cus)
            for node in self.mid1:
                for cus in self.customer[node]:
                    if cus in self.allNodes:
                        continue
                    self.mid2.add(cus)
                    self.allNodes.add(cus)
            for node in self.mid2:
                for cus in self.customer[node]:
                    if cus in self.allNodes:
                        continue
                    self.mid3.add(cus)
                    self.allNodes.add(cus)
            for node in self.mid3:
                for cus in self.customer[node]:
                    if cus in self.allNodes:
                        continue
                    self.mid4.add(cus)
                    self.allNodes.add(cus)
            for node in self.graph.nodes():
                if node in self.allNodes:
                    continue
                if not self.customer[node]:
                    self.stub.add(node)
                else:
                    self.low.add(node) 
                self.allNodes.add(node)
        print(f'Hinfo: clique:{len(self.clique)},high:{len(self.high)},mid:{len(self.high)}/{len(self.mid)}/{len(self.mid1)}/{len(self.mid2)},low:{len(self.mid3)},stub:{len(self.stub)}')

    def qh(self, AS):
        AS = int(AS) if type(AS) == str else AS
        if AS in self.clique:
            return 0
        if AS in self.high:
            return 1
        if AS in self.mid:
            return 2
        if AS in self.mid1:
            return 3
        if AS in self.mid2:
            return 4
        if AS in self.mid3:
            return 5
        if AS in self.mid4:
            return 6
        if AS in self.low:
            return 7
        if AS in self.stub:
            return 8
        return -1

    def ql(self,ASes):
        ans = [0,0,0,0,0,0,0,0,0,0]
        for AS in ASes:
            AS = int(AS)
            res = self.qh(AS)
            if res != -1:
                ans[res]+=1
            else:
                ans[9]+=1
        return ans

    def select(self,ASes,tier):
        if tier ==9:
            tier = -1
        else:
            pass
        ans = []
        for idx,AS in enumerate(ASes):
            AS = int(AS)
            res = self.qh(AS)
            if res == tier:
                # if AS in self.allNodes:
                #     print(f'{AS} pretend')
                ans.append(idx)
        return ans

f = open('/home/lwd/Result/update/as_event_tt_v4.txt')
asns = []
updates = []
aa=[]
aa_d=[]
wa=[]
wa_d=[]
ww=[]
aw=[]
for line in f:
    cot = line.strip().split('|')
    asns.append(cot[0])
    updates.append(int(float(cot[1])))
    about_aa = cot[2].split('_')
    aa.append(int(float(about_aa[1])))
    aa_d.append(int(float(about_aa[3])))
    about_aw = cot[3].split('_')
    aw.append(int(float(about_aw[1])))
    about_ww = cot[4].split('_')
    ww.append(int(float(about_ww[1])))
    about_wa = cot[5].split('_')
    wa.append(int(float(about_wa[1])))
    wa_d.append(int(float(about_wa[3])))

asns=array(asns)
updates=array(updates)
aa=array(aa)
aa_d=array(aa_d)
wa=array(wa)
wa_d=array(wa_d)
ww=array(ww)
aw=array(aw)
updates_idx=argsort(updates)
aa_idx=argsort(aa)
aa_d_idx=argsort(aa_d)
wa_idx=argsort(wa)
wa_d_idx=argsort(wa_d)
ww_idx=argsort(ww)
aw_idx=argsort(aw)


# ltop(20,updates_idx,updates)
# ltop(20,aa_idx,aa)
# ltop(20,aa_d_idx,aa_d)


f6 = open('/home/lwd/Result/update/as_event_tt_v6.txt')

asns6 = []
updates6 = []
aa6=[]
aa_d6=[]
wa6=[]
wa_d6=[]
ww6=[]
aw6=[]
for line in f6:
    cot = line.strip().split('|')
    asns6.append(cot[0])
    updates6.append(int(float(cot[1])))
    about_aa = cot[2].split('_')
    aa6.append(int(float(about_aa[1])))
    aa_d6.append(int(float(about_aa[3])))
    about_aw = cot[3].split('_')
    aw6.append(int(float(about_aw[1])))
    about_ww = cot[4].split('_')
    ww6.append(int(float(about_ww[1])))
    about_wa = cot[5].split('_')
    wa6.append(int(float(about_wa[1])))
    wa_d6.append(int(float(about_wa[3])))

asns6=array(asns6)
updates6=array(updates6)
aa6=array(aa6)
aa_d6=array(aa_d6)
wa6=array(wa6)
wa_d6=array(wa_d6)
ww6=array(ww6)
aw6=array(aw6)
updates_idx6=argsort(updates6)
aa_idx6=argsort(aa6)
aa_d_idx6=argsort(aa_d6)
wa_idx6=argsort(wa6)
wa_d_idx6=argsort(wa_d6)
ww_idx6=argsort(ww6)
aw_idx6=argsort(aw6)

rel4 = '/home/lwd/Result/BN/ar_tsv.rel.bn'
rel6 = '/home/lwd/Result/BN/ar_vpg_v6.rel.bn'
h4 = Hierarchy(rel4,4)
h6 = Hierarchy(rel6,6)
print(h4.ql(asns))
print(h6.ql(asns6))
for tn in h4.ql(asns):
    print(f'{tn/len(asns):.6f}',end=' ')
print('')
for tn in h6.ql(asns6):
    print(f'{tn/len(asns6):.6f}',end=' ')
print('')
def kl(arr1,arr2):
    return 1
    # return entropy(arr1, arr2)

def check_kl(arr1,arr2,cont1,cont2):
    nzc1 = nonzero(cont1)[0]
    nzc2 = nonzero(cont2)[0]
    nzarr1 = take(arr1,nzc1)
    nzarr2 = take(arr2,nzc2)
    nzcont1 = take(cont1,nzc1)
    nzcont2 = take(cont2,nzc2)
    print(f'non zeros a:\033[7m{len(nzarr1):>7}\033[0m \033[7mb:{len(nzarr2):>7}\033[0m',end=' ')
    a,c1,c2= intersect1d(nzarr1,nzarr2,return_indices=True)
    innzarr1 = take(nzarr1,c1)
    innzarr2 = take(nzarr2,c2)
    print(f'intersect \033[7m{len(innzarr1):>7}\033[0m',end=' ')
    # for som in innzarr1:
    #     print(som)
    ncont1 = take(nzcont1,c1)
    ncont2 = take(nzcont2,c2)
    print(f'kl:\033[7m{kl(ncont1,ncont2)}\033[0m')
    print(f'Before in v4 struct:{h4.ql(nzarr1)}, num{sum(nzcont1):>10} per{sum(nzcont1)/sum(h4.ql(nzarr1)) if sum(h4.ql(nzarr1)) != 0 else 0 }')
    print(f'Before in v6 struct:{h6.ql(nzarr2)}, num{sum(nzcont2):>10} per{sum(nzcont2)/sum(h6.ql(nzarr2)) if sum(h6.ql(nzarr2)) != 0 else 0 }')
    print(f'After in v4 struct:{h4.ql(innzarr1)}, num{sum(ncont1):>10} per{sum(ncont1)/sum(h4.ql(innzarr1)) if sum(h4.ql(innzarr1)) != 0 else 0 }')
    print(f'After in v6 struct:{h6.ql(innzarr2)}, num{sum(ncont2):>10} per{sum(ncont2)/sum(h6.ql(innzarr2)) if sum(h6.ql(innzarr2)) != 0 else 0 }')

print(f'as number v4:{len(asns)} v6:{len(asns6)}')


# print('check common as\'s kl divergence over updates')
# check_kl(asns,asns6,updates,updates6)
# print('check common as\'s kl divergence over aa updates')
# check_kl(asns,asns6,aa,aa6)
# print('check common as\'s kl divergence over aa diff updates')
# check_kl(asns,asns6,aa_d,aa_d6)
# print('check common as\'s kl divergence over ww updates')
# check_kl(asns,asns6,ww,ww6)
# print('check common as\'s kl divergence over ww updates')
# check_kl(asns,asns6,aw,aw6)
# print('check common as\'s kl divergence over wa updates')
# check_kl(asns,asns6,wa,wa6)
# print('check common as\'s kl divergence over wa diff updates')
# check_kl(asns,asns6,wa_d,wa_d6)


def check_kl_t(arr1,arr2,cont1,cont2,tier):
    if tier == -1:
        check_kl(arr1,arr2,cont1,cont2)
        return
    tid1 = h4.select(arr1,tier)
    tid2 = h6.select(arr2,tier)
    tarr1 = take(arr1,tid1)
    tarr2 = take(arr2,tid2)
    tcont1 = take(cont1,tid1)
    tcont2 = take(cont2,tid2)
    check_kl(tarr1,tarr2,tcont1,tcont2)

def check_kl_all(tier):
    print('check common as\'s kl divergence over updates')
    check_kl_t(asns,asns6,updates,updates6,tier)
    print('check common as\'s kl divergence over aa updates')
    check_kl_t(asns,asns6,aa,aa6,tier)
    print('check common as\'s kl divergence over aa diff updates')
    check_kl_t(asns,asns6,aa_d,aa_d6,tier)
    print('check common as\'s kl divergence over ww updates')
    check_kl_t(asns,asns6,ww,ww6,tier)
    print('check common as\'s kl divergence over aw updates')
    check_kl_t(asns,asns6,aw,aw6,tier)
    print('check common as\'s kl divergence over wa updates')
    check_kl_t(asns,asns6,wa,wa6,tier)
    print('check common as\'s kl divergence over wa diff updates')
    check_kl_t(asns,asns6,wa_d,wa_d6,tier)


check_kl_all(-1)
# for i in range(0,10):
    # print(f"### select{i}")
    # check_kl_all(i)