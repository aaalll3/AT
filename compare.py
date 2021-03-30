from ctypes import alignment
from logging import critical
import os 
import networkx as nx


from location import *
from os.path import join,abspath,exists
from collections import defaultdict
from hierarchy import Hierarchy

class comm():
    '''
    set up data for validation, comparing between different type of link
    only record one direction and check reverse link when use a link
    '''
    def __init__(self):
        # basic
        self.ti = 0 
        self.tv = 0
        self.hit = 0
        self.cover = 0
        self.valid = dict()

        # hardlink
        self.g=None
        self.hard_links=[set() for i in range(4)]
        self.critical_links=[set() for i in range(5)]
        self.link2VP=defaultdict(set)
        self.linkON=defaultdict(int) # observed number
        self.tier1=['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']

        # relation type
        self.p2c = set()
        self.c2p = set()
        self.p2p = set()
        self.hybrid = set()
        
        # pure realtion type
        self.hy01=0
        self.hyn11=0
        self.hyn10=0
        self.hyn101=0
        self.np2c=0
        self.nc2p=0
        self.np2p=0

        # hierarchy for discrime tier
        boost_file = '/home/lwd/Result/auxiliary/validation'
        self.hierarchy = Hierarchy(boost_file)

    def read(self,valid_file):

        print(f'read validation set from {valid_file}')
        vf = open(valid_file,'r')
        for line in vf:
            if line.startswith('#'):
                continue
            [asn1,asn2,rel]= line.split()
            asn1 = asn1.strip()
            asn2 = asn2.strip()
            if '{' in asn1 or '{' in asn2:
                continue
            asn1 =int(asn1)
            asn2 =int(asn2)
            self.valid[(asn1,asn2)]=rel
            if '&' in rel:
                check=set()

                rrs = rel.split('&')
                for rrr in rrs:
                    check.add(int(rrr))
                    # if int(rrr) == 1:
                    #     self.c2p.add((asn1,asn2))
                    # elif int(rrr) == 0:
                    #     self.p2p.add((asn1,asn2))
                    # elif int(rrr) == -1:
                    #     self.p2c.add((asn1,asn2))
                if -1 in check and 0 in check and 1 in check:
                    self.hyn101+=1
                elif -1 in check and 0 in check:
                    self.hyn10+=1
                elif -1 in check and 1 in check:
                    self.hyn11+=1
                elif 1 in check and 0 in check:
                    self.hy01+=1
                self.hybrid.add((asn1,asn2))
            else:
                if int(rel) == 1:
                    self.c2p.add((asn1,asn2))
                elif int(rel) == 0:
                    self.p2p.add((asn1,asn2))
                elif int(rel) == -1:
                    self.p2c.add((asn1,asn2))
            self.tv +=1
        self.np2c = len(self.p2c)
        self.np2p = len(self.p2p)
        self.nc2p = len(self.c2p)
        print(f'total link in validaton set {self.tv}')
        print(f'\033[35mvalid count\033[0m\np2c {self.np2c}\np2p {self.np2p}\nc2p {self.nc2p}\nhybrid0&1 {self.hy01}\nhybrid-1&1 {self.hyn11}\nhybrid-1&0 {self.hyn10}\nhybrid-1&0&1 {self.hyn101}')
        vf.close()

    def set_hnc_link(self,path_file):
        '''
        get hard and critical link from path
        hard link's definition's from Problink
        critical link's definition's from Toposcope
        '''
        self.g = nx.Graph()
        pf = open(path_file,'r')
        for line in pf:
            if line.startswith('#'):
                continue
            ASes = line.strip().split('|')
            for i in range(len(ASes)-1):
                self.g.add_edge(ASes[i],ASes[i+1])
                # type2: links that's not contains Tier1 AS or VP
                if i > 1 and ASes[i+1] not in self.tier1:
                    self.hard_links[2].add((ASes[i],ASes[i+1]))
                self.link2VP[(ASes[i],ASes[i+1])].add(ASes[0])
                self.linkON[(ASes[i],ASes[i+1])]+=1

        # type0: links whose max degree is less than 100
        for link in self.graph.edges:
            degrees= self.g.degree(link)
            for node,degree in degrees:
                if degree > 100:
                    self.hard_links[0].add(link)
                    break
        # type1: links that observed by less than 100 VPs and more than 50 VPs
        for link,VPset in self.link2VP.items():
            if len(VPset) >= 50 and len(VPset) <= 100:
                self.hard_links[1].add(link)
        # type2; done
        # type3: stub links, whose only adjacent node is tier1 AS
        for node in self.g.nodes:
            adj = self.g.adj[node]
            if len(adj)==1 and adj[0] in self.tier1:
                self.hard_links[3].add((node,adj[0]))
        # set critical links
        # type0: links between tier1 ASes
        tier1_num = len(self.hierarchy.clique)
        tier1_list = list(self.tier1)
        for i in tier1_num:
            for j in range(i):
                # only record one direction
                link = (tier1_list[i],tier1_list[j])
                if link in self.g.edges:
                    self.critical_links[0].add(link)
                    continue
        # type1: high tier link
        high = self.hierarchy.high
        clique = self.hierarchy.clique
        for node in high:
            for neighbor in self.g.adj[node]:
                if neighbor in high or neighbor in clique:
                    self.critical_links[1].add((node,neighbor))
        # type2: high frequency link
        for link in self.g.edges:
            if self.linkON[link]>1000:
                self.critical_links[2].add(link)
        # type3: links between tier1 ASes and low tier ASes or stub ASes
        for node in tier1_list:
            for neighbor in self.g.adj[node]:
                if neighbor in self.hierarchy.stub \
                    or (neighbor in self.hierarchy.low \
                        and self.g.degree[neighbor] < 100):
                    self.critical_links[3].add((node,neighbor))
        # type4: links between ASes with great difference of degree
        for node in tier1_list:
            for neighbor in self.g.adj[node]:
                if abs(self.g.degree[node] - self.g.degree[neighbor]) > bound:
                    self.critical_links[4].add((node,neighbor))

    def reset(self):
        self.ti = 0 
        self.hit = 0
        self.cover = 0

    def compare(self,rel_file):
        print(f'\033[4mtest relationthip {rel_file}\033[0m')
        rf = open(rel_file,'r')
        for line in rf:
            if line.startswith('#'):
                continue
            [asn1,asn2,rel]= line.split('|')
            asn1 = int(asn1)
            asn2 = int(asn2)
            linka = (asn1,asn2)
            linkb = (asn2,asn1)
            rra = self.valid.get(linka,None)
            rrb = self.valid.get(linkb,None)
            rr = None
            rev= False
            if rra == None and rrb ==None:
                continue
            elif rra == None:
                rr = rrb
                rev=True
            elif rrb == None:
                rr = rra
            else:
                rr = rra
            self.cover+=1
            if '&' in rr:
                rrs = rr.split('&')
                for rrr in rrs:
                    if rev:
                        if int(rrr)==-int(rel):
                            self.hit +=1
                            break
                    else:
                        if int(rrr)==int(rel):
                            self.hit +=1
                            break
            else:
                if rev:
                    if int(rr)==-int(rel):
                        self.hit +=1
                else:
                    if int(rr)==int(rel):
                        self.hit +=1
            self.ti+=1
        print(f'result: \033[31mhit {self.hit}\033[0m \033[35mtotal {self.ti}\033[0m \033[36mcover {self.cover}\033[0m \033[32mhit/infer/valid {self.hit}/{self.ti}/{self.tv}\033[0m\n\033[31mprecesion {self.hit/self.ti}\033[0m \033[35mrecall {self.hit/self.tv}\033[0m \033[36mcover rate {self.cover/self.tv}\033[0m')
        rf.close()
        self.reset()

    def total_statistic(self,rel_file):
        print(f'\033[4mtest relationthip {rel_file}\033[0m')

        # in this correlation matrix, the first dimension stands for infer type,
        # which belongs to p2c,p2p or c2p. the second dimension is for validation
        # type, p2c, p2p, c2p, hybrid0&1,hybrid-1&0
        # [T,F,F,F,T,]
        # [F,T,F,T,T,]
        # [F,F,T,T,F,]
        correl_mat=[[0,0,0,0,0,0,0] for i in range(3)]

        rf = open(rel_file,'r')
        for line in rf:
            if line.startswith('#'):
                continue
            [asn1,asn2,rel]= line.split('|')
            asn1 = int(asn1)
            asn2 = int(asn2)
            linka = (asn1,asn2)
            linkb = (asn2,asn1)
            rra = self.valid.get(linka,None)
            rrb = self.valid.get(linkb,None)
            # set some varibles
            rr = None
            rev=False
            # base statistic


            if rra == None and rrb ==None:
                continue
            elif rra == None:
                rr = rrb
                rev=True
            elif rrb == None:
                rr = rra
            else:
                rr = rra
            self.cover+=1
            if '&' in rr:
                check=set()
                rrs = rr.split('&')
                for rrr in rrs:
                    if rev:
                        if int(rrr)==-int(rel):
                            self.hit +=1
                            break
                    else:
                        if int(rrr)==int(rel):
                            self.hit +=1
                            break
                _sum = -1
                for rrr in rrs:
                    check.add(int(rrr))
                if -1 in check and 0 in check:
                    _sum = 4
                elif 1 in check and 0 in check:
                    _sum = 3
                else:
                    continue
                if rev:
                    correl_mat[-int(rel)+1][_sum]+=1
                else:
                    correl_mat[int(rel)+1][_sum]+=1
            else:
                if rev:
                    correl_mat[-int(rel)+1][int(rr)+1]+=1
                    if int(rr)==-int(rel):
                        self.hit +=1
                else:
                    correl_mat[int(rel)+1][int(rr)+1]+=1
                    if int(rr)==int(rel):
                        self.hit +=1
            self.ti+=1
        print(f'result: \033[31mhit {self.hit}\033[0m \033[35mtotal {self.ti}\033[0m \033[36mcover {self.cover}\033[0m \033[32mhit/infer/valid {self.hit}/{self.ti}/{self.tv}\033[0m\n\033[31mprecesion {self.hit/self.ti}\033[0m \033[35mrecall {self.hit/self.tv}\033[0m \033[36mcover rate {self.cover/self.tv}\033[0m')
        for i in range(3):
            print('[',end=' ')
            for j in range(5):
                if j ==4:
                    ends = ' '
                else:
                    ends = ','
                if j < 3:
                    if i==j:
                        print(f'\033[32m{correl_mat[i][j]:6}\033[0m',end=ends)
                    else:
                        print(f'\033[31m{correl_mat[i][j]:6}\033[0m',end=ends)
                else:
                    if (i==0 and j==3) or (i==2 and j==4):
                        print(f'\033[31m{correl_mat[i][j]:6}\033[0m',end=ends)
                    else:
                        print(f'\033[32m{correl_mat[i][j]:6}\033[0m',end=ends)
            print(']')
        print('-'*40)
        datj=[self.np2c,self.np2p,self.nc2p,self.hy01,self.hyn10]
        for i in range(3):
            print('[',end=' ')
            for j in range(5):
                if j ==4:
                    ends = ' '
                else:
                    ends = ','
                if j < 3:
                    if i==j:
                        print(f'\033[32m{(correl_mat[i][j])/(datj[j]):.4f}\033[0m',end=ends)
                    else:
                        print(f'\033[31m{(correl_mat[i][j])/(datj[j]):.4f}\033[0m',end=ends)
                else:
                    if (i==0 and j==3) or (i==2 and j==4):
                        print(f'\033[31m{(correl_mat[i][j])/(datj[j]):.4f}\033[0m',end=ends)
                    else:
                        print(f'\033[32m{(correl_mat[i][j])/(datj[j]):.4f}\033[0m',end=ends)
            print(']')
        rf.close()
        self.reset()

    def cmp2(self,valid_file,a,b):
        print(f'read validation set from {valid_file}')
        valid={}
        vf = open(valid_file,'r')
        for line in vf:
            if line.startswith('#'):
                continue
            [asn1,asn2,rel]= line.split()
            asn1 = asn1.strip()
            asn2 = asn2.strip()
            if '{' in asn1 or '{' in asn2:
                continue
            asn1 =int(asn1)
            asn2 =int(asn2)
            if asn1<asn2:
                valid[(asn1,asn2)]=rel
            else:
                next=[]
                if '&' in rel:
                    rrs = rel.split('&')
                    for rrr in rrs:
                        next.append(str(-int(rrr)))
                    valid[(asn2,asn1)]='&'.join(next)
                else:
                    valid[(asn2,asn1)]=str(-int(rel))
        print(f'total link in validaton set {self.tv}')
        print(f'\033[35mvalid count\033[0m\np2c {self.np2c}\np2p {self.np2p}\nc2p {self.nc2p}\nhybrid0&1 {self.hy01}\nhybrid-1&1 {self.hyn11}\nhybrid-1&0 {self.hyn10}\nhybrid-1&0&1 {self.hyn101}')
        vf.close()
        print(f'compare:\n\033[4m{a}\033[0m\n\033[4m{b}\n\033[0m',end='')
        a_link={}
        b_link={}
        def read_link(path,links):
            f = open(path,'r')
            for line in f:
                if line.startswith('#'):
                    continue
                [asn1,asn2,rel]= line.strip().split('|')
                asn1 =int(asn1)
                asn2 =int(asn2)
                if asn1 < asn2: 
                    links[(asn1,asn2)]=rel
                else:
                    links[(asn2,asn1)]=str(-int(rel))
            return links
        a_link = read_link(a,a_link)
        b_link = read_link(b,b_link)
        # p2c p2p c2p miss
        ###

        # in this correlation matrix, the first dimension stands for infer type,
        # which belongs to p2c,p2p or c2p. the second dimension is for validation
        # type, p2c, p2p, c2p, hybrid0&1,hybrid-1&0
        # [T,F,F,F,T,]
        # [F,T,F,T,T,]
        # [F,F,T,T,F,]
        mat = [[[0,0,0,0]for i in range(4)] for h in range(7)]
        for linka,rela in a_link.items():
            relv = valid.get(linka)
            relb= b_link.get(linka)
            if relv:
                if '&' in relv:
                    check=set()
                    rrs = relv.strip().split('&')
                    for rrr in rrs:
                        check.add(int(rrr))
                    if -1 in check and 0 in check:
                        _sum = 4
                    elif 1 in check and 0 in check:
                        _sum = 3
                    else:
                        continue
                    if relb:
                        mat[_sum][int(rela)+1][int(relb)+1]+=1
                    else:
                        mat[_sum][int(rela)+1][3]+=1
                else:
                    if relb:
                        mat[int(relv)+1][int(rela)+1][int(relb)+1]+=1
                    else:
                        mat[int(relv)+1][int(rela)+1][3]+=1
        for linkb,relb in b_link.items():
            relv = valid.get(linkb)
            rela= a_link.get(linkb)
            if relv:
                if '&' in relv:
                    check=set()
                    rrs = relv.strip().split('&')
                    for rrr in rrs:
                        check.add(int(rrr))
                    if -1 in check and 0 in check:
                        _sum = 4
                    elif 1 in check and 0 in check:
                        _sum = 3
                    else:
                        continue
                    if rela is None:
                        mat[_sum][3][int(relb)+1]+=1
                else:
                    if rela is None:
                        mat[int(relv)+1][3][int(relb)+1]+=1
            ###
        # mat = [[[0,0,0,0]for i in range(4)] for h in range(7)]
        # for ka,va in a_link.items():
        #     vb= b_link.get(ka)
        #     if vb is None:
        #         mat[int(va)+1][3]+=1
        #     else:
        #         mat[int(va)+1][int(vb)+1]+=1
        # for kb,vb in b_link.items():
        #     va = a_link.get(kb)
        #     if va is None:
        #         mat[3][int(vb)+1]+=1
        for k in range(5):
            for i in range(4):
                print('[',end=' ')
                for j in range(4):
                    if j ==3:
                        ends = ' '
                    else:
                        ends = ','
                    form = 0
                    color = 31
                    if i==j:
                        form = 1
                    if k < 3:
                        if i==k:
                            color = 32
                        if j==k:
                            color = 34
                    else:
                        if k == 3:
                            if i == 1 or i ==2:
                                color = 32
                            if j == 1 or j ==2:
                                color = 34
                        if k == 4:
                            if i == 1 or i ==0:
                                color = 32
                            if j == 1 or j ==0:
                                color = 34
                    disp = f'\033[{form};{color}m'
                    print(f'{disp}{mat[k][i][j]:6}\033[0m',end=ends)
                print(']')
            print('-'*30)


if __name__ == "__main__":
    valid_file = '/home/lwd/RIB.test/validation/validation_data.txt'
    file_list=['/home/lwd/Result/vote/tsv/tsf_20201201.rel',
    '/home/lwd/Result/vote/tsv/tsf_20201208.rel',
    '/home/lwd/Result/vote/tsv/tsf_20201215.rel',
    '/home/lwd/Result/vote/tsv/tsf_20201222.rel',
    '/home/lwd/Result/vote/apv/apf.rel',

    '/home/lwd/Result/vote/apv/ap_apv.rel',
    '/home/lwd/Result/vote/apv/ap2_apv.rel',
    '/home/lwd/Result/vote/apv/cf_apv.rel',
    '/home/lwd/Result/vote/apv/tsf.rel',

    '/home/lwd/Result/vote/apv/ap_bv.rel',
    '/home/lwd/Result/vote/apv/ap2_bv.rel',
    '/home/lwd/Result/vote/apv/cf_bv.rel',
    '/home/lwd/Result/vote/apv/tsf_apf.rel',

    '/home/lwd/Result/vote/tsv/ap_tsv_month.rel',
    '/home/lwd/Result/vote/tsv/ap2_tsv_month.rel',
    '/home/lwd/Result/vote/tsv/cf_tsv_month.rel',
    '/home/lwd/Result/vote/tsv/ar_tsv_month.rel',
    '/home/lwd/Result/vote/tsv/tsf_month.rel',

    '/home/lwd/Result/BN/ap2_apv.rel.bn',
    '/home/lwd/Result/BN/ap2_bv.rel.bn',
    '/home/lwd/Result/BN/ap2_tsv_month.rel.bn',
    # '/home/lwd/Result/BN/tsf.rel.bn',
    # '/home/lwd/Result/BN/tsf_apf.rel.bn',
    # '/home/lwd/Result/BN/ar_tsv_month.rel.bn',

    '/home/lwd/Result/NN/ap2_apv.feap.csv.nn',

    '/home/lwd/Result/auxiliary/pc20201201.v4.arout',
    '/home/lwd/Result/TS_working/rel_20201222_vp1.ar',
    '/home/lwd/Result/AP_working/rel_20201201.apr',
    '/home/lwd/Result/AP_working/rel_20201201.cf',
    '/home/lwd/Result/AP_working/test_low.ap2',
    '/home/lwd/Result/vote/apv/ap2_apv_low.rel',
    '/home/lwd/Result/auxiliary/pc202012.v4.arout',
    # '/home/lwd/Result/notexist',
    
    '/home/lwd/AT/stage1.rel',
    '/home/lwd/AT/test_low_true.ap2',

    '/home/lwd/Result/BN/tsf.rel.bn',
    '/home/lwd/Result/BN/ar_tsv_month.rel.bn',
    '/home/lwd/Result/BN/tsf_apf.rel.bn',
    ]

    e = comm()

    test_file_list= []

    for ff in file_list:
        last_name= ff.split('/')[-1]
        # os.system(f'sort {ff}| uniq > /home/lwd/Result/cmp/{last_name}')
        test_file_list.append(f'/home/lwd/Result/cmp/{last_name}')
    # quit()

    e.read(valid_file)
    cnt=0

    sp_list=[
        '/home/lwd/Result/vote/apv/ap2_apv.rel',
        '/home/lwd/Result/vote/apv/tsf.rel',

        '/home/lwd/Result/vote/apv/ap2_bv.rel',
        '/home/lwd/Result/vote/apv/tsf_apf.rel',

        '/home/lwd/Result/vote/tsv/ap2_tsv_month.rel',
        '/home/lwd/Result/vote/tsv/ar_tsv_month.rel',
    ]

    un_list=[

        '/home/lwd/Result/AP_working/rel_20201201.apr',
        '/home/lwd/Result/AP_working/rel_20201208.apr',
        '/home/lwd/Result/AP_working/rel_20201215.apr',
        '/home/lwd/Result/AP_working/rel_20201222.apr',
        '/home/lwd/Result/vote/apv/ap2_apv.rel',
        '/home/lwd/AT/stage1.rel',
    ]

    for ff in test_file_list:
        if exists(ff):
            try:
                # e.compare(ff)
                e.total_statistic(ff)
                pass
            except Exception as es:
                print(es)
        else:
            print(f'\033[7mnot exists: {ff}\033[0m')

    e.cmp2(valid_file,test_file_list[-2],test_file_list[-1])
    e.cmp2(valid_file,test_file_list[-2],test_file_list[-3])