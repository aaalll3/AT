from ctypes import alignment
from logging import critical
import os
from os import path
import networkx as nx
import json
import re
import pandas as pd


from location import *
from os.path import join,abspath,exists
from collections import defaultdict
from hierarchy import Hierarchy
import multiprocessing

# genric modify

class comm():
    '''
    set up data for validation, comparing between different type of link
    only record one direction and check reverse link when use a link
    '''
    def __init__(self,version=4):
        # basic
        self.ti = 0 
        self.tv = 0
        self.hit = 0
        self.cover = 0
        self.valid = dict()

        # hardlink
        self.g=None
        self.hard_links_total=set()
        self.critical_links_total=set()
        self.hard_links=[set() for i in range(4)]
        self.critical_links=[set() for i in range(5)]
        self.link2VP=defaultdict(set)
        self.linkON=defaultdict(int) # observed number
        self.version=version
        if version==4:
            self.tier1=['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
            '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
        else:
            self.tier1= ['174', '1299', '3356', '6057', '6939', '9002', '24482', '35280', '37468', '39533']
        # relation type
        self.p2c = set()
        self.c2p = set()
        self.p2p = set()
        self.hybrid = set()
        
        # pure realtion type statistic
        self.hy01=0
        self.hyn11=0
        self.hyn10=0
        self.hyn101=0
        self.np2c=0
        self.nc2p=0
        self.np2p=0

        #
        self.out=[]

        # enable
        self.validation = False
        self.hnc = False

        # display digit
        self.dd = 3

        self.output_lines = []

        # hierarchy for discrime tier
        boost_file = '/home/lwd/Result/auxiliary/validation_data.clean'

        self.hierarchy = Hierarchy(boost_file,version)

    @staticmethod
    def clear(valid_file):
        vf = open(valid_file)
        wf = open('/home/lwd/Result/auxiliary/validation_data.clean','w')
        for line in vf:
            if line.startswith('#'):
                continue
            [asn1,asn2,rel]= line.split()
            asn1 = asn1.strip()
            asn2 = asn2.strip()
            if '{' in asn1 or '{' in asn2:
                continue
            if '&' in rel:
                continue
            asn1 =int(asn1)
            asn2 =int(asn2)
            wf.write(f'{asn1}|{asn2}|{rel}\n')
        vf.close()
        wf.close()
        
    def read(self,valid_file):
        print(f'read validation set from {valid_file}')
        vf = open(valid_file,'r')
        for line in vf:
            if line.startswith('#'):
                continue
            link = None
            if '|' in line:
                link = line.split('|')
            else:
                link= line.split()
            asn1 = link[0].strip()
            asn2 = link[1].strip()
            rel = link[2]
            if '{' in asn1 or '{' in asn2:
                continue
            asn1 =int(asn1)
            asn2 =int(asn2)
            self.valid[(asn1,asn2)]=rel
            if '&' in rel:
                continue
                check=set()
                rrs = rel.split('&')
                for rrr in rrs:
                    check.add(int(rrr))
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
        if self.hy01 > 0:
            self.dd +=1
        if self.hyn10 > 0:
            self.dd += 1
        if self.hyn101 > 0:
            self.dd += 1
        if self.hyn11 > 0:
            self.dd += 1

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
            if '{' in line:
                continue
            if '}' in line:
                continue
            ASes = line.strip().split('|')
            for i in range(len(ASes)-1):
                self.g.add_edge(ASes[i],ASes[i+1])
                # type2: links that's not contains Tier1 AS or VP
                if i >= 1 and ASes[i] not in self.tier1 and ASes[i+1] not in self.tier1:
                    self.hard_links[2].add((ASes[i],ASes[i+1]))
                    # input(f'add type2 {ASes[i]}-{ASes[i+1]}')
                self.link2VP[(ASes[i],ASes[i+1])].add(ASes[0])
                self.linkON[(ASes[i],ASes[i+1])]+=1
        print(len(self.g.edges))
        print('read path and took type2')
        # type0: links whose max degree is less than 100
        for link in self.g.edges:
            degrees= self.g.degree(link)
            for node,degree in degrees:
                if degree >= 100:
                    # input(f'add type0 {link[0]}-{link[1]}')
                    break
                self.hard_links[0].add(link)

        print('took type0')
        # type1: links that observed by less than 100 VPs and more than 50 VPs
        for link,VPset in self.link2VP.items():
            if len(VPset) >= 50 and len(VPset) <= 100:
                self.hard_links[1].add(link)
                # input(f'add type1 {link[0]}-{link[1]}')
        print('took type1')
        # type2; done
        # type3: stub links, whose only adjacent node is tier1 AS
        for node in self.g.nodes:
            adj = self.g.adj[node]
            if len(adj)==1:
                adj = list(adj.keys())
                if adj[0] in self.tier1:
                    self.hard_links[3].add((node,adj[0]))
                    # input(f'add type3 {node}-{adj[0]}')
        print('took type3')
        # set critical links
        # type0: links between tier1 ASes
        tier1_num = len(self.tier1)
        tier1_list = list(self.tier1)
        for i in range(tier1_num):
            for j in range(i):
                # only record one direction
                link = (tier1_list[i],tier1_list[j])
                if link in self.g.edges:
                    self.critical_links[0].add(link)
                    # input(f'add c0 {link[0]}-{link[1]}')
                    continue
        print('took ct0')
        # type1: high tier link
        high = self.hierarchy.high
        clique = self.hierarchy.clique
        large_high = set()
        for node in high:
            if self.g.degree[node] > 1000:
                large_high.add(node)
        large_high = list(large_high)
        for node in large_high:
            for neighbor in self.g.adj[node]:
                if neighbor in large_high or neighbor in clique:
                    self.critical_links[1].add((node,neighbor))
                    # input(f'add c1 {node}-{neighbor}')
        print('took ct1')
        # type2: high frequency link
        for link in self.g.edges:
            if self.linkON[link]>1000:
                self.critical_links[2].add(link)
                # input(f'add c2 {link[0]}-{link[1]}')
        print('took ct2')
        # type3: links between tier1 ASes and low tier ASes or stub ASes
        for node in tier1_list:
            for neighbor in self.g.adj[node]:
                if neighbor in self.hierarchy.stub \
                    or (neighbor in self.hierarchy.low \
                        and self.g.degree[neighbor] < 50):
                    self.critical_links[3].add((node,neighbor))
                    # input(f'add c3 {node}-{neighbor}')
        print('took ct3')
        # type4: links between ASes with great difference of degree
        for node in self.g.nodes:
            for neighbor in self.g.adj[node]:
                if self.g.degree[node]>5000 and self.g.degree[neighbor] <100:
                    self.critical_links[4].add((node,neighbor))
                    # input(f'add c4 {node}-{neighbor}')
        print('took ct4')

        for links in self.hard_links:
            self.hard_links_total.update(links)
        for links in self.critical_links:
            self.critical_links_total.update(links)

       
        print('done set hnc')
        print('before')
        print(f'{len(self.hard_links_total)} h total')
        for i in range(4):
            print(f'{len(self.hard_links[i])} h type{i}')
        print(f'{len(self.critical_links_total)} c total')
        for i in range(5):
            print(f'{len(self.critical_links[i])} c type{i}')
        def clear(inset):
            for link in list(inset):
                r_link = (link[1],link[0])
                if link in inset and r_link in inset:
                    inset.remove(r_link)
            return inset
        self.hard_links_total = clear(self.hard_links_total)
        for i in range(4):
            self.hard_links[i] = clear(self.hard_links[i])
        self.critical_links_total = clear(self.critical_links_total)
        for i in range(5):
            self.critical_links[i] = clear(self.critical_links[i])
        print('after')
        print(f'{len(self.hard_links_total)} h total')
        for i in range(4):
            print(f'{len(self.hard_links[i])} h type{i}')
        print(f'{len(self.critical_links_total)} c total')
        for i in range(5):
            print(f'{len(self.critical_links[i])} c type{i}')
        jsout={'htotal': list(self.hard_links_total),'htype':[ list(n) for n in self.hard_links],'ctotal':list(self.critical_links_total),'ctype': [ list(n) for n in self.critical_links]}
        loc = f'/home/lwd/Result/auxiliary/hncv{self.version}.link'
        of = open(loc,'w')
        json.dump(jsout,of)
        self.hnc=True

    def load_hnc(self):
        loc = f'/home/lwd/Result/auxiliary/hncv{self.version}.link'
        of = open(loc,'r')
        tmp = json.load(of)
        ll = tmp['htotal']
        def tuplize(a_list):
            res = []
            for n in a_list:
                res.append((n[0],n[1]))
            return res
        self.hard_links_total=set(tuplize(tmp['htotal']))
        self.hard_links=[ set(tuplize(n)) for n in tmp['htype']]
        self.critical_links_total=set(tuplize(tmp['ctotal']))
        self.critical_links=[ set(tuplize(n)) for n in tmp['ctype']]
        self.hnc = True

    def reset(self):
        self.ti = 0 
        self.hit = 0
        self.cover = 0
        self.output_lines = []

    def total_statistic(self,rel_file):
        print(f'\033[4mtest relationthip {rel_file}\033[0m')

        # in this correlation matrix, the first dimension stands for infer type,
        # which belongs to p2c,p2p or c2p. the second dimension is for validation
        # type, p2c, p2p, c2p, hybrid0&1,hybrid-1&0
        # [T,F,F,F,T,]
        # [F,T,F,T,T,]
        # [F,F,T,T,F,]
        hit = 0
        cover = 0
        ti = 0
        correl_mat=[[0,0,0,0,0,0,0] for i in range(3)]
        hard_hit_total=0
        hard_infer_total=0
        critical_hit_total=0
        critical_infer_total=0
        hard_hit = [0 for i in range(4)]
        hard_infer = [0 for i in range(4)]
        critical_hit = [0 for i in range(5)]
        critical_infer = [0 for i in range(5)]
        tp_set=set()
        cnts = defaultdict(int)

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

            correct = False
            cover+=1

            cnts['total']+=1
            if int(rel) == 0:
                cnts['p2p']+=1
            elif int(rel) == 1:
                cnts['c2p']+=1
            elif int(rel) == -1:
                cnts['p2c']+=1

            if rra == None and rrb ==None:
                continue
            elif rra == None:
                rr = rrb
                rev=True
            elif rrb == None:
                rr = rra
            else:
                rr = rra
            if linka in tp_set or linkb in tp_set:
                continue
            #note reverse
            if '&' in rr:
                check=set()
                rrs = rr.split('&')
                for rrr in rrs:
                    if rev:
                        if int(rrr)==-int(rel):
                            hit +=1
                            tp_set.add(linka)
                            tp_set.add(linkb)
                            correct = True
                            break
                    else:
                        if int(rrr)==int(rel):
                            hit +=1
                            tp_set.add(linka)
                            tp_set.add(linkb)
                            correct = True
                            break
                _sum = -1
                _other = -1
                for rrr in rrs:
                    check.add(int(rrr))
                if -1 in check and 0 in check:
                    _sum = 4
                    _other = 3
                elif 1 in check and 0 in check:
                    _sum = 3
                    _other = 4
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
                        hit +=1
                        tp_set.add(linka)
                        tp_set.add(linkb)
                        correct = True
                else:
                    correl_mat[int(rel)+1][int(rr)+1]+=1
                    if int(rr)==int(rel):
                        hit +=1
                        tp_set.add(linka)
                        tp_set.add(linkb)
                        correct = True
            ti+=1
            linka = (str(linka[0]),str(linka[1]))
            linkb = (str(linkb[0]),str(linkb[1]))
            clist={}
            clist[0]=set()
            clist[1]=set()
            clist[2]=set()
            clist[3]=set()
            clist[4]=set()
            clist[10]=set()
            clist[11]=set()
            clist[12]=set()
            clist[13]=set()
            clist[14]=set()
            clist[15]=set()
            # print('hear')
            if self.hnc:
                for idx,lookup in enumerate(self.hard_links):
                    if (linka in lookup or linkb in lookup) and (linka not in clist[idx+1] and linkb not in clist[idx+1]):
                        clist[idx+1].add(linka)
                        clist[idx+1].add(linkb)
                        if correct:
                            hard_hit[idx]+=1
                        hard_infer[idx]+=1
                        cnts[f'h type{idx+1}']+=1

                for idx,lookup in enumerate(self.critical_links):
                    if (linka in lookup or linkb in lookup) and (linka not in clist[idx+11] and linkb not in clist[idx+11]):
                        clist[idx+11].add(linka)
                        clist[idx+11].add(linkb)
                        if correct:
                            critical_hit[idx]+=1
                        critical_infer[idx]+=1
                        cnts[f'c type{idx+1}']+=1


                if (linka in self.hard_links_total or linkb in self.hard_links_total)and (linka not in clist[0] and linkb not in clist[0]):
                    clist[0].add(linka)
                    clist[0].add(linkb)
                    if correct:
                        hard_hit_total+=1
                    hard_infer_total+=1
                    cnts['h total']+=1

                if (linka in self.critical_links_total or linkb in self.critical_links_total) and (linka not in clist[10] and linkb not in clist[10]):
                    clist[10].add(linka)
                    clist[10].add(linkb)
                    if correct:
                        critical_hit_total+=1
                    critical_infer_total+=1
                    cnts['c total']+=1

        if hard_infer_total==0:
            hard_infer_total+=1
        if critical_infer_total==0:
            critical_infer_total+=1
        for idx,lookup in enumerate(self.critical_links):
            if critical_infer[idx]==0:
                critical_infer[idx]+=1
        for idx,lookup in enumerate(self.hard_links):
            if hard_infer[idx]==0:
                hard_infer[idx]+=1
        result={}
        result['total']={}
        result['total']['hit']=hit
        result['total']['infer']=ti
        result['total']['validation']=self.tv
        print(f'result: \033[31mhit {hit:6} \033[35mtotal {ti:6} \033[36mcover {cover:6} \033[32mhit/infer/valid {hit:6}/{ti:6}/{self.tv:6}\n\033[31mprecesion {hit/ti:4f} \033[35mrecall {hit/self.tv:4f} \033[36mcover rate {cover/self.tv:4f}\033[0m')
        if self.hnc:
            result['hard']={}
            result['hard']['hit']=[hard_hit_total]+hard_hit
            result['hard']['infer']=[hard_hit_total]+hard_infer
            result['hard']['validation']=[hard_hit_total]+[len(self.hard_links[idx]) for idx in range(4)]
            result['critical']={}
            result['critical']['hit']=[critical_hit_total]+critical_hit
            result['critical']['infer']=[critical_hit_total]+critical_infer
            result['critical']['validation']=[critical_hit_total]+[len(self.critical_links[idx]) for idx in range(5)]
            print(f'hard link \
            \033[31mprecision {hard_hit_total/hard_infer_total:4f} \
            \033[32mnum {hard_hit_total:6}/{hard_infer_total:6}/{len(self.hard_links_total):6} \
            \033[33merror {1-hard_hit_total/hard_infer_total:4f}\033[0m')
            for idx in range(len(hard_hit)):
                print(f'hard link type{idx+1}: \
                \033[31mprecision {hard_hit[idx]/hard_infer[idx]:4f} \
                \033[32mnum {hard_hit[idx]:6}/{hard_infer[idx]:6}/{len(self.hard_links[idx]):6} \
                \033[33merror {1-hard_hit[idx]/hard_infer[idx]:4f}\033[0m')
            print(f'critical link \
            \033[31mprecision {critical_hit_total/critical_infer_total:4f} \
            \033[32mnum {critical_hit_total:6}/{critical_infer_total:6}/{len(self.critical_links_total):6} \
            \033[33merror {1-critical_hit_total/critical_infer_total:4f}\033[0m')
            for idx in range(len(critical_hit)):
                print(f'critical link type{idx+1}: \
                \033[31mprecision {critical_hit[idx]/critical_infer[idx]:4f} \
                \033[32mnum {critical_hit[idx]:6}/{critical_infer[idx]:6}/{len(self.critical_links[idx]):6} \
                \033[33merror {1-critical_hit[idx]/critical_infer[idx]:4f}\033[0m')
        print('-'*40)
        def safe(a):
            if a == 0:
                return 1
            else:
                return a
        numss=[self.np2c+self.hyn10,self.np2p+self.hy01+self.hyn10,self.nc2p+self.hy01]
        for i in range(3):
            print(f'p2c {correl_mat[i][i]/safe(sum(correl_mat[i]))} {correl_mat[i][i]}/{sum(correl_mat[i])}/{numss[i]}')
        name = ['p2c','p2p','c2p']
        vad = [self.np2c+self.hyn10, self.np2p+self.hy01+self.hyn10,self.nc2p+self.hy01]
        for i in range(3):
            result[name[i]]={'hit':correl_mat[i][i],'infer':sum(correl_mat[i]),'validation':vad[i]}
        for i in range(3):
            print('[',end=' ')
            for j in range(self.dd):
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


        last_name=rel_file.split('/')[-1]
        lines = []
        lines.append(
            f'''|{last_name} precision|{result['total']['hit']/safe(result['total']['infer'])}|{result['p2c']['hit']/safe(result['p2c']['infer'])}|{result['p2p']['hit']/safe(result['p2p']['infer'])}|{result['c2p']['hit']/safe(result['c2p']['infer'])}|{result['hard']['hit'][0]/safe(result['hard']['infer'][0])}|{result['hard']['hit'][1]/safe(result['hard']['infer'][1])}|{result['hard']['hit'][2]/safe(result['hard']['infer'][2])}|{result['hard']['hit'][3]/safe(result['hard']['infer'][3])}|{result['hard']['hit'][4]/safe(result['hard']['infer'][4])}|{result['critical']['hit'][0]/safe(result['critical']['infer'][0])}|{result['critical']['hit'][1]/safe(result['critical']['infer'][1])}|{result['critical']['hit'][2]/safe(result['critical']['infer'][2])}|{result['critical']['hit'][3]/safe(result['critical']['infer'][3])}|{result['critical']['hit'][4]/safe(result['critical']['infer'][4])}|{result['critical']['hit'][5]/safe(result['critical']['infer'][5])}|\n'''
        )
        lines.append(
            f'''|{last_name} error|{1-result['total']['hit']/safe(result['total']['infer'])}|{1-result['p2c']['hit']/safe(result['p2c']['infer'])}|{1-result['p2p']['hit']/safe(result['p2p']['infer'])}|{1-result['c2p']['hit']/safe(result['c2p']['infer'])}|{1-result['hard']['hit'][0]/safe(result['hard']['infer'][0])}|{1-result['hard']['hit'][1]/safe(result['hard']['infer'][1])}|{1-result['hard']['hit'][2]/safe(result['hard']['infer'][2])}|{1-result['hard']['hit'][3]/safe(result['hard']['infer'][3])}|{1-result['hard']['hit'][4]/safe(result['hard']['infer'][4])}|{1-result['critical']['hit'][0]/safe(result['critical']['infer'][0])}|{1-result['critical']['hit'][1]/safe(result['critical']['infer'][1])}|{1-result['critical']['hit'][2]/safe(result['critical']['infer'][2])}|{1-result['critical']['hit'][3]/safe(result['critical']['infer'][3])}|{1-result['critical']['hit'][4]/safe(result['critical']['infer'][4])}|{1-result['critical']['hit'][5]/safe(result['critical']['infer'][5])}|\n'''
        )
        lines.append(
            f'''|{last_name} num|{result['total']['hit']}/{result['total']['infer']}|{result['p2c']['hit']}/{result['p2c']['infer']}|{result['p2p']['hit']}/{result['p2p']['infer']}|{result['c2p']['hit']}/{result['c2p']['infer']}|{result['hard']['hit'][0]}/{result['hard']['infer'][0]}|{result['hard']['hit'][1]}/{result['hard']['infer'][1]}|{result['hard']['hit'][2]}/{result['hard']['infer'][2]}|{result['hard']['hit'][3]}/{result['hard']['infer'][3]}|{result['hard']['hit'][4]}/{result['hard']['infer'][4]}|{result['critical']['hit'][0]}/{result['critical']['infer'][0]}|{result['critical']['hit'][1]}/{result['critical']['infer'][1]}|{result['critical']['hit'][2]}/{result['critical']['infer'][2]}|{result['critical']['hit'][3]}/{result['critical']['infer'][3]}|{result['critical']['hit'][4]}/{result['critical']['infer'][4]}|{result['critical']['hit'][5]}/{result['critical']['infer'][5]}|\n'''
        )

        if os.path.exists('./cmp.md'):
            out = open('./cmp.md','a')
        else:
            out = open('./cmp.md','w')
            # put title
            out.write('||total|p2c|p2p|c2p|hard|h1|h2|h3|h4|critical|c1|c2|c3|c4|c5|\n')
            out.write('|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|\n')
        # line=f'''|{last_name} precision|{result['total']['hit']/safe(result['total']['infer']):5f}/{cnts['total']}|{result['p2c']['hit']/safe(result['p2c']['infer']):5f}/{cnts['p2c']}|{result['p2p']['hit']/safe(result['p2p']['infer']):5f}/{cnts['p2p']}|{result['c2p']['hit']/safe(result['c2p']['infer']):5f}/{cnts['c2p']}|{result['hard']['hit'][0]/safe(result['hard']['infer'][0]):5f}/{cnts['h total']}|{result['hard']['hit'][1]/safe(result['hard']['infer'][1]):5f}/{cnts['h type1']}|{result['hard']['hit'][2]/safe(result['hard']['infer'][2]):5f}/{cnts['h type2']}|{result['hard']['hit'][3]/safe(result['hard']['infer'][3]):5f}/{cnts['h type3']}|{result['hard']['hit'][4]/safe(result['hard']['infer'][4]):5f}/{cnts['h type4']}|{result['critical']['hit'][0]/safe(result['critical']['infer'][0]):5f}/{cnts['c total']}|{result['critical']['hit'][1]/safe(result['critical']['infer'][1]):5f}/{cnts['c type1']}|{result['critical']['hit'][2]/safe(result['critical']['infer'][2]):5f}/{cnts['c type2']}|{result['critical']['hit'][3]/safe(result['critical']['infer'][3]):5f}/{cnts['c type3']}|{result['critical']['hit'][4]/safe(result['critical']['infer'][4]):5f}/{cnts['c type4']}|{result['critical']['hit'][5]/safe(result['critical']['infer'][5]):5f}/{cnts['c type5']}|\n'''
        
        out.writelines(lines)
        # out.write(line)
        out.close()

    def total_statistic_rev(self,rel_file):
        print(f'\033[4mtest relationthip {rel_file}\033[0m')
        def safe(a):
            if a == 0:
                return 1
            else:
                return a

        # in this correlation matrix, the first dimension stands for infer type,
        # which belongs to p2c,p2p or c2p. the second dimension is for validation
        # type, p2c, p2p, c2p, hybrid0&1,hybrid-1&0
        # [T,F,F,F,T,]
        # [F,T,F,T,T,]
        # [F,F,T,T,F,]
        hit = 0
        cover = 0
        ti = 0
        correl_mat=[[0,0,0,0,0,0,0] for i in range(3)]
        hard_hit_total=0
        hard_infer_total=0
        critical_hit_total=0
        critical_infer_total=0
        hard_hit = [0 for i in range(4)]
        hard_infer = [0 for i in range(4)]
        critical_hit = [0 for i in range(5)]
        critical_infer = [0 for i in range(5)]
        tp_set=set()
        cnts = defaultdict(int)

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

            correct = False
            cover+=1

            cnts['total']+=1
            if int(rel) == 0:
                cnts['p2p']+=1
            elif int(rel) == 1:
                cnts['c2p']+=1
            elif int(rel) == -1:
                cnts['p2c']+=1

            if rra == None and rrb ==None:
                continue
            elif rra == None:
                rr = rrb
                rev=True
            elif rrb == None:
                rr = rra
            else:
                rr = rra
            if linka in tp_set or linkb in tp_set:
                continue
            #note reverse
            tp_set.add(linka)
            tp_set.add(linkb)
            if '&' in rr:
                check=set()
                rrs = rr.split('&')
                for rrr in rrs:
                    if rev:
                        if int(rrr)==-int(rel):
                            hit +=1
                            correct = True
                            break
                    else:
                        if int(rrr)==int(rel):
                            hit +=1
                            correct = True
                            break
                _sum = -1
                _other = -1
                for rrr in rrs:
                    check.add(int(rrr))
                if -1 in check and 0 in check:
                    _sum = 4
                    _other = 3
                elif 1 in check and 0 in check:
                    _sum = 3
                    _other = 4
                else:
                    continue
                if rev:
                    if int(rel) == 1:
                        correl_mat[-int(rel)+1][_sum]+=1
                    else:
                        correl_mat[int(rel)+1][_other]+=1
                else:
                    if int(rel) == 1:
                        correl_mat[-int(rel)+1][_other]+=1
                    else:
                        correl_mat[int(rel)+1][_sum]+=1
            else:
                if rev:
                    if int(rel) == 1:
                        correl_mat[-int(rel)+1][int(rr)+1]+=1
                    else:
                        correl_mat[int(rel)+1][-int(rr)+1]+=1
                    if int(rr)==-int(rel):
                        hit +=1
                        correct = True
                else:
                    if int(rel) == 1:
                        correl_mat[-int(rel)+1][-int(rr)+1]+=1
                    else:
                        correl_mat[int(rel)+1][int(rr)+1]+=1
                    if int(rr)==int(rel):
                        hit +=1
                        correct = True
            ti+=1
            linka = (str(linka[0]),str(linka[1]))
            linkb = (str(linkb[0]),str(linkb[1]))
            clist={}
            clist[0]=set()
            clist[1]=set()
            clist[2]=set()
            clist[3]=set()
            clist[4]=set()
            clist[10]=set()
            clist[11]=set()
            clist[12]=set()
            clist[13]=set()
            clist[14]=set()
            clist[15]=set()
            # print('hear')
            if self.hnc:
                for idx,lookup in enumerate(self.hard_links):
                    if (linka in lookup or linkb in lookup) and (linka not in clist[idx+1] and linkb not in clist[idx+1]):
                        clist[idx+1].add(linka)
                        clist[idx+1].add(linkb)
                        if correct:
                            hard_hit[idx]+=1
                        hard_infer[idx]+=1
                        cnts[f'h type{idx+1}']+=1

                for idx,lookup in enumerate(self.critical_links):
                    if (linka in lookup or linkb in lookup) and (linka not in clist[idx+11] and linkb not in clist[idx+11]):
                        clist[idx+11].add(linka)
                        clist[idx+11].add(linkb)
                        if correct:
                            critical_hit[idx]+=1
                        critical_infer[idx]+=1
                        cnts[f'c type{idx+1}']+=1


                if (linka in self.hard_links_total or linkb in self.hard_links_total)and (linka not in clist[0] and linkb not in clist[0]):
                    clist[0].add(linka)
                    clist[0].add(linkb)
                    if correct:
                        hard_hit_total+=1
                    hard_infer_total+=1
                    cnts['h total']+=1

                if (linka in self.critical_links_total or linkb in self.critical_links_total) and (linka not in clist[10] and linkb not in clist[10]):
                    clist[10].add(linka)
                    clist[10].add(linkb)
                    if correct:
                        critical_hit_total+=1
                    critical_infer_total+=1
                    cnts['c total']+=1

        if hard_infer_total==0:
            hard_infer_total+=1
        if critical_infer_total==0:
            critical_infer_total+=1
        for idx,lookup in enumerate(self.critical_links):
            if critical_infer[idx]==0:
                critical_infer[idx]+=1
        for idx,lookup in enumerate(self.hard_links):
            if hard_infer[idx]==0:
                hard_infer[idx]+=1
        result={}
        result['total']={}
        result['total']['hit']=hit
        result['total']['infer']=ti
        result['total']['validation']=self.tv
        print(f'result: \033[31mhit {hit:6} \033[35mtotal {ti:6} \033[36mcover {cover:6} \033[32mhit/infer/valid {hit:6}/{safe(ti):6}/{self.tv:6}\n\033[31mprecesion {hit/safe(ti):4f} \033[35mrecall {hit/safe(self.tv):4f} \033[36mcover rate {cover/safe(self.tv):4f}\033[0m')
        if self.hnc:
            result['hard']={}
            result['hard']['hit']=[hard_hit_total]+hard_hit
            result['hard']['infer']=[hard_infer_total]+hard_infer
            result['hard']['validation']=[hard_hit_total]+[len(self.hard_links[idx]) for idx in range(4)]
            result['critical']={}
            result['critical']['hit']=[critical_hit_total]+critical_hit
            result['critical']['infer']=[critical_infer_total]+critical_infer
            result['critical']['validation']=[critical_hit_total]+[len(self.critical_links[idx]) for idx in range(5)]
            print(f'hard link \
            \033[31mprecision {hard_hit_total/hard_infer_total:4f} \
            \033[32mnum {hard_hit_total:6}/{hard_infer_total:6}/{len(self.hard_links_total):6} \
            \033[33merror {1-hard_hit_total/hard_infer_total:4f}\033[0m')
            for idx in range(len(hard_hit)):
                print(f'hard link type{idx+1}: \
                \033[31mprecision {hard_hit[idx]/hard_infer[idx]:4f} \
                \033[32mnum {hard_hit[idx]:6}/{hard_infer[idx]:6}/{len(self.hard_links[idx]):6} \
                \033[33merror {1-hard_hit[idx]/hard_infer[idx]:4f}\033[0m')
            print(f'critical link \
            \033[31mprecision {critical_hit_total/critical_infer_total:4f} \
            \033[32mnum {critical_hit_total:6}/{critical_infer_total:6}/{len(self.critical_links_total):6} \
            \033[33merror {1-critical_hit_total/critical_infer_total:4f}\033[0m')
            for idx in range(len(critical_hit)):
                print(f'critical link type{idx+1}: \
                \033[31mprecision {critical_hit[idx]/critical_infer[idx]:4f} \
                \033[32mnum {critical_hit[idx]:6}/{critical_infer[idx]:6}/{len(self.critical_links[idx]):6} \
                \033[33merror {1-critical_hit[idx]/critical_infer[idx]:4f}\033[0m')
        print('-'*40)
        numss=[self.np2c+self.hyn10,self.np2p+self.hy01+self.hyn10,self.nc2p+self.hy01]
        print(f'p2c {correl_mat[0][0]/safe(sum(correl_mat[0]))} {correl_mat[0][0]}/{sum(correl_mat[0])}/{numss[0]+numss[2]}')
        print(f'p2c {correl_mat[1][1]/safe(sum(correl_mat[1]))} {correl_mat[1][1]}/{sum(correl_mat[1])}/{numss[1]}')
        name = ['p2c','p2p','c2p']
        vad = [self.np2c+self.hyn10, self.np2p+self.hy01+self.hyn10,self.nc2p+self.hy01]
        for i in range(3):
            result[name[i]]={'hit':correl_mat[i][i],'infer':sum(correl_mat[i]),'validation':vad[i]}
        for i in range(3):
            print('[',end=' ')
            for j in range(self.dd):
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


        last_name=rel_file.split('/')[-1]
        lines = []
        lines.append(
            f'''|{last_name} precision|{result['total']['hit']/safe(result['total']['infer'])}|{result['p2c']['hit']/safe(result['p2c']['infer'])}|{result['p2p']['hit']/safe(result['p2p']['infer'])}|{result['c2p']['hit']/safe(result['c2p']['infer'])}|{result['hard']['hit'][0]/safe(result['hard']['infer'][0])}|{result['hard']['hit'][1]/safe(result['hard']['infer'][1])}|{result['hard']['hit'][2]/safe(result['hard']['infer'][2])}|{result['hard']['hit'][3]/safe(result['hard']['infer'][3])}|{result['hard']['hit'][4]/safe(result['hard']['infer'][4])}|{result['critical']['hit'][0]/safe(result['critical']['infer'][0])}|{result['critical']['hit'][1]/safe(result['critical']['infer'][1])}|{result['critical']['hit'][2]/safe(result['critical']['infer'][2])}|{result['critical']['hit'][3]/safe(result['critical']['infer'][3])}|{result['critical']['hit'][4]/safe(result['critical']['infer'][4])}|{result['critical']['hit'][5]/safe(result['critical']['infer'][5])}|\n'''
        )
        lines.append(
            f'''|{last_name} error|{1-result['total']['hit']/safe(result['total']['infer'])}|{1-result['p2c']['hit']/safe(result['p2c']['infer'])}|{1-result['p2p']['hit']/safe(result['p2p']['infer'])}|{1-result['c2p']['hit']/safe(result['c2p']['infer'])}|{1-result['hard']['hit'][0]/safe(result['hard']['infer'][0])}|{1-result['hard']['hit'][1]/safe(result['hard']['infer'][1])}|{1-result['hard']['hit'][2]/safe(result['hard']['infer'][2])}|{1-result['hard']['hit'][3]/safe(result['hard']['infer'][3])}|{1-result['hard']['hit'][4]/safe(result['hard']['infer'][4])}|{1-result['critical']['hit'][0]/safe(result['critical']['infer'][0])}|{1-result['critical']['hit'][1]/safe(result['critical']['infer'][1])}|{1-result['critical']['hit'][2]/safe(result['critical']['infer'][2])}|{1-result['critical']['hit'][3]/safe(result['critical']['infer'][3])}|{1-result['critical']['hit'][4]/safe(result['critical']['infer'][4])}|{1-result['critical']['hit'][5]/safe(result['critical']['infer'][5])}|\n'''
        )
        lines.append(
            f'''|{last_name} num|{result['total']['hit']}/{result['total']['infer']}|{result['p2c']['hit']}/{result['p2c']['infer']}|{result['p2p']['hit']}/{result['p2p']['infer']}|{result['c2p']['hit']}/{result['c2p']['infer']}|{result['hard']['hit'][0]}/{result['hard']['infer'][0]}|{result['hard']['hit'][1]}/{result['hard']['infer'][1]}|{result['hard']['hit'][2]}/{result['hard']['infer'][2]}|{result['hard']['hit'][3]}/{result['hard']['infer'][3]}|{result['hard']['hit'][4]}/{result['hard']['infer'][4]}|{result['critical']['hit'][0]}/{result['critical']['infer'][0]}|{result['critical']['hit'][1]}/{result['critical']['infer'][1]}|{result['critical']['hit'][2]}/{result['critical']['infer'][2]}|{result['critical']['hit'][3]}/{result['critical']['infer'][3]}|{result['critical']['hit'][4]}/{result['critical']['infer'][4]}|{result['critical']['hit'][5]}/{result['critical']['infer'][5]}|\n'''
        )

        if os.path.exists('./cmp.md'):
            out = open('./cmp.md','a')
        else:
            out = open('./cmp.md','w')
            # put title
            out.write('||total|p2c|p2p|hard|h1|h2|h3|h4|critical|c1|c2|c3|c4|c5|\n')
            out.write('|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|\n')
        line=f'''|{last_name} precision|{result['total']['hit']/safe(result['total']['infer']):5f}/{cnts['total']}|{(result['p2c']['hit']+result['c2p']['hit'])/safe(result['p2c']['infer']+result['c2p']['infer']):5f}/{cnts['p2c']}|{result['p2p']['hit']/safe(result['p2p']['infer']):5f}/{cnts['p2p']}|{result['hard']['hit'][0]/safe(result['hard']['infer'][0]):5f}/{cnts['h total']}|{result['hard']['hit'][1]/safe(result['hard']['infer'][1]):5f}/{cnts['h type1']}|{result['hard']['hit'][2]/safe(result['hard']['infer'][2]):5f}/{cnts['h type2']}|{result['hard']['hit'][3]/safe(result['hard']['infer'][3]):5f}/{cnts['h type3']}|{result['hard']['hit'][4]/safe(result['hard']['infer'][4]):5f}/{cnts['h type4']}|{result['critical']['hit'][0]/safe(result['critical']['infer'][0]):5f}/{cnts['c total']}|{result['critical']['hit'][1]/safe(result['critical']['infer'][1]):5f}/{cnts['c type1']}|{result['critical']['hit'][2]/safe(result['critical']['infer'][2]):5f}/{cnts['c type2']}|{result['critical']['hit'][3]/safe(result['critical']['infer'][3]):5f}/{cnts['c type3']}|{result['critical']['hit'][4]/safe(result['critical']['infer'][4]):5f}/{cnts['c type4']}|{result['critical']['hit'][5]/safe(result['critical']['infer'][5]):5f}/{cnts['c type5']}|\n'''
        out.writelines(lines)
        # out.write(line)
        self.output_lines.append(line)
        out.close()

    def newone(self):
        def safe(a):
            if a == 0:
                return 1
            else:
                return a
        ff = open('./cmp2.md','w')
        ff.write('||total|p2c|p2p|hard|h1|h2|h3|h4|critical|c1|c2|c3|c4|c5|\n')
        ff.write('|-|-|-|-|-|-|-|-|-|-|-|-|-|-|-|\n')
        new_lines=[]
        min_err = {}
        max_err = {}
        data_err=[]
        data_num=[]
        index_err = []
        index_num =[]
        columns=['total','p2c','p2p','hard','h1','h2','h3','h4','critical','c1','c2','c3','c4','c5']
        for line in self.output_lines:
            elem = line.split('|')
            for idx in range(2,len(elem)-1):
                min_err.setdefault(idx,10)
                max_err.setdefault(idx,0)
                pr = 1-float(elem[idx].split('/')[0])
                if pr < min_err[idx] and pr != 0:
                    min_err[idx] = pr
                if pr > max_err[idx] and pr != 0:
                    max_err[idx] = pr

        def safe_d(a,b):
            if b==0:
                return 0
            else:
                return a/b
        for line in self.output_lines:
            elem = line.split('|')
            line_err = [elem[1]+' err']
            line_num = [elem[1]+' num']
            for idx in range(2,len(elem)-1):
                pr = 1-float(elem[idx].split('/')[0])
                # pr = safe_d(max_err[idx],pr)
                tot = elem[idx].split('/')[1]
                line_err.append(f'{pr:.6f}')
                line_num.append(f'{tot}')

            new_lines.append('|'+'|'.join(line_err)+'|\n')
            new_lines.append('|'+'|'.join(line_num)+'|\n')
            index_err.append(line_err[0])
            index_num.append(line_num[0])
            data_err.append([ f'{nn:3.2f}' if type(nn) is float else f'{nn}' for nn in line_err[1:]])
            data_num.append([ f'{nn:3.2f}' if type(nn) is float else f'{nn}' for nn in line_num[1:]])
        
            
        ff.writelines(new_lines)
        data = pd.DataFrame(data_err+data_num,index = index_err+index_num,columns=columns)
        data.to_csv('./cmp.csv')

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

clean = False

if __name__ == "__main__" and clean:
    comm.clear('/home/lwd/RIB.test/validation/validation_data.txt')
    quit()

if __name__ == "__main__":

#list
    valid_file = '/home/lwd/RIB.test/validation/validation_data.txt'
    path_file = '/home/lwd/RIB.test/path.test/pc202012.v4.u.path.clean'
    path6_file = '/home/lwd/RIB.test/path.test/pc202012.v6.u.path.clean'
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
    '/home/lwd/Result/BN/tsf.rel.bn',
    '/home/lwd/Result/BN/tsf_apf.rel.bn',
    '/home/lwd/Result/BN/ar_tsv_month.rel.bn',

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

    # '/home/lwd/Result/BN/tsf.rel.bn',
    # '/home/lwd/Result/BN/ar_tsv_month.rel.bn',
    # '/home/lwd/Result/BN/tsf_apf.rel.bn',
    ]


    sp_list=[
        '/home/lwd/Result/vote/apv/ap2_apv.rel',
        '/home/lwd/Result/vote/apv/tsf.rel',

        '/home/lwd/Result/vote/apv/ap2_bv.rel',
        '/home/lwd/Result/vote/apv/tsf_apf.rel',

        '/home/lwd/Result/vote/tsv/ap2_tsv_month.rel',
        '/home/lwd/Result/vote/tsv/ar_tsv_month.rel',
    ]

    un_list=[
        '/home/lwd/Result/auxiliary/pc20201201.v4.arout',
        '/home/lwd/Result/AP_working/rel_20201201.stg.1',
        '/home/lwd/Result/AP_working/rel_20201201.ap2',
    ]

    s1_list=[
        '/home/lwd/Result/auxiliary/rel_202012.ap2',
        '/home/lwd/Result/auxiliary/pc202012.v4.arout',

        '/home/lwd/Result/vote/tsv/ar_tsv.rel',
        '/home/lwd/Result/vote/apv/ar_apv.rel',
        '/home/lwd/Result/vote/apv/ar_bv.rel',
        '/home/lwd/Result/vote/tsv/ap2_tsv.rel',
        '/home/lwd/Result/vote/apv/ap2_apv.rel',
        '/home/lwd/Result/vote/apv/ap2_bv.rel',
    ]

    s2_list=[
        '/home/lwd/Result/BN/ar_tsv.rel.bn',
        # '/home/lwd/Result/BN/ar_apv.rel.bn',
        # '/home/lwd/Result/BN/ar_bv.rel.bn',
        '/home/lwd/Result/BN/stg.rel.bn',
        '/home/lwd/Result/BN/ap2_tsv.rel.bn',
        # '/home/lwd/Result/BN/ap2_apv.rel.bn',
        # '/home/lwd/Result/BN/ap2_bv.rel.bn',

        # '/home/lwd/Result/vote/tsv/ar_tsv.rel',
        # '/home/lwd/Result/NN/ar_tsv.fea.csv.nn',
    ]
    
    noirr=[
        '/home/lwd/Result/vote/tsv/ar_tsv.rel',
        '/home/lwd/Result/vote/tsv/ar_tsv_noirr.rel',
        '/home/lwd/Result/vote/tsv/ap2_tsv.rel',
        '/home/lwd/Result/vote/tsv/ap2_tsv_noirr.rel',

    ]

    ar_check = [
        '/home/lwd/Result/vote/tsv/ar_tsv.rel',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp0.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp1.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp2.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp3.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp4.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp5.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp6.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp7.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp8.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp9.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp10.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp11.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp12.ar',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp13.ar',
    ]
    c2f_check = [
        '/home/lwd/Result/vote/tsv/ap2_tsv.rel',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp0.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp1.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp2.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp3.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp4.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp5.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp6.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp7.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp8.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp9.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp10.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp11.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp12.ap2',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp13.ap2',
    ]
    stg_check = [
        '/home/lwd/Result/vote/tsv/stg_tsv.rel',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp0.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp1.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp2.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp3.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp4.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp5.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp6.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp7.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp8.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp9.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp10.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp11.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp12.stg',
        '/home/lwd/Result/TS_working/rel_wholemonth_vp13.stg',
    ]
    uny=[
        '/home/lwd/Result/vote/apv/uny_apv.rel.it',
        '/home/lwd/Result/vote/apv/uny_apv.rel.noit',
        '/home/lwd/Result/vote/apv/ap2_apv.rel.it',
        '/home/lwd/Result/vote/apv/ap2_apv.rel.noit',
        '/home/lwd/Result/AP_working/rel_20201201.uny.it',
        '/home/lwd/Result/AP_working/rel_20201201.uny.noit',
    ]

    tmp = [
        '/home/lwd/Result/vote/tsv/ar_tsv.rel',
        '/home/lwd/Result/vote/tsv/stg.rel',
        '/home/lwd/Result/vote/tsv/ap2_tsv.rel',

    ]

    v6 = [
        # '/home/lwd/Result/auxiliary/pc20201201.v6.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c3.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c4.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c5.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c6.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c7.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c8.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c13.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c15.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c17.arout',
        # '/home/lwd/Result/auxiliary/pc20201201.v6c20.arout',
        # '/home/lwd/Result/BN/ap2_vpg_v6.rel.bn',
        # '/home/lwd/Result/BN/ar_vpg_v6.rel.bn',
        # '/home/lwd/Result/BN/stg.rel.bn',

        # '/home/lwd/Result/BN/ap2_vpg_v6.rel.bn_0.5_10',
        # '/home/lwd/Result/BN/ap2_vpg_v6.rel.bn2',
        # '/home/lwd/Result/BN/ap2_vpg_v6.rel.bn_0.5_40',
        # '/home/lwd/Result/BN/ap2_vpg_v6.rel.bn_0.6_25',

        # '/home/lwd/Result/BN/ar_vpg_v6.rel.bn2',
        # '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.4_25',

        # '/home/lwd/Result/BN/stg.rel.bn_0.5_10',
        # '/home/lwd/Result/BN/stg.rel.bn2',
        # '/home/lwd/Result/BN/stg.rel.bn_0.5_40',
        # '/home/lwd/Result/BN/stg.rel.bn_0.4_25',
        # '/home/lwd/Result/BN/stg.rel.bn_0.6_25',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.5_5',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.5_10',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.5_15',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.5_20',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.5_25',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.5_30',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.5_35',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.5_40',

        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.1_25',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.2_25',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.3_25',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.4_25',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.5_25',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.6_25',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.7_25',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.8_25',


        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_ad52',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_ad62',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_ad72',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_ad82',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_ad83',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_ad84',
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_ad85',

        '/home/lwd/Result/BN/ar_vpg_v6.rel_ad72.bn_0.5_30',
        
        '/home/lwd/Result/BN/ar_vpg_v6.rel.bn_0.7_30',
    ]
    seed = [
        # '/home/lwd/Result/TS_working/rel_20201201_vp0.ap2',
        # '/home/lwd/Result/TS_working/rel_20201201_vp0.ap2.cmp',
        # '/home/lwd/Result/TS_working/rel_20201201_vp0.ap2.cmp1',
        # '/home/lwd/Result/TS_working/rel_20201201_vp0.ap2.lcmp',
        # '/home/lwd/Result/TS_working/rel_20201201_vp0.ap2.lcmp1',
        # '/home/lwd/Result/TS_working/rel_20201201_vp0.ap2.lcmp2',
        # '/home/lwd/Result/TS_working/rel_20201201_vp0.ap2.cv_lcmp2',
        # '/home/lwd/Result/auxiliary/pc20201201.v4.c_irr.ap2out',
        # '/home/lwd/Result/auxiliary/pc20201201.v4.irr.ap2out',
        # '/home/lwd/Result/auxiliary/pc20201201.v4.ap2out',
        '/home/lwd/Result/BN/ar_vpg.rel.bn',
        '/home/lwd/Result/BN/lap2_vpg.rel.bn',
        '/home/lwd/Result/BN/sap2_vpg.rel.bn',
        '/home/lwd/Result/BN/stg.rel.bn',
    ]
#usage
    e = comm(6)
    e.read(valid_file)
    # e.set_hnc_link(path_file)
    # quit()
    if os.path.exists(f'/home/lwd/Result/auxiliary/hncv{e.version}.link'):
        e.load_hnc()
    else:
        e.set_hnc_link(path_file)
    cnt=0


    test_file_list= []


    # go = noirr[0:2]
    # go = noirr[-2:]
    # go = tmp
    # go = s1_list
    go = seed
    for ff in go:
        last_name= ff.split('/')[-1]
        test_file_list.append(f'/home/lwd/Result/cmp/{last_name}')
        os.system(f'sort {ff}| uniq > /home/lwd/Result/cmp/{last_name}')
    # quit()

    os.system('rm ./cmp.md')
    # e.total_statistic('/home/lwd/Result/auxiliary/pc20201201.v4.arout',)
    # quit()
    for ff in test_file_list:
        if exists(ff):
            e.total_statistic_rev(ff)
            try:
                # e.compare(ff)
                pass
            except Exception as es:
                print(es)
        else:
            print(f'\033[7mnot exists: {ff}\033[0m')

    e.newone()

    # e.cmp2(valid_file,test_file_list[-7],test_file_list[-1])
    # e.cmp2(valid_file,test_file_list[0],test_file_list[1])