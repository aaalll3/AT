import os 

from location import *
from os.path import join,abspath,exists



class comm():
    def __init__(self):
        self.ti = 0 
        self.tv = 0
        self.hit = 0
        self.cover = 0
        self.valid = dict()

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
            self.tv +=1
        print(f'total link in validaton set {self.tv}')
        vf.close()

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
            if rra == None and rrb ==None:
                continue
            elif rra == None:
                rr = rrb
                self.cover+=1
                if '&' in rr:
                    rrs = rr.split('&')
                    for rrr in rrs:
                        if int(rrr)==-int(rel):
                            self.hit +=1
                            break
                else:
                    if int(rr)==-int(rel):
                        self.hit +=1
            elif rrb == None:
                rr = rra
                self.cover+=1
                if '&' in rr:
                    rrs = rr.split('&')
                    for rrr in rrs:
                        if int(rrr)==int(rel):
                            self.hit +=1
                            break
                else:
                    if int(rr)==int(rel):
                        self.hit +=1
            else:
                rr = rra
                self.cover+=1
                if '&' in rr:
                    rrs = rr.split('&')
                    for rrr in rrs:
                        if int(rrr)==int(rel):
                            self.hit +=1
                            break
                else:
                    if int(rr)==int(rel):
                        self.hit +=1
            self.ti+=1
        print(f'result: \033[31mhit {self.hit}\033[0m \033[35mtotal {self.ti}\033[0m \033[36mcover {self.cover}\033[0m\n\033[31mprecesion {self.hit/self.ti}\033[0m \033[35mrecall {self.hit/self.tv}\033[0m \033[36mcover rate {self.cover/self.tv}\033[0m')
        rf.close()
        self.reset()

if __name__ == "__main__":
    valid_file = '/home/lwd/RIB.test/validation/validation_data.txt'
    file_list=['/home/lwd/Result/vote/tsv/tsf_20201201.rel',
    '/home/lwd/Result/vote/tsv/tsf_20201208.rel',
    '/home/lwd/Result/vote/tsv/tsf_20201215.rel',
    '/home/lwd/Result/vote/tsv/tsf_20201222.rel',
    '/home/lwd/Result/vote/apv/apf.rel',
    '/home/lwd/Result/vote/apv/tsf.rel',
    '/home/lwd/Result/vote/apv/tsf_apf.rel',
    '/home/lwd/code/Apollo.code/stage_1_res.txt',
    '/home/lwd/Result/auxiliary/pc20201201.v4.arout',
    '/home/lwd/Result/TS_working/rel_20201222_vp1.ar',
    '/home/lwd/Result/AP_working/rel_20201201.apr',
    '/home/lwd/Result/auxiliary/pc202012.v4.arout',]

    e = comm()

    e.read(valid_file)
    cnt=0
    # for k,v in e.valid.items():
    #     print(k,v)
    #     cnt+=1
    #     if cnt >20:
    #         break
    for ff in file_list:
        if exists(ff):
            e.compare(ff)
        else:
            print(f'not exists: {ff}')
