import os
import sys
import multiprocessing
from location import checke,auxiliary,pure_path_dir,rdir,vdir
from groupByOrigin import groupByOrigin
from groupByVP import groupByVP
from stage1 import Struc
from stage2 import Stage2
import time


# for multiprocess
def use(args):
    args[0](args[1],args[2],args[3])

def use_bn_go(args):
    start = time.time()
    func = args[0]
    org = args[1]
    peer = args[2]
    rel = args[3]
    prob = args[4]
    path = args[5]
    out = args[6]
    func(org,peer,rel,prob,path,out)
    end = time.time()
    print(f'done compute {rel}, takes {end-start}s')

class Infeur(object):
    def __init__(self, path_file, irr_file = None, boost_file = None, org_file = None, peering_file=None, id = 'who', working_dir_name='tmp',version = 4,remove =False) -> None:
        if remove:
            print('\033[31mremove ON!\033[0m') 
            time.sleep(0.5)
        # first create struc
        self.struc = Struc()

        self.path_file = os.path.join(pure_path_dir,path_file)
        self.irr_file = irr_file if irr_file else os.path.join(auxiliary,'irr.txt')
        self.boost_file = boost_file if boost_file else os.path.join(auxiliary,'boost_file.ar')
        self.org_name = org_file if org_name else os.path.join(auxiliary,'20201001.as-org2info.txt')
        self.peering_name = peering_name if peering_name else os.path.join(auxiliary,'peeringdb.sqlite3')
        if version == 4:
            self.ar_version = os.path.join(os.path.abspath('./TopoScope'),'asrank_irr.pl')
        elif version == 6:
            self.ar_version = os.path.join(os.path.abspath('./TopoScope'),'asrank_irr_v6.pl')
        if not checke(self.boost_file):
            self.struc.boost(self.ar_version,self.path_file,self.boost_file)
        dependent_file = [self.path_file,self.irr_file,self.boost_file,self.org_name,self.peering_name,self.ar_version,]
        for name in dependent_file:
            assert checke(name)
        # init
        self.struc = Struc()
        self.id = id
        self.version = version
        self.remove = remove
        self.fulldir_name = None
        self.fullvote_name = None
        self.param = {
            'group_size':25
        }

        self.use = use
        self.use_bn_go = use_bn_go
        self.prepare(working_dir_name)


    def prepare(self,workingd_dir_name):
        '''
        read all path and divde path according to VP
        '''
        p1 = time.time()
        print('\033[31mNOW\033[0m prepare')
        thisround = workingd_dir_name
        fulldir_name  = os.path.join(rdir,thisround)
        fullvote_name = os.path.join(vdir,thisround)
        print(f'infer run at {thisround}')
        if checke(fulldir_name):
            if self.remove:
                os.system(f'rm -r {fulldir_name}')
            else:
                raise AssertionError(f'{fulldir_name} already occupied')
        if checke(fullvote_name) and self.remove:
            if self.remove:
                os.system(f'rm -r {fullvote_name}')
            else:
                raise AssertionError(f'{fullvote_name} already occupied')
        os.system(f'mkdir {fulldir_name}')
        os.system(f'mkdir {fullvote_name}')
        self.fulldir_name = fulldir_name
        self.fullvote_name = fullvote_name

        self.vpg = groupByVP()
        self.orig =  groupByOrigin(25,self.fulldir_name)
        self.vpg.get_relation(self.boost_file)
        self.vpg.cal_hierarchy(self.version)
        self.vpg.set_VP_type(self.path_file,self.version)
        self.vpg.clean_vp(self.fulldir_name)
        if len(os.listdir(self.fulldir_name)):
            print('\033[31munclean\033[0m')
        else:
            print('\033[32mcleaned\033[0m')
        self.vpg.divide_VP(self.param['group_size'],self.fulldir_name,self.id)

        self.orig.just_divide(self.path_file,self.id)
        p2 = time.time()
        print(f'\033[31mdone\033[0m prepare, takes {p2-p1:.2f} seconds')

    def basic(self):
        '''
        four basic infer method
        c2f loose 
        c2f strict
        as rank
        strong rule
        '''
        print('\033[31mNOW\033[0m infer')
        files = os.listdir(self.fulldir_name)
        in_files = []
        out_files = []
        for file_name in files:
            if file_name.startswith('path'):
                in_files.append(os.path.join(self.fulldir_name,file_name))
                oname= file_name.split('.')[0]
                oname = oname.replace('path','rel')
                out_files.append(os.path.join(self.fulldir_name,oname))

        # run as rank separately
        for ii,oo in zip(in_files,out_files):
            self.struc.infer_ar(self.ar_version,ii,oo+'.ar')

        # for multiprocess
        def use(args):
            args[0](args[1],args[2],args[3],args[4])

        # adding c2f arguments: function, path file, output file, iterations(now discard), version
        args = []
        for ii,oo in zip(in_files,out_files):
            args.append([self.struc.c2f_loose,ii,oo+'.lap2',irr_file])
            args.append([self.struc.c2f_strict,ii,oo+'.sap2',irr_file])
        with multiprocessing.Pool(96) as pool:
            pool.map(self.use,args)
        
        self.struc.c2f_strong(self.path_file,os.path.join(self.fulldir_name,f'rel_{self.id}.stg'),1)

    def vote(self):
        '''
        vote by vp over ASrank and Core2leaf results,
        get probability of strong rule file
        '''
        print('\033[31mNOW\033[0m vote')
        files = os.listdir(self.fulldir_name)
        file_num = 0
        for name  in files:
            if name.endswith('.path'):
                if 'vp' in name:
                    file_num+=1

# vp
        file_list=[f'rel_{self.id}_vp{i}.lap2' for i in range(0,file_num)]
        file_list=[os.path.join(self.fulldir_name,one) for one in file_list]
        output_file=os.path.join(self.fullvote_name,'lap2_vpg.rel')
        self.struc.vote_simple_vp(file_num,file_list,output_file,path_file=self.path_file)

        file_list=[f'rel_{self.id}_vp{i}.sap2' for i in range(0,file_num)]
        file_list=[os.path.join(self.fulldir_name,one) for one in file_list]
        output_file=os.path.join(self.fullvote_name,'sap2_vpg.rel')
        self.struc.vote_simple_vp(file_num,file_list,output_file,path_file=self.path_file)

        file_list=[f'rel_{self.id}_vp{i}.ar' for i in range(0,file_num)]
        file_list=[os.path.join(self.fulldir_name,one) for one in file_list]
        output_file=os.path.join(self.fullvote_name,'ar_vpg.rel')
        self.struc.vote_simple_vp(file_num,file_list,output_file,path_file=self.path_file)

# ori
        # file_list=[f'rel_{self.id}_ori{i}.lap2' for i in range(0,file_num)]
        # file_list=[os.path.join(self.fulldir_name,one) for one in file_list]
        # output_file=os.path.join(self.fullvote_name,'lap2_orig.rel')
        # self.struc.vote_simple_ori(file_num,file_list,output_file,path_file=self.path_file)

        # file_list=[f'rel_{self.id}_ori{i}.sap2' for i in range(0,file_num)]
        # file_list=[os.path.join(self.fulldir_name,one) for one in file_list]
        # output_file=os.path.join(self.fullvote_name,'sap2_orig.rel')
        # self.struc.vote_simple_ori(file_num,file_list,output_file,path_file=self.path_file)

        # file_list=[f'rel_{self.id}_ori{i}.ar' for i in range(0,file_num)]
        # file_list=[os.path.join(self.fulldir_name,one) for one in file_list]
        # output_file=os.path.join(self.fullvote_name,'ar_orig.rel')
        # self.struc.vote_simple_ori(file_num,file_list,output_file,path_file=self.path_file)

        print('NOW prob')
        stg_file = os.path.join(self.fulldir_name,f'rel_{self.id}.stg')
        outf = os.path.join(self.fullvote_name,'stg.rel')
        os.system(f'cp {stg_file} {outf}')
        stg_file=[outf]
        self.struc.prob_simple_vp(1,stg_file,outf,path_file=self.path_file)

    def stage2(self):
        '''
        run BN over relationship file and coresponding probability
        '''
        stage2= Stage2()
        print('\033[31mNOW\033[0m bn')
        vp_files=[
        'lap2_vpg.rel',
        'sap2_vpg.rel',
        'ar_vpg.rel',
        'stg.rel',
        ]
        vp_files=[ os.path.join(self.fullvote_name,name) for name in vp_files]

        rels = vp_files 

        mp_args=[]
        for name in rels:
            print(f'adding {name}')
            checke(name)
            outname = name.strip().split('/')[-1]
            outname = outname+'.bn'
            outname = os.path.join('/home/lwd/Result/BN',outname)
            checke(name)
            checke(name+'.prob')
            mp_args.append([stage2.BN_go,self.org_name,self.peering_name,name,name+'.prob',self.path_file,outname,6])
            # BN_go(org_name,peering_name,name,name+'.prob',path_file,outname)

        with multiprocessing.Pool(50) as pool:
            pool.map(self.use_bn_go,mp_args)
    
    def done(self):
        print('\033[31mNOW\033[0m finishing')
        if self.remove:
            print(f'\033[31mremove \033[0m {self.fulldir_name}')
            print(f'\033[31mremove \033[0m {self.fullvote_name}')
            os.system(f'rm -r {self.fulldir_name}')
            os.system(f'rm -r {self.fullvote_name}')

    def infer(self):
        p1 = time.time()
        self.basic()
        p2 = time.time()
        print(f'\033[32mdone\033[0m infer, takes {p2-p1:.2f} seconds')
        self.vote()
        p3 = time.time()
        print(f'\033[32mdone\033[0m vote, takes {p3-p2:.2f} seconds')
        self.stage2()
        p4 = time.time()
        print(f'\033[32mdone\033[0m bn, takes {p4-p3:.2f} seconds')
        self.done()
        p5 = time.time()
        print(f'\033[33mtotal time consumption: {p5-p1:.2f} seconds\033[0m')

if __name__ == '__main__':
    path_file =os.path.join(pure_path_dir,'pc20201201.v4.u.path.clean')
    irr_file='/home/lwd/Result/auxiliary/irr.txt'
    boost_file='/home/lwd/Result/auxiliary/pc20201201.v4.arout'
    org_name= os.path.join(auxiliary,'20201001.as-org2info.txt')
    peering_name= os.path.join(auxiliary,'peeringdb.sqlite3')
    infeur = Infeur(path_file=path_file,irr_file=irr_file,boost_file=boost_file,org_file=org_name,peering_file=peering_name,id='whocare',working_dir_name='tmp',version=4,remove=True)
    infeur.infer()

