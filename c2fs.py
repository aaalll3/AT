
    def core2leaf(self, path_files, output_file,version=4):
        """
        A easy core to leaf infer with irr

        a|b|r 
        r has three values
        -1 for p2c
        1 for c2p
        0 for p2p
        4 for confilct link
        """
        thetier_1=None
        if version ==4 :
            thetier_1=self.tier_1
        else:
            thetier_1=self.tier_1_v6
        link_rel_c2f = dict()
        for path_file in path_files:
            pf = open(path_file,'r')
            for line in pf:
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
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
                    if ASes[i] in thetier_1:
                        if prime_t1 == i-1:
                            link_rel_c2f.setdefault((ASes[i-1],ASes[i]),0)
                        prime_t1 = i
                    # if ASes[i] in self.tier_1 and ASes[i+1] in self.tier_1:
                    #     self.link_rel_c2f.setdefault((ASes[i],ASes[i+1]),0)
            pf.close()
        wf = open(output_file,'w')
        for link,rel in link_rel_c2f.items():
            if rel != 4:
                line = f'{link[0]}|{link[1]}|{rel}\n'
                wf.write(line)
        wf.close()

    def c2f_loose(self, path_files, output_file, it = 5,version=4):
        """
        core to leaf followed by iterations
        """    
        thetier_1=None
        all_link=set()
        if version ==4 :
            thetier_1=self.tier_1
        else:
            thetier_1=self.tier_1_v6
        p1 = time.time()
        print('ap_it start')
        link_rel_ap = dict()
        non_t1 =list()
        if type(path_files) is not list:
            path_files = [path_files]
        for path_file in path_files:
            pf = open(path_file,'r')
            for line in pf:
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
                prime_t1 = 10000
                for i in range(len(ASes)-1):
                    all_link.add((ASes[i],ASes[i+1]))
                    all_link.add((ASes[i+1],ASes[i]))
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
                    if ASes[i] in thetier_1:
                        if prime_t1 == i-1:
                            link_rel_ap.setdefault((ASes[i-1],ASes[i]),0)
                        prime_t1 = i
                # if prime_t1 == 10000:
                non_t1.append(ASes)
            pf.close()
        p2 = time.time()
        print(f'done first time: {p2-p1}s')
        turn = 0
        while True:
            tmp = []
            convert = False
            turn += 1
            t1= time.time()
            print(f'start it{turn}, {len(non_t1)}')
            for ASes in non_t1:
                convert_sub = False
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
                    for i in range(idx_11+1,len(ASes)-1):
                        rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                        if rel is None:
                            convert = True
                            convert_sub = True
                            link_rel_ap[(ASes[i],ASes[i+1])] = -1
                        else:
                            if rel != -1:
                                link_rel_ap[(ASes[i],ASes[i+1])]=4
                if idx_1 !=0:
                    for i in range(idx_1-1):
                        rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                        if rel is None:
                            convert = True
                            convert_sub = True
                            link_rel_ap[(ASes[i],ASes[i+1])]=1 
                        else:
                            if rel != 1:
                                link_rel_ap[(ASes[i],ASes[i+1])]=4
                if idx_0 !=0:
                    if idx_0>=2:
                        for i in range(idx_0-1):
                            rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                            if rel is None:
                                convert = True
                                convert_sub = True
                                link_rel_ap[(ASes[i],ASes[i+1])]=1
                            else:
                                if rel != 1:
                                    link_rel_ap[(ASes[i],ASes[i+1])]=4
                    if idx_0<=len(ASes)-2:
                        for i in range(idx_0+1,len(ASes)-1):
                            rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                            if rel is None:
                                convert = True
                                convert_sub = True
                                link_rel_ap[(ASes[i],ASes[i+1])]=-1
                            else:
                                if rel != -1:
                                    link_rel_ap[(ASes[i],ASes[i+1])]=4
                if not convert_sub:
                    tmp.append(ASes)

            if not convert or turn >= it:
                break
            non_t1 = tmp
            t2= time.time()
            print(f'for it{turn}, takes {t2-t1}s')
        wf = open(output_file,'w')
        linkset = set()
        for link,rel in link_rel_ap.items():
            if link in linkset:
                continue
            rev = (link[1],link[0])
            asn1 = int(link[0])
            asn2 = int(link[1])
            if asn1 < asn2:
                line = f'{asn1}|{asn2}|{rel}\n'
            else:
                line = f'{asn2}|{asn1}|{-rel}\n'
            if rel != 4:
                wf.write(line)
            linkset.add(link)
            linkset.add(rev)
        for link in all_link:
            if link in linkset:
                continue
            rev = (link[1],link[0])
            asn1 = int(link[0])
            asn2 = int(link[1])
            if asn1 < asn2:
                line = f'{asn1}|{asn2}|{0}\n'
            else:
                line = f'{asn2}|{asn1}|{0}\n'
            if rel != 4:
                wf.write(line)
            linkset.add(link)
            linkset.add(rev)
        wf.close()
        p3= time.time()
        print(f'iteration takes {p3-p2}s')
        print(f'ap_it takes {p3-p1}s')

    def c2f_strict(self, path_files, output_file, it = 5,version=4):
        """
        core to leaf followed by iterations
        """    
        thetier_1=None
        all_link=set()
        if version ==4 :
            thetier_1=self.tier_1
        else:
            thetier_1=self.tier_1_v6
        p1 = time.time()
        print('ap_it start')
        link_rel_ap = dict()
        non_t1 =list()
        if type(path_files) is not list:
            path_files = [path_files]
        for path_file in path_files:
            pf = open(path_file,'r')
            for line in pf:
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
                prime_t1 = 10000
                for i in range(len(ASes)-1):
                    all_link.add((ASes[i],ASes[i+1]))
                    all_link.add((ASes[i+1],ASes[i]))
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
                    if ASes[i] in thetier_1:
                        if prime_t1 == i-1:
                            link_rel_ap.setdefault((ASes[i-1],ASes[i]),0)
                        prime_t1 = i
                # if prime_t1 == 10000:
                non_t1.append(ASes)
            pf.close()
        p2 = time.time()
        print(f'done first time: {p2-p1}s')
        turn = 0
        while True:
            tmp = []
            convert = False
            turn += 1
            t1= time.time()
            print(f'start it{turn}, {len(non_t1)}')
            for ASes in non_t1:
                convert_sub = False
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
                    for i in range(idx_11+1,len(ASes)-1):
                        rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                        if rel is None:
                            convert = True
                            convert_sub = True
                            link_rel_ap[(ASes[i],ASes[i+1])] = -1
                        else:
                            if rel != -1:
                                link_rel_ap[(ASes[i],ASes[i+1])]=4
                if idx_1 !=0:
                    for i in range(idx_1-1):
                        rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                        if rel is None:
                            convert = True
                            convert_sub = True
                            link_rel_ap[(ASes[i],ASes[i+1])]=1 
                        else:
                            if rel != 1:
                                link_rel_ap[(ASes[i],ASes[i+1])]=4
                if idx_0 !=0:
                    if idx_0>=2:
                        for i in range(idx_0-1):
                            rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                            if rel is None:
                                convert = True
                                convert_sub = True
                                link_rel_ap[(ASes[i],ASes[i+1])]=1
                            else:
                                if rel != 1:
                                    link_rel_ap[(ASes[i],ASes[i+1])]=4
                    if idx_0<=len(ASes)-2:
                        for i in range(idx_0+1,len(ASes)-1):
                            rel = link_rel_ap.get((ASes[i],ASes[i+1]))
                            if rel is None:
                                convert = True
                                convert_sub = True
                                link_rel_ap[(ASes[i],ASes[i+1])]=-1
                            else:
                                if rel != -1:
                                    link_rel_ap[(ASes[i],ASes[i+1])]=4
                if not convert_sub:
                    tmp.append(ASes)

            if not convert or turn >= it:
                break
            non_t1 = tmp
            t2= time.time()
            print(f'for it{turn}, takes {t2-t1}s')
        wf = open(output_file,'w')
        linkset = set()
        for link,rel in link_rel_ap.items():
            if link in linkset:
                continue
            rev = (link[1],link[0])
            asn1 = int(link[0])
            asn2 = int(link[1])
            if asn1 < asn2:
                line = f'{asn1}|{asn2}|{rel}\n'
            else:
                line = f'{asn2}|{asn1}|{-rel}\n'
            if rel != 4:
                wf.write(line)
            linkset.add(link)
            linkset.add(rev)
        wf.close()
        p3= time.time()
        print(f'iteration takes {p3-p2}s')
        print(f'ap_it takes {p3-p1}s')

    def c2f_strong(self,infile,outfile,it=1,version=4):
        thetier_1=None
        if version ==4 :
            thetier_1=self.tier_1
        else:
            thetier_1=self.tier_1_v6
        true_path=[]
        link2rel = {}
        clique1=None
        clique2=None
        with open(infile,'r') as ff:
            for line in ff:
                discard=False
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
                for i in range(len(ASes)-1):
                    if (ASes[i],ASes[i+1]) in self.irr_c2p:
                        link2rel[(ASes[i],ASes[i+1])]=1
                    if (ASes[i+1],ASes[i]) in self.irr_c2p:
                        link2rel[(ASes[i],ASes[i+1])]=-1
                    if (ASes[i],ASes[i+1]) in self.irr_p2p or (ASes[i+1],ASes[i]) in self.irr_p2p:
                        link2rel[(ASes[i],ASes[i+1])]=0
                    if ASes[i] in thetier_1 and ASes[i+1] in thetier_1:
                        link2rel[(ASes[i],ASes[i+1])]=0
                        link2rel[(ASes[i+1],ASes[i])]=0
                for i in range(len(ASes)):
                    if ASes[i] in thetier_1:
                        if clique1 is None:
                            clique1 = i
                        elif clique2 is None:
                            clique2 = i
                        else:
                            discard = True
                if clique1 and clique2:
                    if clique2 - clique1 != 1:
                        discard = True
                if discard:
                    continue
                true_path.append(ASes)
        ff.close()
        for turn in range(it):
            tmp_true_path=[]
            for ASes in true_path:
                descend=False
                discard = False
                for i in range(len(ASes)-1):
                    rel=  link2rel.get((ASes[i],ASes[i+1]))
                    if rel:
                        if rel == 1:
                            if descend:
                                discard = True
                                break
                        elif rel == 0:
                            if descend:
                                discard = True
                                break
                            descend = True
                        elif rel == -1:
                            descend = True
                        else:
                            discard= True
                            break
                if discard:
                    continue
                tmp_true_path.append(ASes)
            true_path=tmp_true_path
            added_rel={}
            for ASes in true_path:
                for i in range(len(ASes)-2):
                    rel =  link2rel.get((ASes[i],ASes[i+1]))
                    if rel:
                        if rel == 1:
                            pass
                        elif rel == 0:
                            added_rel.setdefault((ASes[i+1],ASes[i+2]),-1)
                        elif rel == -1:
                            added_rel.setdefault((ASes[i+1],ASes[i+2]),-1)
                        else:
                            pass
            for link,rel in added_rel.items():
                forward_rel = link2rel.get(link)
                backward_rel = link2rel.get((link[1],link[0]))
                if forward_rel is None and backward_rel is None:
                    link2rel[link]=rel
                    continue
                elif forward_rel and backward_rel is None:
                    if forward_rel == 1:
                        link2rel[link]=4
                elif forward_rel is None and backward_rel:
                    if backward_rel == -1:
                        link2rel[link]=4
                elif forward_rel and backward_rel:
                    if forward_rel == 1 and backward_rel == 1:
                        link2rel[link]=4
                    if forward_rel == -1 and backward_rel == -1:
                        link2rel[link]=4
        with open(outfile,'w') as of:
            for link,rel in link2rel.items():
                if rel ==4:
                    continue
                of.write(f'{link[0]}|{link[1]}|{rel}\n')

    def c2f_unary(self,infile, outfile,it):
        tier1s = [ '174', '209', '286', '701', '1239', '1299', '2828', '2914', '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
        untouched = []
        link2rel = {}
        last_tier1 = 10000
        print(f'start uny for {infile}')
        with open(infile,'r') as ff:
            for line in ff:
                last_tier1 = 10000
                if line.startswith('#'):
                    continue
                ASes = line.strip().split('|')
                for i in range(len(ASes)-1):
                    if (ASes[i],ASes[i+1]) in self.irr_c2p:
                        link2rel[(ASes[i],ASes[i+1])]=1
                    if (ASes[i+1],ASes[i]) in self.irr_c2p:
                        link2rel[(ASes[i],ASes[i+1])]=-1
                    if (ASes[i],ASes[i+1]) in self.irr_p2p or (ASes[i+1],ASes[i]) in self.irr_p2p:
                        link2rel[(ASes[i],ASes[i+1])]=0
                for i in range(len(ASes)):
                    if ASes[i] in tier1s:
                        if last_tier1 == i-1:
                            link2rel[(ASes[i-1],ASes[i])]=0
                        last_tier1 = i
                for i in range(last_tier1+1,len(ASes)-1):
                    link2rel.setdefault((ASes[i],ASes[i+1]),-1)
                    if link2rel.get((ASes[i+1],ASes[i])) == -1:
                        link2rel[(ASes[i],ASes[i+1])] = 4
                        link2rel[(ASes[i+1],ASes[i])] = 4
                if last_tier1 == 10000:
                    untouched.append(ASes)
        cnt = 0
        look = open('./lookit.txt','w')
        while(it):
            cnt +=1
            tmp_untouched=[]
            last_p2c = 10000
            convert = False
            print(f'it {cnt}: for {len(untouched)} paths')
            for ASes in untouched:
                pre={}
                post={}
                see = False
                last_p2c = 10000
                for i in range(len(ASes)-1):
                    rel = link2rel.get((ASes[i],ASes[i+1]))
                    if rel:
                        if rel == -1:
                            last_p2c = i
                    # pre[(i,i+1)]=rel
                if last_p2c == 10000:
                    tmp_untouched.append(ASes)
                    continue
                for i in range(last_p2c,len(ASes)-1):
                    link2rel.setdefault((ASes[i],ASes[i+1]),-1) 
                    if link2rel.get((ASes[i+1],ASes[i])) == -1:
                        link2rel[(ASes[i],ASes[i+1])] = 4
                        link2rel[(ASes[i+1],ASes[i])] = 4
                        see = True
                    convert = True
            untouched = tmp_untouched
            if not convert:
                break                
        with open(outfile,'w') as of:
            for link,rel in link2rel.items():
                if rel ==4:
                    continue
                of.write(f'{link[0]}|{link[1]}|{rel}\n')
    