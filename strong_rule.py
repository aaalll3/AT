import re


def strong(irr_file, infile,outfile,K=1):
    '''
    strong rule
    path_file: location of path file
    output_file: location of output relation file
    it: iteration times
    '''
    irr_c2p = set()
    irr_p2p = set()

    with open(irr_file,'r') as f:
        lines = f.readlines()
    for line in lines:
        tmp = re.split(r'[\s]+',line)
        if tmp[2] == '1':
            irr_c2p.add((tmp[0],tmp[1]))
        if tmp[2] == '0':
            irr_p2p.add((tmp[0],tmp[1]))
        if tmp[2] == '-1':
            irr_c2p.add((tmp[1],tmp[0]))

    thetier_1=['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
        '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
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
                if (ASes[i],ASes[i+1]) in irr_c2p:
                    link2rel[(ASes[i],ASes[i+1])]=1
                if (ASes[i+1],ASes[i]) in irr_c2p:
                    link2rel[(ASes[i],ASes[i+1])]=-1
                if (ASes[i],ASes[i+1]) in irr_p2p or (ASes[i+1],ASes[i]) in irr_p2p:
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
    for _ in range(K):
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
            first = True
            for i in range(len(ASes)-2):
                rel =  link2rel.get((ASes[i],ASes[i+1]))
                if rel:
                    if rel == 1:
                        pass
                    elif rel == 0:
                        added_rel.setdefault((ASes[i+1],ASes[i+2]),-1)
                        first=False
                    elif rel == -1:
                        added_rel.setdefault((ASes[i+1],ASes[i+2]),-1)
                        first=False
                    else:
                        first=False
                        pass
                if not first:
                    break
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