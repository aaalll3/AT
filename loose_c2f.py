import time
import re

def loose( irr_file, path_file, output_file, it = 5):
    """
    core to leaf followed by iterations
    path_file: location of path file
    output_file: location of output relation file
    it: iteration times
    """    
    irr_c2p = set()
    irr_p2p = set()

    with open(irr_file,'r') as f:
        lines = f.readlines()
    for line in lines:
        # tmp = line.strip().split('|')
        tmp = re.split(r'[\s]+',line)
        if tmp[2] == '1':
            irr_c2p.add((tmp[0],tmp[1]))
        if tmp[2] == '0':
            irr_p2p.add((tmp[0],tmp[1]))
        if tmp[2] == '-1':
            irr_c2p.add((tmp[1],tmp[0]))

    thetier_1 = ['174', '209', '286', '701', '1239', '1299', '2828', '2914', 
        '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956']
    p1 = time.time()
    print('loose core2leaf start')
    link_rel_ap = dict()
    all_link=set()
    non_t1 =list()
    if type(path_file) is not list:
        path_file = [path_file]
    for path_file in path_file:
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
                if(ASes[i],ASes[i+1]) in irr_c2p:
                    link_rel_ap.setdefault((ASes[i],ASes[i+1]),1)
                if(ASes[i+1],ASes[i]) in irr_c2p:
                    link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                if(ASes[i],ASes[i+1]) in irr_p2p or (ASes[i+1],ASes[i]) in irr_p2p:
                    link_rel_ap.setdefault((ASes[i],ASes[i+1]),0)
                if ASes[i] in thetier_1:
                    if prime_t1 == i-1:
                        link_rel_ap.setdefault((ASes[i-1],ASes[i]),0)
                    prime_t1 = i
                if prime_t1 == 10000:
                    non_t1.append(ASes)
        pf.close()
    p2 = time.time()
    print(f'done first time: {p2-p1}s')
    for turn in range(it):
        t1= time.time()
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
                for i in range(idx_11+1,len(ASes)-1):
                    rel = link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                    if rel != -1:
                        link_rel_ap[(ASes[i],ASes[i+1])]=4
            if idx_1 !=0:
                for i in range(idx_1-1):
                    rel=link_rel_ap.setdefault((ASes[i],ASes[i+1]),1)
                    if rel != 1:
                        link_rel_ap[(ASes[i],ASes[i+1])]=4
            if idx_0 !=0:
                if idx_0>=2:
                    for i in range(idx_0-1):
                        rel = link_rel_ap.setdefault((ASes[i],ASes[i+1]),1)
                        if rel != 1:
                            link_rel_ap[(ASes[i],ASes[i+1])]=4
                if idx_0<=len(ASes)-2:
                    for i in range(idx_0+1,len(ASes)-1):
                        rel = link_rel_ap.setdefault((ASes[i],ASes[i+1]),-1)
                        if rel != -1:
                            link_rel_ap[(ASes[i],ASes[i+1])]=4
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
    print(f'loose c2f takes {p3-p1}s')
