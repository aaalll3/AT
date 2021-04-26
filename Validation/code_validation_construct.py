import os, sys
import re
import multiprocessing
from collections import defaultdict
import sqlite3
import requests
from bs4 import BeautifulSoup

sys.path.append('..')
sys.path.append('.')

from location import *

class validation_construct:
    def __init__(self, args):

        self.input_dir = args[0]
        self.output_dir = args[1]

        self.community_dict = args[2]
        self.routeservers = args[3]
        self.asn_org = args[4]
        self.community_relationships = defaultdict(set)
        #print(len(self.community_dict.keys()), len(self.routeservers), len(self.asn_org.keys()))
    
        '''
        If a path contains unallocated ASNs, 
        return False, else return True.
        '''

    def ASNAllocated(self, arr):
        for asn in arr:
            asn = int(asn)
            if asn == 23456 or asn == 0:
                return False
            elif asn > 399260:
                return False
            elif asn > 64495 and asn < 131072:
                return False
            elif asn > 141625 and asn < 196608:
                return False
            elif asn > 210331 and asn < 262144:
                return False
            elif asn > 270748 and asn < 327680:
                return False
            elif asn > 328703 and asn < 393216:
                return False
        return True
    
    def constrcut_validation_data(self):
        validation_file = open(self.output_dir + os.sep + 'validation_data.txt', 'w')

        for file_name in os.listdir(self.input_dir):
            full_name = os.path.join(self.input_dir, file_name)
            if os.path.isfile(full_name) and 'v6' in full_name:
                file_handle = open(full_name, 'r')
                for line in file_handle:
                    line = line.strip()
                    temp = line.split('**')
                    if len(temp) == 2 and len(temp[0]) > 0 and len(temp[1]) > 0:
                        final_path = temp[0].split('|')
                        communities = temp[1].split()
                                
                        cr_dict = defaultdict(set)
                        for community in communities:
                            if community in self.community_dict.keys():
                                temp = community.split(':')
                                if temp[0] in final_path and final_path[-1] != temp[0]:
                                    idx = final_path.index(temp[0])
                                    marker = final_path[idx]
                                    neighbor = final_path[idx + 1]
                                    if ((marker in self.asn_org.keys() and neighbor in self.asn_org.keys()) and (self.asn_org[marker] != self.asn_org[neighbor])) or (marker not in self.asn_org.keys() or neighbor not in self.asn_org.keys()):
                                        if marker < neighbor:
                                            rel = None
                                            if self.community_dict[community] == '0':
                                                rel = '0'
                                            else:
                                                rel = str(- int(self.community_dict[community]))
                                            cr_dict[(marker, neighbor)].add(rel)
                                        elif marker > neighbor:
                                            cr_dict[(neighbor, marker)].add(self.community_dict[community])
                        
                        for link, rel_set in cr_dict.items():
                            if len(rel_set) == 1:
                                self.community_relationships[link].add(list(rel_set)[0])
                            elif len(rel_set) == 2:
                                if '0' in rel_set:
                                    rel_list = list(rel_set)
                                    rel_list.remove('0')
                                    self.community_relationships[link].add(rel_list[0])

                file_handle.close()
        
        for link, relationship_set in sorted(self.community_relationships.items(), key=lambda item: item[0][0]):
            validation_file.write(' '.join((link[0], link[1], '&'.join(relationship_set))) + '\n')


        validation_file.close()
        

        
        

def worker(args):
    print("Begin construct" + args[0] + " directory's validation data!" )
    extracter = validation_construct(args)
    extracter.constrcut_validation_data()
    print(args[0] + " directory's rib files have been succesfully extracted!" )
    
def get_community_dict(filename):
    community_dict = dict()
    file = open(filename, 'r')
    for line in file:
        line = line.strip()
        if len(line) > 0:
            temp = line.split()
            c = temp[0]
            rel = temp[1]
            if 'x' in c:
                num = c.count('x')
                end = 10 ** num
                for i in range(end):
                    replace_str = str(i).zfill(num)
                    idx = c.index('x')
                    c_temp = c[0:idx] + replace_str + c[idx + num : -1]
                    if not c_temp in community_dict.keys():
                        community_dict[c_temp] = rel
                    else:
                        print('Wrong in community file!')
                        sys.exit(-1)
            else:
                if not c in community_dict.keys():
                    community_dict[c] = rel
                else:
                    print(line + 'Wrong in community file!')
                    sys.exit(-1)
    return community_dict

def get_routeservers():
    routeservers = set()
    try:
        url = 'https://ixpdb.euro-ix.net/en/ixpdb/route-servers/'
        r = requests.get(url)
        r.raise_for_status()
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, 'html.parser')
        tr_nodes = soup.tbody.find_all('tr')
        for tr_node in tr_nodes:
            td_nodes = tr_node.find_all('td')
            routeservers.add(td_nodes[0].string.strip())
    except:
        print("Extract Community Relationships: Can't connect to Euro IX server!")
    

    conn = sqlite3.connect('/home/lwd/Result/auxiliary/peeringdb.sqlite3')
    c = conn.cursor()
    for row in c.execute("SELECT asn, info_type FROM 'peeringdb_network'"):
        asn, info_type = row
        if info_type == 'Route Server':
            routeservers.add(str(asn))
    
    print('Succesfully get routeserver list!')
    return routeservers

def extract_asn_org(file_name):
    asn_org = dict()
    try:
        org_file = open(file_name, 'r')
        format_cnt = 0
        for line in org_file:
            if line.startswith('# format'):
                format_cnt += 1
            if format_cnt == 2:
                temp = line.split('|')
                asn_org[temp[0]] = temp[3]
        print('Community relationships Extraction: Succesfully get ASN to organization mapping relationships!')
    except:
        print("Extract Community Relationships: Can't connect to CAIDA to get ASN to organization mapping!")
    
    return asn_org

if __name__ == "__main__":
    # set path
    comm_file=os.path.join(auxiliary,'relationship_communities.txt')
    asorg_file=os.path.join(auxiliary,'20201001.as-org2info.txt')
    input_dir=raw_path_dir
    output_dir=os.path.abspath('/home/lwd/RIB.test/validation6')
    
    checke(comm_file)
    checke(asorg_file)
    checke(input_dir)
    checke(output_dir)
    # quit()
    # working
    community_dict = get_community_dict(comm_file)
    routeserver_set = get_routeservers()
    mapping = extract_asn_org(asorg_file)
    worker([input_dir,output_dir,community_dict,routeserver_set,mapping])

    quit()
    input_root = os.path.abspath('..') + os.sep + 'prefix_path_community'
    output_root = 'validation_data'

    begin_year = 2020
    end_year = 2020

    begin_month = 8
    end_month = 11

    args_list = []
    for year in range(begin_year, end_year + 1):
        month_begin = 1
        month_end = 12
        if year == begin_year:
            month_begin = begin_month
        if year == end_year:
            month_end = end_month
        
        for month in range(month_begin, month_end + 1):
            input_dir = os.sep.join((input_root, str(year), str(month).zfill(2)))
            output_dir = os.sep.join((output_root, str(year), str(month).zfill(2)))
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            args_list.append([input_dir, output_dir, community_dict, routeserver_set, mapping])
    
    process_num = 12
    with multiprocessing.Pool(process_num) as pool:
        pool.map(worker, args_list)
    
    
            





