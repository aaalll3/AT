import time

#######################################################################
### Core to leaf algorithm to infer AS relationships:               ###
###     Usage: core2leaf_loose(path_file, output_file, irr_file)          ###
###     irr_file = None: don't use irr file                         ###
#######################################################################

class core2leaf_loose:
    def __init__(self, path_file, output_file, irr_file = None):
        #about IRR links
        self.irr_file = irr_file
        self.irr_rels = dict()
        if irr_file:
            self.get_irr_relationship()
        #about inferring
        self.path_file = path_file
        self.output_file = output_file
        self.clique_set = set(('174', '209', '286', '701', '1239', '1299', '2828', '2914', 
        '3257', '3320', '3356', '3491', '5511', '6453', '6461', '6762', '6830', '7018', '12956'))
        self.link_rel = dict()
        self.all_link = set()
        self.paths = []
        self.change_num = 0
    
    def get_irr_relationship(self):
        with open(self.irr_file) as f:
            for line in f:
                temp = line.strip().split()
                if len(temp) != 3:
                    continue
                link = (temp[0], temp[1])
                rel = temp[2]
                #make link[0] < link[1]
                if link[0] > link[1]:
                    link = (temp[1], temp[0])
                    rel = str(-int(rel))
                self.irr_rels[link] = rel

    def infer_from_clique_path(self):
        begin_time = time.time()
        with open(self.path_file) as f:
            for line in f:
                if line.startswith('#'):
                    continue
                asn_list = line.strip().split('|')
                clique_idx = None
                for i in range(0, len(asn_list) - 1):
                    link = (asn_list[i], asn_list[i + 1])
                    #all link set
                    self.all_link.add(link)
                    #add irr rels to inference result
                    if link in self.irr_rels.keys():
                        self.link_rel[link] = self.irr_rels[link]
                    elif (link[1], link[0]) in self.irr_rels.keys():
                        self.link_rel[link] = str(-int(self.irr_rels[(link[1], link[0])]))
                    #jump the link next to clique AS
                    if asn_list[i] in self.clique_set:
                        clique_idx = i
                        if asn_list[i + 1] in self.clique_set:
                            self.link_rel[link] = '0'
                        continue
                    #if previous as has clique as
                    if clique_idx != None:
                        rel = '-1'
                        #if previous rel not equal rel now
                        old_rel = self.link_rel.setdefault(link, rel)
                        if old_rel != rel:
                            self.link_rel[link] = '2'
                    #add to paths for iteration
                    self.paths.append(asn_list)
        end_time = time.time()
        print(f'Infer from clique path done: {end_time - begin_time}s.')
    
    
    def get_p2c_along(self, asn_list, first_p2c):
        for i in range(first_p2c + 1, len(asn_list) - 1):
            link = (asn_list[i], asn_list[i + 1])
            rel = '-1'
            if link not in self.link_rel.keys():
                self.link_rel[link] = rel
                self.change_num += 1
            else:
                if self.link_rel[link] != rel and self.link_rel[link] != '2':
                    self.link_rel[link] = '2'
                    self.change_num += 1
    
    def get_c2p_along(self, asn_list, last_c2p):
        for i in range(0, last_c2p):
            link = (asn_list[i], asn_list[i + 1])
            rel = '1'
            if link not in self.link_rel.keys():
                self.link_rel[link] = rel
                self.change_num += 1
            else:
                if self.link_rel[link] != rel and self.link_rel[link] != '2':
                    self.link_rel[link] = '2'
                    self.change_num += 1

    def iteration(self):
        time_now = time.time()
        iteration_time = 0
        while True:
            self.change_num = 0
            for asn_list in self.paths:
                first_p2c = None
                last_c2p = None
                p2p_arr = []
                for i in range(0, len(asn_list) - 1):
                    if first_p2c == None and ((asn_list[i], asn_list[i + 1]) in self.link_rel.keys() and self.link_rel[(asn_list[i], asn_list[i + 1])] == '-1'):
                        first_p2c = i
                    elif ((asn_list[i], asn_list[i + 1]) in self.link_rel.keys() and self.link_rel[(asn_list[i], asn_list[i + 1])] == '1'):
                        last_c2p = i
                    elif ((asn_list[i], asn_list[i + 1]) in self.link_rel.keys() and self.link_rel[(asn_list[i], asn_list[i + 1])] == '0')\
                        or ((asn_list[i + 1], asn_list[i]) in self.link_rel.keys() and self.link_rel[(asn_list[i + 1], asn_list[i])] == '0'):
                        p2p_arr.append(i)
                if len(p2p_arr) > 1:
                    continue
                elif len(p2p_arr) == 1:
                    p2p_idx = p2p_arr[0]
                    self.get_c2p_along(asn_list, p2p_idx) 
                    self.get_p2c_along(asn_list, p2p_idx)
                if first_p2c != None and first_p2c < len(asn_list) - 2:
                    self.get_p2c_along(asn_list, first_p2c)
                if last_c2p != None:
                    self.get_c2p_along(asn_list, last_c2p)
            iteration_time += 1
            end_time = time.time()
            print(f'Infer from iteration {iteration_time} done: {end_time - time_now}s; change num: {self.change_num}')
            if self.change_num == 0:
                print('Iteration done!')
                break
            time_now = end_time   

    def remove_duplicate(self):
        new_link_rel = dict()
        for link, rel in self.link_rel.items():
            reverse = False
            if link[0] > link[1]:
                link = (link[1], link[0])
                reverse = True
            if link in new_link_rel.keys():
                continue
            if rel == '2':
                new_link_rel[link] = '2'
            elif rel == '-1':
                if reverse:
                    link = (link[1], link[0])
                if (link[1], link[0]) in self.link_rel.keys():
                    if self.link_rel[(link[1], link[0])] != '1':
                        if reverse:
                            new_link_rel[(link[1], link[0])] = '2'
                        else:
                            new_link_rel[link] = '2'
                    else:
                        if reverse:
                            new_link_rel[(link[1], link[0])] = '1'
                        else:
                            new_link_rel[link] = '-1'
                else:
                    if reverse:
                        new_link_rel[(link[1], link[0])] = '1'
                    else:
                        new_link_rel[link] = '-1'
            elif rel == '1':
                if reverse:
                    link = (link[1], link[0])
                if (link[1], link[0]) in self.link_rel.keys():
                    if self.link_rel[(link[1], link[0])] != '-1':
                        if reverse:
                            new_link_rel[(link[1], link[0])] = '2'
                        else:
                            new_link_rel[link] = '2'
                    else:
                        if reverse:
                            new_link_rel[(link[1], link[0])] = '-1'
                        else:
                            new_link_rel[link] = '1'
                else:
                    if reverse:
                        new_link_rel[(link[1], link[0])] = '-1'
                    else:
                        new_link_rel[link] = '1'
            elif rel == '0':
                if reverse:
                    link = (link[1], link[0])
                if (link[1], link[0]) in self.link_rel.keys():
                    if self.link_rel[(link[1], link[0])] != '0':
                        if reverse:
                            new_link_rel[(link[1], link[0])] = '2'
                        else:
                            new_link_rel[link] = '2'
                    else:
                        if reverse:
                            new_link_rel[(link[1], link[0])] = '0'
                        else:
                            new_link_rel[link] = '0'
                else:
                    if reverse:
                        new_link_rel[(link[1], link[0])] = '0'
                    else:
                        new_link_rel[link] = '0'
        self.link_rel = new_link_rel

                            

    def set_p2p(self):
        for link in self.all_link:
            if link[0] > link[1]:
                link = (link[1], link[0])
            if link in self.link_rel.keys():
                continue
            self.link_rel[(link)] = '0'
    
    def write_result(self):
        with open(self.output_file, 'w') as f:
            for link, rel in self.link_rel.items():
                if rel == '2':
                    continue
                f.write('|'.join((link[0], link[1], rel)) + '\n')
    
    def run(self):
        self.infer_from_clique_path()
        self.iteration()
        self.remove_duplicate()
        self.set_p2p()
        self.write_result()
        
if __name__ == "__main__":
    instance = core2leaf_loose('pc202012.v4.u.path.clean', 'core2leaf_loose.txt', None)
    instance.run()










