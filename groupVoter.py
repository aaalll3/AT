from collections import defaultdict
import numpy as np

class groupVoter(object):
    def __init__(self, fileNum, dir_name):
        self.fileNum = fileNum
        self.dir = dir_name
        self.prob = defaultdict(lambda: np.array([0.0, 0.0, 0.0]))
        self.linknum = defaultdict(int)
        self.trusted_link = dict()
        self.group_link_nums = []

    def getProb(self):
        for i in range(self.fileNum):
            file_link_num = 0
            _filename = self.dir + 'originASRel' + str(i) + '.txt'
            with open(_filename) as f:
                for line in f:
                    if line.startswith('#'):
                        continue
                    [asn1, asn2, rel] = line.strip().split('|')
                    asn1, asn2 = int(asn1), int(asn2)
                    if rel == '0':
                        self.prob[(asn1, asn2)] += np.array([1.0, 0.0, 0.0])
                        self.prob[(asn2, asn1)] += np.array([1.0, 0.0, 0.0])
                    elif rel == '-1':
                        self.prob[(asn1, asn2)] += np.array([0.0, 1.0, 0.0])
                        self.prob[(asn2, asn1)] += np.array([0.0, 0.0, 1.0])
                    elif rel == '1':
                        self.prob[(asn1, asn2)] += np.array([0.0, 0.0, 1.0])
                        self.prob[(asn2, asn1)] += np.array([0.0, 1.0, 0.0])
                    
                    self.linknum[(asn1, asn2)] += 1
                    self.linknum[(asn2, asn1)] += 1
                    file_link_num += 1
            self.group_link_nums.append(file_link_num)
        print(self.group_link_nums, sum(self.group_link_nums)/len(self.group_link_nums))
        self.writeResult()


    def writeResult(self):
        alllink = set()
        fout = open(self.dir + 'originrel_prime_prime.txt', 'w')
        for link in self.prob:
            if link in alllink:
                continue
            prob = self.prob[link]
            reverse_link = (link[1], link[0])
            seenNum = prob.sum()
            
            if prob[0] == seenNum:
                if link[0] < link[1]:
                    fout.write(str(link[0]) + '|' + str(link[1]) + '|0\n')
                else:
                    fout.write(str(link[1]) + '|' + str(link[0]) + '|0\n')
            elif prob[1] == seenNum and seenNum > 3:
                fout.write(str(link[0]) + '|' + str(link[1]) + '|-1\n')
            elif prob[2] == seenNum and seenNum > 3:
                fout.write(str(link[1]) + '|' + str(link[0]) + '|-1\n')

            alllink.add(link)
            alllink.add(reverse_link)
        fout.close()






if __name__ == "__main__":
    voter = Voter(10, 'tmp/')
    voter.getProb()
