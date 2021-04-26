import random, os
from collections import defaultdict
import multiprocessing


from hierarchy import Hierarchy


class groupByOrigin:
    def __init__(self, groupNum, dir) -> None:
        self.groupNum = groupNum
        self.dir = dir
        if not os.path.exists(self.dir):
            os.mkdir(self.dir)
        self.tier = Hierarchy('/home/lwd/Result/auxiliary/pc202012.v4.arout')
        self.origin2path = defaultdict(set)
        self.allOrigin = set()
        self.cliqueOrigin = []
        self.highOrigin = []
        self.lowOrigin = []
        self.stubOrigin = []

        self.originList = []
        for i in range(self.groupNum):
            self.originList.append([])



    def getOriginGroup(self,full_path='pc202012.v4.u.path.clean'):
        with open(full_path) as f:
            for line in f:
                ASes = line.strip().split('|')
                origin = ASes[-1]
                self.origin2path[origin].add(line.strip())
                self.allOrigin.add(origin)
        for origin in self.allOrigin:
            if origin in self.tier.clique:
                self.cliqueOrigin.append(origin)
            elif origin in self.tier.high:
                self.highOrigin.append(origin)
            elif origin in self.tier.low:
                self.lowOrigin.append(origin)
            else:
                self.stubOrigin.append(origin)
        
        self.originGroup(self.cliqueOrigin)
        self.originGroup(self.highOrigin)
        self.originGroup(self.lowOrigin)
        self.originGroup(self.stubOrigin)
    
    def originGroup(self, originList):
        if len(originList) % self.groupNum == 0:
            groupSize = len(originList) // self.groupNum
        else:
            groupSize = len(originList) // self.groupNum + 1
        
        tempList = [i for i in originList]
        for i in range(self.groupNum):
            if len(tempList) < groupSize:
                for origin in tempList:
                    self.originList[i].append(origin)
            else:
                for _ in range(groupSize):
                    idx = random.randint(0, len(tempList) - 1)
                    self.originList[i].append(tempList.pop(idx))
    
    def writeOriginPath(self):
        for i in range(self.groupNum):
            f = open(self.dir + 'originASPath' + str(i) + '.txt', 'w')
            for origin in self.originList[i]:
                for path in self.origin2path[origin]:
                    f.write(path + '\n')
            f.close()

    def just_write_path(self,date):
        for i in range(self.groupNum):
            name = os.path.join(self.dir,f'path_{date}_ori{i}.path') 
            f = open(name, 'w')
            for origin in self.originList[i]:
                for path in self.origin2path[origin]:
                    f.write(path + '\n')
            f.close()
    
    def worker(self, args):
        src = args[0]
        dst = args[1]
        os.system("perl asrank.pl --clique clique.txt " + src + " > " + dst)


    def groupInfer(self):
        process_num = self.groupNum
        args_list = []
        for i in range(self.groupNum):
            args_list.append([self.dir + "originASPath" + str(i) + ".txt", self.dir + "originASRel" + str(i) + ".txt"])
        
        with multiprocessing.Pool(process_num) as pool:
            pool.map(self.worker, args_list)
        
    def just_divide(self,full_path,date):
        self.getOriginGroup(full_path)
        self.just_write_path(date)

    def run(self):
        self.getOriginGroup()
        self.writeOriginPath()
        self.groupInfer()



if __name__ == '__main__':
    grouper = groupByOrigin(10, 'tmp/')
    grouper.run()
    




