from Knn import Knn
import os
from os import path
import sys
import re
import logging
import resource
import networkx as nx
import json
from networkx.algorithms.structuralholes import mutual_weight
import pandas as pd
import time
import numpy as np
import copy, math, operator, random, scipy, sqlite3
import matplotlib.pyplot as plt
import lightgbm as lgb
import xgboost as xgb
import multiprocessing

from itertools import permutations
from random import shuffle
from collections import defaultdict
from collections import Counter
from os.path import abspath, join, exists
from networkx.algorithms.centrality import group
from logging import warn,debug,info
from sklearn import preprocessing
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neighbors import NearestNeighbors
from sklearn.model_selection import train_test_split

from TopoScope.topoFusion import TopoFusion
from location import *
from hierarchy import Hierarchy

resource.setrlimit(resource.RLIMIT_STACK, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

log_location = abspath(join('./log',f'log_{time.time()}'))
logging.basicConfig(filename=log_location,level=logging.INFO)


from link import Links
from BN import BN

class Stage2():
    def __init__(self):
        pass

    def BN_go(self,org_name,peering_name,rel_file,prob_file,path_file,output_file,version=4):
        my = BN()
        my.BN_go(org_name,peering_name,rel_file,prob_file,path_file,output_file,version)

    def NN_go(self,input_file,output):
        my = Knn()
        my.NN_go(input_file,output)