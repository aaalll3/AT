import networkx as nx

g= nx.Graph()
g.add_edge(1,2)
g.add_edge(2,3)
x = nx.algorithms.clique.find_cliques(g)
print(x)
print(list(x))