# Given our MRjob output csv, map the paths onto a matplotlib
# 17 May, 2019

import pandas as pd
import pickle, re, sys, osmnx as ox
import matplotlib.pyplot as plt

NODE_RE = re.compile('[0-9]{5,}')
TIME_RE = re.compile('[0-9]\.[0-9]{1,}')

def get_nodes(g_fname):
    G = pickle.load(open(g_fname, "rb"))
    nodes = ox.graph_to_gdfs(G, nodes=True, edges = False)
    nodes = nodes[['x', 'y']]
    nodes.columns = ['lat', 'long']

    return nodes

def get_n_and_t(row):
    n1, n2 = NODE_RE.findall(row)
    time = TIME_RE.search(row).group()

    return int(n1), int(n2), float(time)

def get_formatted_edges(res_fname):
    edges = pd.read_csv(res_fname, header = None)

    n1s = []
    n2s = []
    times = []

    for row in edges.iterrows():
        n1, n2, time = get_n_and_t(str(row))

        n1s.append(n1)
        n2s.append(n2)
        times.append(time)

    edges['n1'] = n1s
    edges['n2'] = n2s
    edges['time_spent'] = times

    return edges.iloc[:, 1:]

def map(res_fname, g_fname):
    nodes = get_nodes(g_fname)
    edges = get_formatted_edges(res_fname)

    for _, row in edges.iterrows():
        n1, n2, time_spent = row

        lat1, lon1 = nodes.loc[n1]
        lat2, lon2 = nodes.loc[n2]

        plt.plot([lat1, lat2], [lon1, lon2], 'r', linewidth = 1, 
            alpha = 0.05 * time_spent)

    plt.show()

if __name__ == "__main__":
    res_fname = sys.argv[1]
    g_fname = sys.argv[2]

    map(res_fname, g_fname)














    


