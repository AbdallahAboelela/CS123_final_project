# Given our MRjob output csv, map the paths onto a matplotlib
# 17 May, 2019

import pandas as pd
import pickle, re, sys, os, osmnx as ox
import matplotlib.pyplot as plt

ONE_PERCENT = ['output/christ_eve.csv', 'output/christmas.csv']

# NODE_RE = re.compile('[0-9]{6,}')
# TIME_RE = re.compile('[0-9]\.[0-9]{1,}')
# YEAR_RE = re.compile('y[0-9]{4}')
# DAY_RE = re.compile('[a-z]{3}')

def get_nodes(g_fname):
    G = pickle.load(open(g_fname, "rb"))
    nodes = ox.graph_to_gdfs(G, nodes=True, edges = False)
    nodes = nodes[['x', 'y']]
    nodes.columns = ['lat', 'long']

    return nodes

def get_items(line):
    line = line.replace('\t', ',')
    line = line.replace(',,', ',')
    line = line.replace('"', '')
    line = line.replace('..', '.')
    line = line.replace('y', '')

    print(line)
    year, n1, n2, tod, time = line.split(',')

    return int(year), str(tod), int(n1), int(n2), float(time[:-1])

def get_formatted_edges(res_fname):
    # edges = pd.read_csv(res_fname, header = None)

    tods = []
    years = []
    n1s = []
    n2s = []
    times = []

    # for row in edges.iterrows():
    #     year, tod, n1, n2, time = get_regex(str(row))

    with open(res_fname) as f:
        for line in f:
            if len(line) > 10:
                year, tod, n1, n2, time = get_items(line)

                years.append(year)
                n1s.append(n1)
                n2s.append(n2)
                times.append(time)
                tods.append(tod)

    edges = pd.DataFrame()
    edges['year'] = years
    edges['n1'] = n1s
    edges['n2'] = n2s
    edges['time_spent'] = times
    edges['tod'] = tods

    if res_fname in ONE_PERCENT:
        edges['time_spent'] = edges['time_spent'] / 100

    return edges

def map(year, tod, nodes, edges):
    filtered = edges[edges['year'] == year]

    if tod != 'full':
        filtered = filtered[filtered['tod'] == tod]
    
    for _, row in filtered.iterrows():
        print(row)
        _, n1, n2, time_spent, tod = row

        lat1, lon1 = nodes.loc[n1]
        lat2, lon2 = nodes.loc[n2]

        plt.plot([lat1, lat2], [lon1, lon2], 'r', linewidth = 1, 
            alpha = 0.05 * time_spent)

    # plt.axis('off')
    plt.show()

    # plt.savefig('{}_{}_traffic.png'.format(year, tod), bbox_inches='tight')
    # plt.clf()

if __name__ == "__main__":
    tods = ['mor', 'aft', 'eve', 'nit', 'full']
    
    nodes = get_nodes('dataproc_work/G_adj.p')

    for fname in os.listdir('output/'):
    
        edges = get_formatted_edges('output/' + fname)

        for year in range(2009, 2017):
            for tod in tods:
                print('{}, {}, {}'.format(fname[:-4], year, tod))
                map(year, tod, nodes, edges)











    


