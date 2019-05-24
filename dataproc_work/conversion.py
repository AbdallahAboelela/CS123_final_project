import osmnx as ox
import pandas as pd
import pickle 




def converter(G, csv):
    '''
    '''
    col_names = ['node_ids', 'nu']
    df = pd.read_csv(csv)
    pickle.load(open(G_adj_path, 'rb'))
    nodes_proj, edges_proj = ox.graph_to_gdfs(G, nodes=True, edges=True)
    for node in nodes:
        time = edges_proj.loc[(edges_proj['u']==node[0]) & (edges_proj['v']==node[1]), 'time']

