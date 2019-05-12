'''
Purpose: Merge polygon shapefiles for New York boroughs, then turn
    into OSMNX Graph
'''
import osmnx as ox
import geopandas as gpd
from shapely.ops import cascaded_union
import itertools
import pickle

def construct_G_adj():
    borough_bds = gpd.read_file('borough_bds.geojson')
    ## Source: https://stackoverflow.com/questions/40385782/
    ## make-a-union-of-polygons-in-python-geopandas-or-shapely-into-a-single-geometr
    polygon = gpd.GeoSeries(cascaded_union(borough_bds['geometry']))
    G = ox.graph_from_polygon(polygon[0], network_type='drive')


    speed_lims = gpd.read_file('speed_lims.geojson')
    speed_lims.drop(['shape_leng', 'postvz_sg', 'street'], axis=1, inplace=True)
    speed_lims['postvz_sl'][speed_lims['postvz_sl'] == '0'] = 25

    speed_lims_bounds = speed_lims['geometry'].bounds
    speed_lims['u'] = ox.get_nearest_nodes(G, speed_lims_bounds['minx'],\
        speed_lims_bounds['miny'], method='kdtree')
    speed_lims['v'] = ox.get_nearest_nodes(G, speed_lims_bounds['maxx'],\
        speed_lims_bounds['maxy'], method='kdtree')
    speed_lims['A'] = speed_lims['u'].map(str) + ' ' + speed_lims['v'].astype(str)
    speed_lims['B'] = speed_lims['v'].map(str) + ' ' + speed_lims['u'].astype(str)

    speed_lims.drop(['geometry', 'u', 'v'], axis=1, inplace=True)

    ## speed_lims columns: [postvz_sl, node_1, node_2]

    # dataframe 
    nodes_proj, edges_proj = ox.graph_to_gdfs(G, nodes=True, edges=True)
    edges_proj['A'] = edges_proj['u'].map(str) + ' ' + edges_proj['v'].astype(str)

    ### merge the data frame    
    merged_df_1 = edges_proj.merge(speed_lims, how='left', on='A')
    merged_df_2 = edges_proj.merge(speed_lims, how='left', left_on=['A'], right_on=['B'])
    merged_df = merged_df_1.merge(merged_df_2, left_on=['A'], right_on=['A_x'])
    ##

    merged_df['time'] = merged_df['length'] / merged_df['postvz_sl']
    G_adj = ox.save_load.gdfs_to_graph(nodes_df, merged_df)  # we can save this and call this when need it in function 
    pickle.dump(com_Gs, open('adj_G.p', 'wb'))


def get_path_time(G, curr_loc, dest_loc):

    pickle.load(open('adj_G.p', 'rb'))

    orig_node = ox.get_nearest_node(G, curr_loc, method='euclidean')
    target_node = ox.get_nearest_node(G, dest_loc, method='euclidean')
    route = nx.shortest_path(G_adj, source=orig_node, target=target_node, weight='time')
    
    nodes_proj, edges_proj = ox.graph_to_gdfs(G, nodes=True, edges=True)

    pairs = []
    times = []
    for i in len(route):
        pair = (route[i], route[i+1])
        time = edges_proj.loc[(edges_proj['u']==pair[0]) & (edges_proj['v']==pair[1], 'time')]
        pairs.append(pair)
        times.append(time)

    return pairs, times 





    