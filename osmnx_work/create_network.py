'''
Purpose:
    Construct Geodataframe of New York Street Network. 
'''
import osmnx as ox
import geopandas as gpd
from shapely.ops import cascaded_union
import networkx as nx
import itertools
import pandas as pd
import pickle

def construct_G_adj():
    '''
    Purpose: 
        Construct Geodataframe of New York Street Network. 
        It goes through two steps to accomplish this task:
        Step 1: Merge polygon shapefiles for New York boroughs, so that 
        we get boundary of New York. 
        Step 2: Incorporate speed limit data to our network. We create new 
        column with information about how long it would take a car to go 
        through a street given speed limit. This column will let our 
        Dijkstra's algorithm be based on these times rather then length of 
        street. 

    Returns:
        None. It creates 2 pickle files:
        'G_adj.p': geodataframe of New York Street Network
        'G_edges_proj.p': geodataframe of New York Edges Network
    '''
    borough_bds = gpd.read_file('data/borough_bds.geojson')
    ## Following lines of code motivated by:
    ## https://stackoverflow.com/questions/40385782/make-a-union-of-polygons-
    ## in-python-geopandas-or-shapely-into-a-single-geometr
    polygon = gpd.GeoSeries(cascaded_union(borough_bds['geometry']))
    G = ox.graph_from_polygon(polygon[0], network_type='drive')

    speed_lims = gpd.read_file('data/speed_lims.geojson')
    speed_lims.drop(['shape_leng', 'postvz_sg', 'street'], axis=1, inplace=True)
    speed_lims['postvz_sl'] = speed_lims['postvz_sl'].astype(int)
    speed_lims['postvz_sl'][speed_lims['postvz_sl'] == 0] = 25

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
    merged_df_1 = edges_proj.merge(speed_lims[['postvz_sl', 'A']], how='left', on=['A'])
    merged_df_2 = edges_proj.merge(speed_lims[['postvz_sl', 'B']], how='left', left_on=['A'], right_on=['B'])
    
    merged_df = merged_df_1.merge(merged_df_2[['postvz_sl', 'A']], on=['A'])
    merged_df.drop('A', axis=1, inplace=True)

    nan_speed = pd.isnull(merged_df['postvz_sl_x'])
    merged_df['postvz_sl_x'][nan_speed] = merged_df['postvz_sl_y'][nan_speed]
    merged_df.drop('postvz_sl_y', axis=1, inplace=True)
    nan_speed_2 = pd.isnull(merged_df['postvz_sl_x'])
    merged_df['postvz_sl_x'][nan_speed_2] = 25

    # 'length' is in meters so need to convert miles to meters
    merged_df['postvz_sl_x'] *= 1609.34 / 60
    print(merged_df.columns)
    merged_df['time'] = merged_df['length'] / merged_df['postvz_sl_x']
    merged_df.drop(['postvz_sl_x'], axis=1, inplace=True)
    G_adj = ox.save_load.gdfs_to_graph(nodes_proj, merged_df)  # we can save this and call this when need it in function 
    pickle.dump(G_adj, open('../dataproc_work/G_adj.p', 'wb'))
    pickle.dump(merged_df, open('../dataproc_work/G_edges_proj.p', 'wb'))







