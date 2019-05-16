# Map Reduce for Project KASA
# 8 May, 2019

# EXAMPLE COMMAND LINE RUN COMMAND TO OUTPUT INTO FILE test_write.csv
# python3 mr_trips.py duper_short.csv > test_write.csv

# To stream time output into time.txt run
# { time python3 mr_trips.py duper_short.csv > test_write.csv ; } 2> time.txt

# To run the above in dataproc with 8 computer run 

### BOUNDARIES COPIED BELOW

'''
Purpose: Merge polygon shapefiles for New York boroughs, then turn
    into OSMNX Graph
'''
import osmnx as ox
import geopandas as gpd
from shapely.ops import cascaded_union
import networkx as nx
import itertools

from mrjob.job import MRJob
import pandas as pd
import pickle
from datetime import datetime

def construct_G_adj():
    borough_bds = gpd.read_file('borough_bds.geojson')
    ## Source: https://stackoverflow.com/questions/40385782/
    ## make-a-union-of-polygons-in-python-geopandas-or-shapely-into-a-single-geometr
    polygon = gpd.GeoSeries(cascaded_union(borough_bds['geometry']))
    G = ox.graph_from_polygon(polygon[0], network_type='drive')


    speed_lims = gpd.read_file('speed_lims.geojson')
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

    ##
    # 'length' is in meters so need to convert miles to meters
    merged_df['postvz_sl_x'] *= 1609.34 / 60
    print(merged_df.columns)
    merged_df['time'] = merged_df['length'] / merged_df['postvz_sl_x']
    merged_df.drop(['postvz_sl_x'], axis=1, inplace=True)
    G_adj = ox.save_load.gdfs_to_graph(nodes_proj, merged_df)  # we can save this and call this when need it in function 
    pickle.dump(G_adj, open('G_adj.p', 'wb'))


def get_path_time(G, curr_loc, dest_loc):

    orig_node = ox.get_nearest_node(G, curr_loc, method='euclidean')
    target_node = ox.get_nearest_node(G, dest_loc, method='euclidean')
    route = nx.shortest_path(G, source=orig_node, target=target_node, weight='time')
    
    nodes_proj, edges_proj = ox.graph_to_gdfs(G, nodes=True, edges=True)

    pairs = []
    times = []
    for i in range(len(route) - 1):
        pair = (route[i], route[i+1])

        time = edges_proj.loc[(edges_proj['u']==pair[0]) & (edges_proj['v']==pair[1]), 'time']
        
        pairs.append(pair)

        try:
            times.append(float(time))

        except:
            times.append(float(time.iloc[0]))


    return pairs, times

### Actual MR_TRIPS BELOW

class MRNodeTime(MRJob):

    def mapper_init(self):
        
        # self.G = pickle.load(open('/Users/keiirizawa/Desktop/CS123_final_project/dataproc_work/G_adj.p', 'rb'))
        
        self.G = pickle.load(open('/Users/abdallahaboelela/Documents/GitHub/'
            'CS123_final_project/dataproc_work/G_adj.p', 'rb'))

    def mapper(self, _, line):

        l = line.split(',')
        d_lat, d_long = l[2:4]
        p_lat, p_long = l[13:15]

        if not d_lat == "dropoff_latitude":
            
            d_lat = float(d_lat)
            d_long = float(d_long)
            p_lat = float(p_lat)
            p_long = float(p_long)

            d_dt = datetime.strptime(l[1], '%Y-%m-%d %H:%M:%S')
            p_dt = datetime.strptime(l[12], '%Y-%m-%d %H:%M:%S')
            
            paths, times = get_path_time(self.G, (p_lat, p_long), (d_lat, d_long))

            ideal_tot_time = sum(times)
            actual_tot_time = (d_dt - p_dt).seconds / 60

            for i, nodes in enumerate(paths):
                time = times[i]

                yield str((min(nodes), max(nodes))), time/ideal_tot_time * actual_tot_time

    def combiner(self, path, times):
        yield path, sum(times)

    def reducer(self, path, times):
        yield path, round(sum(times), 3)

if __name__ == '__main__':
    MRNodeTime.run()