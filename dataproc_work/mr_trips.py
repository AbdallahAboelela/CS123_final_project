# Map Reduce for Project KASA
# 8 May, 2019

# EXAMPLE COMMAND LINE RUN COMMAND TO OUTPUT INTO FILE test_write.csv
# python3 mr_trips.py duper_short.csv > test_write.csv

# To stream time output into time.txt run
# { time python3 mr_trips.py duper_short.csv > test_write.csv ; } 2> time.txt

import os
# os.system("sudo pip3 install --upgrade pip")
# #os.system("sudo -H pip3 install wheel")
# #os.system("sudo -H pip3 install pandas")
# os.system("sudo pip3 install mrjob")
# os.system("xcode-select --install")
# os.system('/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"')
# os.system("brew install spatialindex")
# #os.system("pip3 uninstall osmnx")
# os.system("sudo pip3 install osmnx")
# os.system("sudo apt install python3-rtree")
# #os.system("pip3 uninstall networkx")
# os.system("sudo pip3 install networkx")

from mrjob.job import MRJob
import numpy as np
import pandas as pd
import pickle
from datetime import datetime
#import boundaries
#import osmnx as ox
import networkx as nx
import time


def get_path_time(G, curr_loc, dest_loc):
    orig_node = get_nearest_node(G, curr_loc, method='euclidean')
    target_node = get_nearest_node(G, dest_loc, method='euclidean')
    route = nx.shortest_path(G, source=orig_node, target=target_node, weight='time')
    
    #nodes_proj, edges_proj = ox.graph_to_gdfs(G, nodes=True, edges=True)

    pairs = []
    #times = []
    for i in range(len(route) - 1):
        pair = (route[i], route[i+1])

        #time = edges_proj.loc[(edges_proj['u']==pair[0]) & (edges_proj['v']==pair[1]), 'time']
        
        pairs.append(pair)

        #try:
        #    times.append(float(time))

        #except:
        #    times.append(float(time.iloc[0]))


    return pairs#, times

class MRNodeTime(MRJob):

    def mapper_init(self):
        # self.G = pickle.load(open('/Users/keiirizawa/Desktop/CS123_final_project/dataproc_work/G_adj.p', 'rb'))
        #G_adj_path = '/home/adam_a_oppenheimer/G_adj.p' #os.path.abspath('G_adj.p')
        #G_adj_path = '/Users/adamalexanderoppenheimer/Desktop/CS123_final_project/dataproc_work/G_adj.p'
        #G_adj_path = '/Users/keiirizawa/Desktop/CS123_final_project/dataproc_work/G_adj.p'
        G_adj_path = 'G_adj.p'
        self.G = pickle.load(open(G_adj_path, 'rb'))

        #self.G = pickle.load(open('/Users/abdallahaboelela/Documents/GitHub/'
        #    'CS123_final_project/dataproc_work/G_adj.p', 'rb'))

    def mapper(self, _, line):

        l = line.split(',')
        d_lat, d_long = l[2:4]
        p_lat, p_long = l[13:15]

        if not d_lat == "dropoff_latitude":
            
            try:
                d_lat = float(d_lat)
                d_long = float(d_long)
                p_lat = float(p_lat)
                p_long = float(p_long)

                d_dt = datetime.strptime(l[1], '%Y-%m-%d %H:%M:%S')
                p_dt = datetime.strptime(l[12], '%Y-%m-%d %H:%M:%S')

                paths = get_path_time(self.G, (p_lat, p_long), (d_lat, d_long))
                #formerly boundaries.get_path_time

                #ideal_tot_time = sum(times)
                #actual_tot_time = (d_dt - p_dt).seconds / 60

                for nodes in paths:
                    yield str((min(nodes), max(nodes))), 1
                    # This line needs to be changed to a proportion instead of time measure
                    # We want delay not just streets that are driven on a lot
            except:
                print('Failure. Diagnose later')
                print('d_lat: ', d_lat)
                print('d_long: ', d_long)
                print('p_lat: ', p_lat)
                print('p_long: ', p_long)
                pass

    def combiner(self, path, times):
        yield path, sum(times)

    def reducer(self, path, times):
        yield path, round(sum(times), 3)

def get_nearest_node(G, point, method='haversine', return_dist=False):
    """
    Return the graph node nearest to some specified (lat, lng) or (y, x) point,
    and optionally the distance between the node and the point. This function
    can use either a haversine or euclidean distance calculator.
    Parameters
    ----------
    G : networkx multidigraph
    point : tuple
        The (lat, lng) or (y, x) point for which we will find the nearest node
        in the graph
    method : str {'haversine', 'euclidean'}
        Which method to use for calculating distances to find nearest node.
        If 'haversine', graph nodes' coordinates must be in units of decimal
        degrees. If 'euclidean', graph nodes' coordinates must be projected.
    return_dist : bool
        Optionally also return the distance (in meters if haversine, or graph
        node coordinate units if euclidean) between the point and the nearest
        node.
    Returns
    -------
    int or tuple of (int, float)
        Nearest node ID or optionally a tuple of (node ID, dist), where dist is
        the distance (in meters if haversine, or graph node coordinate units
        if euclidean) between the point and nearest node
    """
    start_time = time.time()

    if not G or (G.number_of_nodes() == 0):
        raise ValueError('G argument must be not be empty or should contain at least one node')

    # dump graph node coordinates into a pandas dataframe indexed by node id
    # with x and y columns
    coords = [[node, data['x'], data['y']] for node, data in G.nodes(data=True)]
    df = pd.DataFrame(coords, columns=['node', 'x', 'y']).set_index('node')

    # add columns to the dataframe representing the (constant) coordinates of
    # the reference point
    df['reference_y'] = point[0]
    df['reference_x'] = point[1]

    # calculate the distance between each node and the reference point
    if method == 'haversine':
        # calculate distance vector using haversine (ie, for
        # spherical lat-long geometries)
        distances = great_circle_vec(lat1=df['reference_y'],
                                     lng1=df['reference_x'],
                                     lat2=df['y'],
                                     lng2=df['x'])

    elif method == 'euclidean':
        # calculate distance vector using euclidean distances (ie, for projected
        # planar geometries)
        distances = euclidean_dist_vec(y1=df['reference_y'],
                                       x1=df['reference_x'],
                                       y2=df['y'],
                                       x2=df['x'])

    else:
        raise ValueError('method argument must be either "haversine" or "euclidean"')

    # nearest node's ID is the index label of the minimum distance
    nearest_node = distances.idxmin()
    # log('Found nearest node ({}) to point {} in {:,.2f} seconds'.format(nearest_node, point, time.time()-start_time))

    # if caller requested return_dist, return distance between the point and the
    # nearest node as well
    if return_dist:
        return nearest_node, distances.loc[nearest_node]
    else:
        return nearest_node

def great_circle_vec(lat1, lng1, lat2, lng2, earth_radius=6371009):
    """
    Vectorized function to calculate the great-circle distance between two
    points or between vectors of points, using haversine.
    Parameters
    ----------
    lat1 : float or array of float
    lng1 : float or array of float
    lat2 : float or array of float
    lng2 : float or array of float
    earth_radius : numeric
        radius of earth in units in which distance will be returned (default is
        meters)
    Returns
    -------
    distance : float or vector of floats
        distance or vector of distances from (lat1, lng1) to (lat2, lng2) in
        units of earth_radius
    """

    phi1 = np.deg2rad(lat1)
    phi2 = np.deg2rad(lat2)
    d_phi = phi2 - phi1

    theta1 = np.deg2rad(lng1)
    theta2 = np.deg2rad(lng2)
    d_theta = theta2 - theta1

    h = np.sin(d_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(d_theta / 2) ** 2
    h = np.minimum(1.0, h) # protect against floating point errors

    arc = 2 * np.arcsin(np.sqrt(h))

    # return distance in units of earth_radius
    distance = arc * earth_radius
    return distance

def euclidean_dist_vec(y1, x1, y2, x2):
    """
    Vectorized function to calculate the euclidean distance between two points
    or between vectors of points.
    Parameters
    ----------
    y1 : float or array of float
    x1 : float or array of float
    y2 : float or array of float
    x2 : float or array of float
    Returns
    -------
    distance : float or array of float
        distance or vector of distances from (x1, y1) to (x2, y2) in graph units
    """

    # euclid's formula
    distance = ((x1 - x2) ** 2 + (y1 - y2) ** 2) ** 0.5
    return distance


if __name__ == '__main__':
    MRNodeTime.run()

