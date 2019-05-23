# Map Reduce for Project KASA
# 8 May, 2019

# EXAMPLE COMMAND LINE RUN COMMAND TO OUTPUT INTO FILE test_write.csv
# python3 mr_trips.py duper_short.csv > test_write.csv

# To stream time output into time.txt run
# { time python3 mr_trips.py duper_short.csv > test_write.csv ; } 2> time.txt

import os
print(os.listdir())
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
#import pandas as pd
import pickle
from datetime import datetime
#import boundaries
import osmnx as ox
import networkx as nx


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

class MRNodeTime(MRJob):

    def mapper_init(self):
        # self.G = pickle.load(open('/Users/keiirizawa/Desktop/CS123_final_project/dataproc_work/G_adj.p', 'rb'))
        G_adj_path = '/home/adam_a_oppenheimer/G_adj.p' #os.path.abspath('G_adj.p')
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

                paths, times = get_path_time(self.G, (p_lat, p_long), (d_lat, d_long))
                #formerly boundaries.get_path_time

                ideal_tot_time = sum(times)
                actual_tot_time = (d_dt - p_dt).seconds / 60

                for i, nodes in enumerate(paths):
                    yield str((min(nodes), max(nodes))), actual_tot_time/ideal_tot_time

            except:
                pass

    def combiner(self, path, times):
        yield path, sum(times)

    def reducer(self, path, times):
        yield path, round(sum(times), 3)

if __name__ == '__main__':
    MRNodeTime.run()
