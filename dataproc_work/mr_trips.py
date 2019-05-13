# Map Reduce for Project KASA
# 8 May, 2019

from mrjob.job import MRJob
from google.cloud import bigquery
import pandas as pd
import boundaries 
import pickle

class MRNodeTime(MRJob):
    def mapper_init(self):
        self.G = pickle.load(open('G_adj.p', 'rb'))

    def mapper(self, _, line):
        l = line.split(',')
        d_lat, d_long = l[2:4]
        p_lat, p_long = l[13:15]

        paths, times = boundaries.get_path_time(self.G, (p_lat, p_long), (d_lat, d_long))
        
        for i, nodes in enumerate(paths):
            time = times[i]
            yield (min(nodes), max(nodes)), time

    def combiner(self, path, times):
        yield path, sum(times)

    def reducer_init(self):
        cols = ['n1', 'n2', 'total_time']
        self.node_times = pd.DataFrame(columns = cols)

    def reducer(self, path, times):
        yield path, sum(times)

if __name__ == '__main__':
    MRNodeTime.run()