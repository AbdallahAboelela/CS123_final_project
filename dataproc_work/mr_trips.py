# Map Reduce for Project KASA
# 8 May, 2019

from mrjob.job import MRJob
from google.cloud import bigquery
import pandas as pd
import boundaries 
import pickle

TITLES = ["dropoff_latitude", "dropoff_longitude", "pickup_latitude", "pickup_longitude"]


class MRNodeTime(MRJob):
    def mapper_init(self):
        self.G = pickle.load(open('/Users/abdallahaboelela/Documents/GitHub/'
            'CS123_final_project/dataproc_work/G_adj.p', 'rb'))

    def mapper(self, _, line):

        l = line.split(',')
        d_lat, d_long = l[2:4]
        p_lat, p_long = l[13:15]

        hi = [p_lat, p_long, d_lat, d_long]

        if not (d_lat == TITLES[0] or d_long == TITLES[1] or p_lat == TITLES[2] or
            p_long == TITLES[3]) and not "0.0" in hi:

            paths, times = boundaries.get_path_time(self.G, (p_lat, p_long), (d_lat, d_long))

            for i, nodes in enumerate(paths):
                time = times[i]
                yield (min(nodes), max(nodes)), time

    def combiner(self, path, times):
        yield path, sum(times)

    def reducer(self, path, times):
        yield path, sum(times)

if __name__ == '__main__':
    MRNodeTime.run()