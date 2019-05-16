# Map Reduce for Project KASA
# 8 May, 2019

# EXAMPLE COMMAND LINE RUN COMMAND TO OUTPUT INTO FILE test_write.csv
# python3 mr_trips.py duper_short.csv > test_write.csv

# To stream time output into time.txt run
# { time python3 mr_trips.py duper_short.csv > test_write.csv ; } 2> time.txt

import os
from mrjob.job import MRJob
import pandas as pd
import pickle
from datetime import datetime

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

            try:
                paths, times = get_path_time(self.G, (p_lat, p_long), (d_lat, d_long))

                ideal_tot_time = sum(times)
                actual_tot_time = (d_dt - p_dt).seconds / 60

                for i, nodes in enumerate(paths):
                    time = times[i]

                    yield str((min(nodes), max(nodes))), time/ideal_tot_time * actual_tot_time

            except:
                pass

    def combiner(self, path, times):
        yield path, sum(times)

    def reducer(self, path, times):
        yield path, round(sum(times), 3)

if __name__ == '__main__':
    MRNodeTime.run()