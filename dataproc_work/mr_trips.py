# Map Reduce for Project KASA
# 8 May, 2019

# EXAMPLE COMMAND LINE RUN COMMAND TO OUTPUT INTO FILE test_write.csv
# python3 mr_trips.py duper_short.csv > test_write.csv

# To stream time output into time.txt run
# { time python3 mr_trips.py duper_short.csv > test_write.csv ; } 2> time.txt

from mrjob.job import MRJob
import pandas as pd
import pickle
from datetime import datetime
import networkx as nx
import osmnx_code
import util


class MRNodeTime(MRJob):

    def mapper_init(self):
        # self.G = pickle.load(open('/Users/keiirizawa/Desktop/CS123_final_project/dataproc_work/G_adj.p', 'rb'))
        #G_adj_path = '/home/adam_a_oppenheimer/G_adj.p' #os.path.abspath('G_adj.p')
        #G_adj_path = '/Users/adamalexanderoppenheimer/Desktop/CS123_final_project/dataproc_work/G_adj.p'
        #G_adj_path = '/Users/keiirizawa/Desktop/CS123_final_project/dataproc_work/G_adj.p'
        
        G_adj_path = '/home/akaboelela/dataproc_work/G_adj.p'
        G_edges_proj = '/home/akaboelela/dataproc_work/G_edges_proj.p'
        dates = pd.read_csv('/home/akaboelela/dataproc_work/mr_filter_dates.csv', header = None)
        
        self.G = pickle.load(open(G_adj_path, 'rb'))
        self.edges_proj = pickle.load(open(G_edges_proj, 'rb'))
        self.start = datetime.strptime('2020-' + dates.iloc[0, 0], '%Y-%m-%d %H:%M:%S')
        self.end = datetime.strptime('2020-' + dates.iloc[0, 1], '%Y-%m-%d %H:%M:%S')

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

                if self.start <= p_dt.replace(year=2019) <= self.end:

                    paths, times = util.get_path_time(self.G, self.edges_proj, (p_lat, p_long), (d_lat, d_long))
                    #formerly boundaries.get_path_time

                    ideal_tot_time = sum(times)
                    actual_tot_time = (d_dt - p_dt).seconds / 60

                    for nodes in paths:
                        yield 'y{}, {}, {}, {}'.format(p_dt.year, min(nodes), max_nodes, util.get_time_of_day(p_dt)), actual_tot_time / ideal_tot_time

            except:
                pass

    def combiner(self, path_year, times):
        yield path_year, sum(times)

    def reducer(self, path_year, times):
        yield path_year, round(sum(times), 3)


if __name__ == '__main__':
    MRNodeTime.run()

