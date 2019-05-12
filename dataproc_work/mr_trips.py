# Map Reduce for Project KASA
# 8 May, 2019


# import PLACEHOLDER FOR KEI AND ADAM
from mrjob.job import MRJob
from google.cloud import bigquery
import pandas as pd

# ASKS Prof WACHS HOW TO READ FROM GOOGLE

def get_trips_df(limit):
    client = bigquery.Client()
    dataset_table_id = '`bigquery-public-data.new_york.tlc_yellow_trips_2009`'

    sql = '''SELECT pickup_latitude as p_lat, pickup_longitude as p_long,
        dropoff_latitude as d_lat, dropoff_longitude as d_long
        FROM ''' + dataset_table_id + ''' 
        LIMIT ''' + str(limit) + ';'

    trips = client.query(sql).to_dataframe()

    return trips

# ASKS Prof WACHS plottig with MRjob (see bigquery.py)
def plot(mrjob_result):
    plt.plot(x, x)
    plt.show()

class MRNodeTime(MRJob):
    def mapper(self, _, line):
        p_lat, p_long, d_lat, d_long = line.split(',')
        paths, times = adam_kei.get(p_lat, p_long, d_lat, d_long)
        
        for i, n1, n2 in enumerate(paths):
            time = times[i]
            yield (min([n1, n2]), max([n1, n2])), time

    def combiner(self, path, times):
        yield path, sum(times)

    def reducer_init(self):
        cols = ['n1', 'n2', 'total_time']
        self.node_times = pd.DataFrame(columns = cols)

    def reducer(self, path, times):
        yield path, sum(times)

if __name__ == '__main__':
    MRNodeTime.run()