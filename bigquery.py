# Attempt at connecting to google bigquery dataset
# Abdallah Aboelela
# 3 May, 2019

# Requirement: create an env with your API key first 
# (Can't find the link for this. Try running the code and follow the link from the error)

from google.cloud import bigquery
import matplotlib.pyplot as plt

def map_pickups(limit):
    client = bigquery.Client()
    dataset_table_id = '`bigquery-public-data.new_york.tlc_yellow_trips_2009`'

    sql = '''SELECT pickup_latitude, pickup_longitude 
        FROM ''' + dataset_table_id + ''' 
        LIMIT ''' + str(limit) + ';'

    trips = client.query(sql).to_dataframe()

    trips = trips[trips.pickup_latitude < 100]
    trips = trips[trips.pickup_longitude > -150]
    trips = trips[trips.pickup_longitude < -50]

    trips.plot(kind="scatter", x="pickup_longitude", y="pickup_latitude", alpha=0.1)
    plt.show()

# Useful links:
# Matplotlib maps - http://www.bigendiandata.com/2017-06-27-Mapping_in_Jupyter/