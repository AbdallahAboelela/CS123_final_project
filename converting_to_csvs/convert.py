# Connecting to bigquery and saving everything as a csv
# Requirement: create an env with your API key first 

from google.cloud import bigquery
import os

def convert_to_csv():
    client = bigquery.Client()

    dataset_prefix = '`bigquery-public-data.new_york.tlc_yellow_trips_'
    year = 2009

    while year <= 2016:
        sql = 'SELECT * FROM ' + dataset_prefix + str(year) + '` LIMIT 1000;'

        trips = client.query(sql).to_dataframe()

        fname = str(year) + '_trips.csv'

        trips.to_csv(fname)

        os.system("gsutil cp " + "./" + fname + " gs://kasa_nyc_taxi_data/NYC_taxi_2009-2016.csv/")
        os.system("rm ./" + fname)
        
        year += 1

if __name__ == "__main__":
    convert_to_csv()