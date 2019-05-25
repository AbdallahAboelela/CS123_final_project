# Connecting to gcs bucket and getting first and last line of each
# file

import os
import pandas as pd
import sys
from datetime import datetime
import csv

GET_REPO = "gs://kasa_nyc_taxi_data/converted_data/"
FILE_PREFIX = "taxi_data_"
FILE_SUFFIX = ".csv"
N = 5

def get_dates(num):
    full_name, str_num = get_fname(num)
    file_name = full_name[len(GET_REPO):]

    os.system("gsutil cp " + full_name + " ./")

    df = pd.read_csv(file_name).drop("Unnamed: 0", axis = 1)

    first_trip_dt = datetime.strptime(df.iloc[0]["pickup_datetime"], '%Y-%m-%d %H:%M:%S')
    last_trip_dt = datetime.strptime(df.iloc[-1]["pickup_datetime"], '%Y-%m-%d %H:%M:%S')

    return file_name, first_trip_dt, last_trip_dt

def get_fname(num):
    n = len(str(num))

    str_num = (N - n) * "0" + str(num)

    fname = GET_REPO + FILE_PREFIX + str_num + FILE_SUFFIX

    return fname, str_num


if __name__ == "__main__":
    num = int(sys.argv[1])

    with open("file_dates.csv", "w") as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        writer.writerow(['file_num', 'first_trip_dt', 'last_trip_dt'])
        
        while num < 2:
            file_name, first_trip_dt, last_trip_dt = get_dates(num)

            writer.writerow([num, first_trip_dt, last_trip_dt])

            os.system("rm " + file_name)

            num += 1

