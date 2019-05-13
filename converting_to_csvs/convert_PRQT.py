# Connecting to gcs bucket and converting everything to a csv

import os
import pandas as pd
import sys

GET_REPO = "gs://kasa_nyc_taxi_data/NYC_taxi_2009-2016.parquet/"
FILE_PREFIX = "part-r-"
FILE_SUFFIX = "-ec9cbb65-519d-4bdb-a918-72e2364c144c.snappy.parquet"
UPLOAD_REPO = "gs://kasa_nyc_taxi_data/NYC_taxi_2009-2016.csv/"
N = 5

def convert_file(num):
    fname, str_num = get_fname(num)

    os.system("gsutil cp " + fname + " ./")

    old_name = fname[len(GET_REPO):]

    df = pd.read_parquet(old_name)

    new_name = "taxi_data_" + str_num + ".csv"

    df.to_csv(new_name)

    return new_name, old_name

def get_fname(num):
    n = len(str(num))

    str_num = (N - n) * "0" + str(num)

    fname = GET_REPO + FILE_PREFIX + str_num + FILE_SUFFIX

    return fname, str_num

if __name__ == "__main__":
    num = int(sys.argv[1])

    while num < 800:
        new_name, old_name = convert_file(num)

        print("uploading")

        os.system("gsutil mv " + new_name + 
            " gs://kasa_nyc_taxi_data/converted_data/" + 
            new_name)

        os.system("rm " + old_name)

        num += 1


