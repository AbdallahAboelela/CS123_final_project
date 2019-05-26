import pandas as pd
import os
from datetime import datetime

GET_REPO = "gs://kasa_nyc_taxi_data/converted_data/"
FILE_PREFIX = "taxi_data_"
FILE_SUFFIX = ".csv"
N = 5

FILE_NAME = './file_dates.csv'

def extract_files(date1, date2):
    '''
    date1 = '01-04'
    date2 = '01-10'
    '''
    date1 = datetime.strptime('2019-' + date1, '%Y-%m-%d %H:%M:%S')
    date2 = datetime.strptime('2019-' + date2, '%Y-%m-%d %H:%M:%S')

    dir_name = str(date1).strip(' ')[4:] + '_' + str(date2).strip(' ')[4:] #4 to skip year
    df = pd.read_csv(FILE_NAME, infer_datetime_format=True, parse_dates=['first_trip_dt', 'last_trip_dt'])

    df.first_trip_dt = df.first_trip_dt.apply(lambda x: x.year = 2019)
    df.last_trip_dt = df.last_trip_dt.apply(lambda x: x.year = 2019)
    # , date_parser=date_parse)

    for index, row in df.iterrows():
        if (row['first_trip_dt'] <= date1 <= row['last_trip_dt']) or\
                (row['first_trip_dt'] <= date2 <= row['last_trip_dt']):
            full_name, str_num = get_fname(row['file_num'])
            os.system('mkdir {}'.format(dir_name))
            os.system('gsutil cp {} {}/{}'.format(full_name, dir_name, str_num))

def get_fname(num):
    n = len(str(num))

    str_num = (N - n) * "0" + str(num)

    fname = GET_REPO + FILE_PREFIX + str_num + FILE_SUFFIX

    return fname, str_num

