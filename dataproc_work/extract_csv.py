import pandas as pd
import os

GET_REPO = "gs://kasa_nyc_taxi_data/converted_data/"
FILE_PREFIX = "taxi_data_"
FILE_SUFFIX = ".csv"
N = 5

def extract_files(file, date1, date2):
    '''
    '''
    dir_name = str(date1).strip(' ') + '_' + str(date2).strip(' ')
    date_parse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date().replace(year=2019)
    df = pd.read_csv(file, parse_dates=['first_trip_dt', 'last_trip_dt'], date_parser=date_parse)

    for index, row in df.iterrows():
        if date1 >= row['first_trip_dt'] or date2 <= row['last_trip_dt']:
            full_name, _ = get_fname(row['file_num'])
            os.system('gsutil cp {} ./relevant_csvs/{}'.format(full_name, dir_name))
    

def get_fname(num):
    n = len(str(num))

    str_num = (N - n) * "0" + str(num)

    fname = GET_REPO + FILE_PREFIX + str_num + FILE_SUFFIX

    return fname, str_num

