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
    date1 = datetime.strptime('2020-' + date1, '%Y-%m-%d %H:%M:%S')
    date2 = datetime.strptime('2020-' + date2, '%Y-%m-%d %H:%M:%S')

    dir_name = str(date1).strip(' ')[4:] + '_' + str(date2).strip(' ')[4:] #4 to skip year
    df = pd.read_csv(FILE_NAME, infer_datetime_format=True, parse_dates=['first_trip_dt', 'last_trip_dt'])
 
    df.first_trip_dt = df.first_trip_dt.apply(lambda dt: dt.replace(year = 2020))
    df.last_trip_dt = df.last_trip_dt.apply(lambda dt: dt.replace(year = 2020))
    # , date_parser=date_parse)


    os.system('mkdir relevant_csvs')
    for index, row in df.iterrows():
        if (row['first_trip_dt'] <= date1 <= row['last_trip_dt']) or\
                (row['first_trip_dt'] <= date2 <= row['last_trip_dt']):
            full_name, str_num = get_fname(row['file_num'])

            os.system('gsutil cp {} relevant_csvs/'.format(full_name))
            df = pd.read_csv('relevant_csvs/' + full_name[len(GET_REPO):],\
                infer_datetime_format=True, parse_dates=['pickup_datetime'])
            try:
                if not df:
                    print(df)
            except:
                pass
            df = check_dates_recursion(df, date1, date2, 0, 0, df.shape[0] - 1, df.shape[0] - 1)#check_dates(df, date1, date2)
            df = df.sample(frac=0.0001)
            df.to_csv('relevant_csvs/' + full_name[len(GET_REPO):])

def check_dates_recursion(csv, date1, date2, start, prev_start, end, prev_end):
    first = csv.iloc[start]
    last = csv.iloc[end]
    middle = (start + end) // 2
    
    if (date1 <= first['pickup_datetime'].replace(year=2020) <= date2) and\
            (date1 <= last['pickup_datetime'].replace(year=2020) <= date2) and not\
            (date1 <= csv.iloc[start - 1]['pickup_datetime'].replace(year=2020) <= date2) and not\
            (date1 <= csv.iloc[end + 1]['pickup_datetime'].replace(year=2020) <= date2):
        print(start)
        print(end)
        return csv.iloc[start:end + 1]
        
    if date2 <= csv.iloc[middle]['pickup_datetime'].replace(year=2020):
        check_dates_recursion(csv, date1, date2, start, start, middle, middle)
        
    elif date1 >= csv.iloc[middle]['pickup_datetime'].replace(year=2020):
        check_dates_recursion(csv, date1, date2, middle, middle, end, end)
    
    elif (date1 <= first['pickup_datetime'].replace(year=2020) <= date2) and not\
            (date1 <= csv.iloc[start - 1]['pickup_datetime'].replace(year=2020) <= date2) and\
            (date1 <= last['pickup_datetime'].replace(year=2020) <= date2):
            #First correct, last too small
            new_end = (end + prev_end) // 2
            check_dates_recursion(csv, date1, date2, start, start, new_end, prev_end)

    elif (date1 <= first['pickup_datetime'].replace(year=2020) <= date2) and not\
            (date1 <= csv.iloc[start - 1]['pickup_datetime'].replace(year=2020) <= date2) and not\
            (date1 <= last['pickup_datetime'].replace(year=2020) <= date2):
            #First correct, last too large
            new_end = (end + middle) // 2
            check_dates_recursion(csv, date1, date2, start, start, new_end, end)

    elif (date1 <= last['pickup_datetime'].replace(year=2020) <= date2) and not\
            (date1 <= csv.iloc[end + 1]['pickup_datetime'].replace(year=2020) <= date2) and\
            (date1 <= first['pickup_datetime'].replace(year=2020) <= date2):
            #Last correct, first too large
            new_start = (start + prev_start) // 2
            check_dates_recursion(csv, date1, date2, new_start, prev_start, end, end)

    elif (date1 <= last['pickup_datetime'].replace(year=2020) <= date2) and not\
            (date1 <= csv.iloc[end + 1]['pickup_datetime'].replace(year=2020) <= date2) and not\
            (date1 <= first['pickup_datetime'].replace(year=2020) <= date2):
            #Last correct, first too small
            new_start = (start + middle) // 2
            check_dates_recursion(csv, date1, date2, new_start, prev_start, end, end)
            
    elif (date1 <= first['pickup_datetime'].replace(year=2020) <= date2) and\
        (date1 <= last['pickup_datetime'].replace(year=2020) <= date2):
        new_start = (start + prev_start) // 2
        new_end = (end + prev_end) // 2
        check_dates_recursion(csv, date1, date2, new_start, prev_start, new_end, prev_end)
    else:
        new_start = (start + middle) // 2
        new_end = (end + middle) // 2
        check_dates_recursion(csv, date1, date2, new_start, prev_start, new_end, prev_end)


def check_dates(csv, date1, date2):
    first = csv.iloc[0]
    last = csv.iloc[csv.shape[0] - 1]
    if not (date1 <= first['pickup_datetime'].replace(year=2020) <= date2)\
        and not (date1 <= last['pickup_datetime'].replace(year=2020) <= date2):
        return False
    elif date1 <= first['pickup_datetime'].replace(year=2020)\
            and last['pickup_datetime'].replace(year=2020) <= date2:
        return csv
    else:
        i = csv.shape[0] - 1
        j = 0
        if date1 <= first['pickup_datetime'].replace(year=2020):
            while not last['pickup_datetime'].replace(year=2020) <= date2:
                i -= 1
                last = csv.iloc[i]
        if last['pickup_datetime'].replace(year=2020) <= date2:
            while not first['pickup_datetime'].replace(year=2020) >= date1:
                j += 1
                first = csv.iloc[j]
        return csv.iloc[j:i + 1, :]

def get_fname(num):
    n = len(str(num))

    str_num = (N - n) * "0" + str(num)

    fname = GET_REPO + FILE_PREFIX + str_num + FILE_SUFFIX

    return fname, str_num

