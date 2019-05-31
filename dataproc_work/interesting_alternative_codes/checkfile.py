import sys
import pandas as pd
from datetime import datetime
import gcsfs

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
            

if __name__ == "__main__":
    NUM_ARGS = len(sys.argv)

    if NUM_ARGS != 4:
        print("usage: python3 " + sys.argv[0] + " <file name for taxi data> " +
              "<start date>\n  <end date> ")
        sys.exit(0)
        
    csv = pd.read_csv(sys.argv[1], parse_dates=['pickup_datetime'], infer_datetime_format=True)

    start = datetime.strptime(sys.argv[2], '%Y-%m-%d')

    end = datetime.strptime(sys.argv[3], '%Y-%m-%d')

    results = check_dates(csv, start, end)

    print(results)
