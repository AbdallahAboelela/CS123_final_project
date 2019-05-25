import sys
import pandas as pd
from datetime import datetime

def check_dates(csv, date1, date2):
    first = csv.iloc[0]
    last = csv.iloc(csv.shape[0])
    if not (date1 < first['pickup_datetime'] < date2) and not (date1 < last['pickup_datetime'] < date2):
        return False
    elif date1 < first['pickup_datetime'] and last['pickup_datetime'] < date2:
        return csv
    elif date1 < first['pickup_datetime']:
        i = csv.shape[0]
        while not last['pickup_datetime'] < date2:
            i -= 1
            last = csv.iloc(i)
        return csv.iloc(0:i + 1)
    elif last['pickup_datetime'] < date2:
        i = 0
        while not first['pickup_datetime'] > date1:
            i += 1
            first = csv.iloc(i)
        return csv.iloc(i:)
            

if __name__ == "__main__":
    NUM_ARGS = len(sys.argv)

    if NUM_ARGS != 3:
        print("usage: python3 " + sys.argv[0] + " <file name for taxi data> " +
              "<start date>\n  <end date> ")
        sys.exit(0)
        
    csv = pd.read_csv(sys.argv[1], parse_dates=['pickup_datetime'], infer_datetime_format=True)

    start = datetime.strptime(sys.argv[2], '%Y-%m-%d')

    end = datetime.strptime(sys.argv[3], '%Y-%m-%d')

    results = check_dates(csv, start, end)

    if results:
        return results
    else:
        return None
