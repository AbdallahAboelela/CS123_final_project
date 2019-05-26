import extract_csv
import os
#import map_ny
import sys
import csv

def run(date1, date2):
    '''
    file = dataframe with which dates are in which files
    date1 = '01-04'
    date2 = '01-10'
    '''
    dir_name = str(date1).strip(' ') + '_' + str(date2).strip(' ')

    with open('mr_filter_dates.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow([date1, date2])

    # extract_csv.extract_files(date1, date2)

    os.system('python3 mr_trips.py -r dataproc --num-core-instances 4'
        ' --conf-path mrjob.conf relevant_csvs/*.csv > output.csv')

    #map_ny.map('G_adj.p', 'output.csv')

if __name__ == "__main__":
    date1 = sys.argv[1]
    date2 = sys.argv[2]
    run(date1 + ' 00:00:00', date2 + ' 00:00:00')