'''
Purpose: 
    Main file to run entire project.
'''
import extract_csv
import os
#import map_ny
import sys
import csv

def run(date1, date2, filename):
    '''
    file = dataframe with which dates are in which files
    date1 = '01-04'
    date2 = '01-10'
    '''

    with open('mr_filter_dates.csv', 'w') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow([date1, date2])

    extract_csv.extract_files(date1, date2)

    #Use this to run locally:
    #os.system('python3 mr_trips.py -r local --conf-path mrjob.conf relevant_csvs/*.csv > output.csv')

    run_code = 'python3 mr_trips.py -r dataproc --num-core-instances 4' +\
        ' --conf-path mrjob.conf relevant_csvs/*.csv > output/' + filename + '.csv'
    os.system(run_code)

    # os.system('python3 mr_trips.py relevant_csvs/*.csv > output.csv')

    #map_ny.map('G_adj.p', 'output.csv')

if __name__ == "__main__":
    dates_to_run_csv = sys.argv[1]
    with open(dates_to_run_csv, newline='') as csvfile:
        csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in csv_reader:
            date1, date2, filename = row
            run(str(date1) + ' 00:00:00', str(date2) + ' 23:59:59', str(filename))
