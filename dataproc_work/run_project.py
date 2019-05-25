import extract_csv
import os
import map_ny

def run(file, date1, date2):
    '''
    file = dataframe with which dates are in which files
    date1 = '01-04'
    date2 = '01-10'
    '''
    dir_name = str(date1).strip(' ')[4:] + '_' + str(date2).strip(' ')[4:] 
    extract_csv.extract_files(file, date1, date2)

    os.system('python3 mr_trips.py -r dataproc --num-core-instances 4'
        ' --conf-path mrjob.conf dataproc_work/relevant_csvs/{} > output.csv'.format(dir_name)

    map_ny.map(G_FNAME, output.csv)