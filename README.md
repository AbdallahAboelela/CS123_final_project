# CS123 final project

## Done
1. Adjusted osmnx to give shortest paths in terms of time.  
    See files XXXXX for how this was done and see Notes below for how to access  
    
2. Converting parquet files in GCS bucket to csv (currently running on 4 VM instances - 13 May, 2019)  
    See file convert_PRQT.py on how this was done and see Notes below for how to access  
    convert_BQ.py was a failed attempt at doing the same thing through BigQuery  

## To do
1. Write file to create csv with paths and total times using MRjob given csv file or files 
2. Write file to plot paths given csv output from MRjob  

## Notes
To use our adjusted osmnx, see file: XXXXX  

To pull files from GCS bucket, use gsutil command  
    > gsutil cp gs://kasa_nyc_taxi_data/converted_data/\[FILENAME] \[DEST_DIRECTORY]  
    For more, see: https://cloud.google.com/storage/docs/gsutil  

### Note for Kei (how to use MRJOB):
(1) export GOOGLE_APPLICATION_CREDENTIALS=/Users/keiirizawa/Desktop/kei_api_key.json
(1) export GOOGLE_APPLICATION_CREDENTIALS=/Users/adamalexanderoppenheimer/Desktop/new_google_api_key.json
(2) time python3 mr_trips.py -r dataproc --num-core-instances 4 --conf-path /Users/adamalexanderoppenheimer/Desktop/CS123_final_project/dataproc_work/mrjob.conf csvs/first_hundred.csv


    - sudo pip3 install --upgrade pip
    - sudo pip3 install mrjob
    - xcode-select --install
    - /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
    - brew install spatialindex
    - sudo pip3 install osmnx
    - sudo apt-get install python3-rtree
    - sudo pip3 install networkx