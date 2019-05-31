README
=========================== 

## Libraries Used


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
(2) time python3 mr_trips.py -r dataproc --num-core-instances 4 --conf-path /Users/keiirizawa/Desktop/CS123_final_project/dataproc_work/mrjob.conf csvs/first_hundred.csv > dataproc_output.csv
(2) time python3 mr_trips.py -r dataproc --num-core-instances 4 --conf-path /Users/adamalexanderoppenheimer/Desktop/CS123_final_project/dataproc_work/mrjob.conf csvs/first_hundred.csv > dataproc_output.csv

## Explanation of our Codes


## dataproc_work Folder

### (1) Main File

#### run_project.py
Main file to run entire project.

### (2) MapReduce Files

Files used to run MapReduce. 

#### mr_trip.py
Yields key value paors that we use to generate traffice level maps of New York. 

**The following are files called by mr_trip.py:**

#### util.py
Util file for MapReduce (mr_trip.py). Contains helper functions.

#### osmnx_code.py
Contains code from OSMNX library that we use for MapReduce. Had to copy functions we use because having trouble importing OSMNX library in MapReduce using dataproc. 

### (3) Extracting Files from Google Cloud Bucket 

#### extract_csv.py 
Given interested dates want to analyze in main file (run_project.py), extract relevant csv files in google cloud bucket (csv files that include interested dates). 


### (4) Connecting to SSH

#### ssh_connect.py

### (5) Dataproc 

#### dataproc_init.sh

### (6) Traffic Level Map 

#### map_ny.py 


















