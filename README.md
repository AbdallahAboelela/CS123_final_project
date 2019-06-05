README
=========================== 

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

## osmnx_work Folder

### (1) create_network.py 
Constructs New York Street Netwrok geodataframe.

