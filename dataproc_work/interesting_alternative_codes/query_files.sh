#!/bin/bash

date1=2010-01-04
date2=2010-01-10

for i in `seq 100 799`; do
    file=gs://kasa_nyc_taxi_data/converted_data/taxi_data_00$i.csv
    python3 checkfile.py $file $date1 $date2 >> ${date1}-${date2}.csv
    echo 'finished $i'
    done