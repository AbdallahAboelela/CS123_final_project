#!/bin/bash

date1 = '2010-05-05'
date2 = '2010-07-05'

for i in gs://kasa_nyc_taxi_data/converted_data/*.csv; do
    python3 checkfile.py $i date1 date2 >> $date1-$date2.csv
    done
