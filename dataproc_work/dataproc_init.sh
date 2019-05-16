#!/bin/bash

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

python get-pip.py

pip3 install mrjob
pip3 install geopandas
pip3 install osmnx