#!/bin/bash

curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py

python get-pip.py -y

sudo pip3 install mrjob -y
sudo pip3 install geopandas -y
sudo pip3 install osmnx -y