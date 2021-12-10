#!/bin/bash
echo "getting sensor data csv"
wget https://www.dropbox.com/s/a3qivjdpc30aqg1/all_sensor_data.csv
# Please note, weather data csv 2811011.csv was downloaded locally from 
# https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00003822/detail
# With dates starting June 1, 2019 to present and then scp'd to this directory in the cluster.
# Included this data since csv is so small.
