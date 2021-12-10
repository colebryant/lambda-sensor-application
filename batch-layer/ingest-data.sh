#!/bin/bash
echo "ingesting sensor and weather data to hdfs"
hdfs dfs -mkdir /cwbryant/project
hdfs dfs -mkdir /cwbryant/project/sensor
hdfs dfs -mkdir /cwbryant/project/weather

hdfs dfs -put all_sensor_data.csv /cwbryant/project/sensor
hdfs dfs -put 2811011.csv /cwbryant/project/weather

hdfs dfs -ls /cwbryant/project/sensor
hdfs dfs -ls /cwbryant/project/weather

echo "cleaning up local data files"
rm all_sensor_data.csv
rm 2811011.csv