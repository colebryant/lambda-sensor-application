#!/bin/bash
echo "creating hive/orc table for sensor data"
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver -f hive_files/create_table_sensor.hql

echo "creating hive/orc table for weather data"
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver -f hive_files/create_table_weather.hql