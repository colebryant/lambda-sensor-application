#!bin/bash
echo "running scala scripts to create view tables"
spark-shell -i scala_files/join_tables.scala
spark-shell -i scala_files/latest_readings.scala
spark-shell -i scala_files/hourly_totals.scala

echo "moving view tables to hbase"
echo -e "create \"cwbryant_latest_readings_view\",\"readings\"" | hbase shell -n
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver -f hive_files/readings_to_hbase.hql
echo -e "create \"cwbryant_hourly_totals_view\",\"totals\"" | hbase shell -n
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver -f hive_files/totals_to_hbase.hql

