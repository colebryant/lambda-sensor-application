-- Create external table from csv
DROP TABLE IF EXISTS cwbryant_sensor_csv;
CREATE EXTERNAL TABLE cwbryant_sensor_csv(
RowNum STRING,
SensorDescription STRING,
Latitude DOUBLE,
Longitude DOUBLE,
ReadingTimestamp STRING,
WaterLevel DOUBLE,
FilteredWaterLevel DOUBLE,
SensorID STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
"separatorChar" = "\,",
"quoteChar"     = "\""
)

STORED AS TEXTFILE location '/cwbryant/project/sensor'

TBLPROPERTIES ("skip.header.line.count"="1");

-- Select a few rows to confirm data
SELECT * FROM cwbryant_sensor_csv LIMIT 5;

-- Create ORC table from csv hive table. Include new field containing only date
DROP TABLE IF EXISTS cwbryant_sensor;
CREATE TABLE cwbryant_sensor(
RowNum STRING,
SensorDescription STRING,
Latitude DOUBLE,
Longitude DOUBLE,
ReadingTimestamp STRING,
WaterLevel DOUBLE,
FilteredWaterLevel DOUBLE,
SensorID STRING,
ReadingDate STRING,
ReadingHour STRING)
STORED AS ORC;

-- Parse date from timestamp
INSERT OVERWRITE TABLE cwbryant_sensor
SELECT *
FROM (SELECT *, 
      SUBSTR(cwbryant_sensor_csv.ReadingTimestamp, 1, 10) AS readingdate,
      SUBSTR(cwbryant_sensor_csv.ReadingTimestamp, 12, 2) AS readinghour FROM cwbryant_sensor_csv) t1;

-- Select a few rows to confirm data
SELECT SensorDescription, ReadingDate FROM cwbryant_sensor limit 5;
