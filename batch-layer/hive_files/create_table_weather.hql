-- Create external table from csv
DROP TABLE IF EXISTS cwbryant_weather_csv;
CREATE EXTERNAL TABLE cwbryant_weather_csv(
StationCode STRING,
StationName STRING,
WeatherDate STRING,
AWND STRING,
PGTM STRING,
PRCP STRING,
SNOW STRING,
SNWD STRING,
TAVG STRING,
TMAX STRING,
TMIN STRING,
WDF2 STRING,
WDF5 STRING,
WSF2 STRING,
WSF5 STRING,
WT01 STRING,
WT02 STRING,
WT03 STRING,
WT08 STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
"separatorChar" = "\,",
"quoteChar"     = "\""
)

STORED AS TEXTFILE location '/cwbryant/project/weather'

TBLPROPERTIES ("skip.header.line.count"="1");

-- Select a few rows to confirm data
SELECT * FROM cwbryant_weather_csv LIMIT 5;

-- Create ORC table from csv hive table
DROP TABLE IF EXISTS cwbryant_weather;
CREATE TABLE cwbryant_weather(
StationCode STRING,
StationName STRING,
WeatherDate STRING,
AWND STRING,
PGTM STRING,
PRCP STRING,
SNOW STRING,
SNWD STRING,
TAVG STRING,
TMAX STRING,
TMIN STRING,
WDF2 STRING,
WDF5 STRING,
WSF2 STRING,
WSF5 STRING,
WT01 STRING,
WT02 STRING,
WT03 STRING,
WT08 STRING)
STORED AS ORC;

-- Copy the CSV table to the ORC table
INSERT OVERWRITE TABLE cwbryant_weather SELECT cwbryant_weather_csv.* FROM cwbryant_weather_csv;

-- Select a few frows to confirm data
SELECT StationName, WeatherDate FROM cwbryant_weather LIMIT 5;
