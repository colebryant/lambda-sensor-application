-- Move the table from Hive to HBase
DROP TABLE IF EXISTS cwbryant_latest_readings_ext;
CREATE EXTERNAL TABLE cwbryant_latest_readings_ext (
    SensorID STRING,
    SensorDescription STRING,
    Latitude STRING,
    Longitude STRING,
    ReadingTimestamp STRING,
    WaterLevel DOUBLE,
    FilteredWaterLevel DOUBLE,
    ReadingDate STRING,
    ReadingHour STRING,
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
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 
':key,
readings:SensorDescription,
readings:Latitude,
readings:Longitude,
readings:ReadingTimestamp,
readings:WaterLevel,
readings:FilteredWaterLevel,
readings:ReadingDate,
readings:ReadingHour,
readings:AWND,
readings:PGTM,
readings:PRCP,
readings:SNOW,
readings:SNWD,
readings:TAVG,
readings:TMAX,
readings:TMIN,
readings:WDF2,
readings:WDF5,
readings:WSF2,
readings:WSF5,
readings:WT01,
readings:WT02,
readings:WT03,
readings:WT08')
TBLPROPERTIES ('hbase.table.name' = 'cwbryant_latest_readings_view');

INSERT OVERWRITE TABLE cwbryant_latest_readings_ext
SELECT SensorID,
       SensorDescription,
       Latitude,
       Longitude,
       ReadingTimestamp,
       WaterLevel,
       FilteredWaterLevel,
       ReadingDate,
       ReadingHour,
       AWND,
       PGTM,
       PRCP,
       SNOW,
       SNWD,
       TAVG,
       TMAX,
       TMIN,
       WDF2,
       WDF5,
       WSF2,
       WSF5,
       WT01,
       WT02,
       WT03,
       WT08
FROM cwbryant_latest_readings;
