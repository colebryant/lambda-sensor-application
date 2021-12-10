import org.apache.spark.sql.SaveMode

var sensor = spark.table("cwbryant_sensor")

var weather = spark.table("cwbryant_weather")

// Join sensor and weather tables
var sensor_and_weather = sensor.join(weather, sensor("ReadingDate") <=> weather("WeatherDate")).
    select(sensor("SensorDescription"),
           sensor("Latitude"),
           sensor("Longitude"),
           sensor("ReadingTimestamp"),
           sensor("WaterLevel"),
           sensor("FilteredWaterLevel"),
           sensor("SensorID"),
           sensor("ReadingDate"),
           sensor("ReadingHour"),
           weather("AWND"),
           weather("PGTM"),
           weather("PRCP"),
           weather("SNOW"),
           weather("SNWD"),
           weather("TAVG"),
           weather("TMAX"),
           weather("TMIN"),
           weather("WDF2"),
           weather("WDF5"),
           weather("WSF2"),
           weather("WSF5"),
           weather("WT01"),
           weather("WT02"),
           weather("WT03"),
           weather("WT08"))

sensor_and_weather.write.mode(SaveMode.Overwrite).saveAsTable("cwbryant_sensor_and_weather")
System.exit(0)



