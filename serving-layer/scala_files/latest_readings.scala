import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode

var sensor_and_weather = spark.table("cwbryant_sensor_and_weather")

val w = Window.partitionBy($"sensorid")

var latest_readings = sensor_and_weather.withColumn("maxReadingTimestamp", max("ReadingTimeStamp").over(w)).
    filter($"maxReadingTimestamp" === $"ReadingTimeStamp").
    drop("maxReadingTimestamp").
    withColumnRenamed("maxReadingTimestamp", "ReadingTimeStamp")
    

latest_readings.write.mode(SaveMode.Overwrite).saveAsTable("cwbryant_latest_readings")
System.exit(0)
