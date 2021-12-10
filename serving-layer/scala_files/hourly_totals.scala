import org.apache.spark.sql.SaveMode

var sensor = spark.table("cwbryant_sensor")

// Scale double values to bigint by multiplying by 10,000,000 in order to be atomically incremented
var hourly_totals = sensor.groupBy("SensorID").
    agg(
    count(when(col("ReadingHour") === "00", true)).as("00ReadingTotal"),
    sum(when(col("ReadingHour") === "00", col("WaterLevel")*lit(10000000)).otherwise(0)).as("00ReadingSum"),
    count(when(col("ReadingHour") === "01", true)).as("01ReadingTotal"),
    sum(when(col("ReadingHour") === "01", col("WaterLevel")*lit(10000000)).otherwise(0)).as("01ReadingSum"),
    count(when(col("ReadingHour") === "02", true)).as("02ReadingTotal"),
    sum(when(col("ReadingHour") === "02", col("WaterLevel")*lit(10000000)).otherwise(0)).as("02ReadingSum"),
    count(when(col("ReadingHour") === "03", true)).as("03ReadingTotal"),
    sum(when(col("ReadingHour") === "03", col("WaterLevel")*lit(10000000)).otherwise(0)).as("03ReadingSum"),
    count(when(col("ReadingHour") === "04", true)).as("04ReadingTotal"),
    sum(when(col("ReadingHour") === "04", col("WaterLevel")*lit(10000000)).otherwise(0)).as("04ReadingSum"),
    count(when(col("ReadingHour") === "05", true)).as("05ReadingTotal"),
    sum(when(col("ReadingHour") === "05", col("WaterLevel")*lit(10000000)).otherwise(0)).as("05ReadingSum"),
    count(when(col("ReadingHour") === "06", true)).as("06ReadingTotal"),
    sum(when(col("ReadingHour") === "06", col("WaterLevel")*lit(10000000)).otherwise(0)).as("06ReadingSum"),
    count(when(col("ReadingHour") === "07", true)).as("07ReadingTotal"),
    sum(when(col("ReadingHour") === "07", col("WaterLevel")*lit(10000000)).otherwise(0)).as("07ReadingSum"),
    count(when(col("ReadingHour") === "08", true)).as("08ReadingTotal"),
    sum(when(col("ReadingHour") === "08", col("WaterLevel")*lit(10000000)).otherwise(0)).as("08ReadingSum"),
    count(when(col("ReadingHour") === "09", true)).as("09ReadingTotal"),
    sum(when(col("ReadingHour") === "09", col("WaterLevel")*lit(10000000)).otherwise(0)).as("09ReadingSum"),
    count(when(col("ReadingHour") === "10", true)).as("10ReadingTotal"),
    sum(when(col("ReadingHour") === "10", col("WaterLevel")*lit(10000000)).otherwise(0)).as("10ReadingSum"),
    count(when(col("ReadingHour") === "11", true)).as("11ReadingTotal"),
    sum(when(col("ReadingHour") === "11", col("WaterLevel")*lit(10000000)).otherwise(0)).as("11ReadingSum"),
    count(when(col("ReadingHour") === "12", true)).as("12ReadingTotal"),
    sum(when(col("ReadingHour") === "12", col("WaterLevel")*lit(10000000)).otherwise(0)).as("12ReadingSum"),
    count(when(col("ReadingHour") === "13", true)).as("13ReadingTotal"),
    sum(when(col("ReadingHour") === "13", col("WaterLevel")*lit(10000000)).otherwise(0)).as("13ReadingSum"),
    count(when(col("ReadingHour") === "14", true)).as("14ReadingTotal"),
    sum(when(col("ReadingHour") === "14", col("WaterLevel")*lit(10000000)).otherwise(0)).as("14ReadingSum"),
    count(when(col("ReadingHour") === "15", true)).as("15ReadingTotal"),
    sum(when(col("ReadingHour") === "15", col("WaterLevel")*lit(10000000)).otherwise(0)).as("15ReadingSum"),
    count(when(col("ReadingHour") === "16", true)).as("16ReadingTotal"),
    sum(when(col("ReadingHour") === "16", col("WaterLevel")*lit(10000000)).otherwise(0)).as("16ReadingSum"),
    count(when(col("ReadingHour") === "17", true)).as("17ReadingTotal"),
    sum(when(col("ReadingHour") === "17", col("WaterLevel")*lit(10000000)).otherwise(0)).as("17ReadingSum"),
    count(when(col("ReadingHour") === "18", true)).as("18ReadingTotal"),
    sum(when(col("ReadingHour") === "18", col("WaterLevel")*lit(10000000)).otherwise(0)).as("18ReadingSum"),
    count(when(col("ReadingHour") === "19", true)).as("19ReadingTotal"),
    sum(when(col("ReadingHour") === "19", col("WaterLevel")*lit(10000000)).otherwise(0)).as("19ReadingSum"),
    count(when(col("ReadingHour") === "20", true)).as("20ReadingTotal"),
    sum(when(col("ReadingHour") === "20", col("WaterLevel")*lit(10000000)).otherwise(0)).as("20ReadingSum"),
    count(when(col("ReadingHour") === "21", true)).as("21ReadingTotal"),
    sum(when(col("ReadingHour") === "21", col("WaterLevel")*lit(10000000)).otherwise(0)).as("21ReadingSum"),
    count(when(col("ReadingHour") === "22", true)).as("22ReadingTotal"),
    sum(when(col("ReadingHour") === "22", col("WaterLevel")*lit(10000000)).otherwise(0)).as("22ReadingSum"),
    count(when(col("ReadingHour") === "23", true)).as("23ReadingTotal"),
    sum(when(col("ReadingHour") === "23", col("WaterLevel")*lit(10000000)).otherwise(0)).as("23ReadingSum")
)

hourly_totals.write.mode(SaveMode.Overwrite).saveAsTable("cwbryant_hourly_totals")
System.exit(0)
