import scala.reflect.runtime.universe._


case class KafkaReading(
    sensor_id: String,
    water_level: Double,
    reading_hour: String)