import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.util.Bytes

object StreamReadings {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")
  
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val hourlyTotals = hbaseConnection.getTable(TableName.valueOf("cwbryant_hourly_totals_view"))


  def incrementReadingHourlyTotals(kr : KafkaReading) : String = {
    val inc = new Increment(Bytes.toBytes(kr.sensor_id))
    if(kr.reading_hour == "00") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("00ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("00ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "01") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("01ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("01ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "02") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("02ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("02ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "03") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("03ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("03ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "04") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("04ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("04ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "05") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("05ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("05ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "06") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("06ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("06ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "07") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("07ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("07ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "08") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("08ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("08ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "09") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("09ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("09ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "10") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("10ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("10ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "11") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("11ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("11ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "12") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("12ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("12ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "13") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("13ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("13ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "14") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("14ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("14ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "15") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("15ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("15ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "16") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("16ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("16ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "17") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("17ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("17ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "18") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("18ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("18ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "19") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("19ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("19ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "20") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("20ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("20ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "21") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("21ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("21ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "22") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("22ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("22ReadingSum"), (kr.water_level*10000000).longValue())
    }
    if(kr.reading_hour == "23") {
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("23ReadingTotal"), 1)
      inc.addColumn(Bytes.toBytes("totals"), Bytes.toBytes("23ReadingSum"), (kr.water_level*10000000).longValue())
    }

    hourlyTotals.increment(inc)
    return "Updated speed layer for reading from sensor id " + kr.sensor_id
  }
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }
    
    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamReadings")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("cwbryant_readings")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    val serializedRecords = stream.map(_.value);

    val krs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaReading]))
    System.out.println("Streaming Consumer Started")

    // Update speed table    
    val processedFlights = krs.map(incrementReadingHourlyTotals)
    processedFlights.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
