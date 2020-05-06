import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
object KafkaSink extends App{
  val spark=SparkSession
    .builder()
    .appName("learning")
    .master("local[12]")
    .getOrCreate()
  import spark.implicits._


  //taking input from kafka topic1 and output in console
  val df=spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe","topic1")
    .option("failOnDataLoss","false")
    .load()





  //writing to kafka topic2
  val outputdf=df
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "topic2")
    .option("checkpointLocation","false")
    .start()

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  outputdf.awaitTermination



}
