import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
object WorldCountFromKafkaSource extends App{
  val spark=SparkSession
    .builder()
    .appName("learning")
    .master("local[2]")
    .getOrCreate()
  import spark.implicits._


  //taking input from kafka and output in console
  val df=spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe","topic1")
    .load()

  val df2=df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    .as[(String, String)]
  val df3=df2.map(x=>x._2)
  val df4=df3.flatMap(_.split(" "))
  val count=df4.groupBy("value").count()


  val query=count.writeStream.format("console").outputMode("complete").start
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  query.awaitTermination

}
