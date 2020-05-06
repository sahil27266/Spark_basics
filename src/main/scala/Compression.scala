import org.apache.log4j.{Level, Logger}  //this import are to prevent the un necessary logs on console
import org.apache.spark.sql.SparkSession

object Compression extends App {
  val spark=SparkSession
    .builder()
    .master("local[2]")   //you can take any number of cores>1 as per your requirement
    .appName("Compression")
    .getOrCreate()


  import spark.implicits._

  val df=spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("subscribe","topicX")  //you can specify the name of the topic from where you want to get the stream
    .option("failOnDataLoss","false") //this setting is to prevent the stopping app in case of data loss
    .load()

  val df2=df.selectExpr("CAST(KEY AS STRING)","CAST(VALUE AS STRING)") //converting the key and value in string
  val df3=df2.as[Tuple2[String,String]]
  val lines=df3.map(x=>x._2)  //storing only value(not necessary for yot to do the same)


  val query=lines.writeStream
    .format("text")
    .option("compression", "bZip2")  //specify the compression technique to be use for compression
    .option("checkpointLocation","Enter your checkpointing location")
    .option("path","enter the path where you want to write")
    .start


  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  query.awaitTermination
}
