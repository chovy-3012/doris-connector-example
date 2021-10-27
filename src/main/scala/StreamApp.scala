import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

object StreamApp {
  //kafka
  val kafkaServers = "xxxxx"
  val kafkaTopic = "xxxxx"
  //doris
  val feNodes = "xxxx:8030"
  val dorisUser = "root"
  val dorisPwd = ""
  val dorisTable = "test.test_stream"
  val checkpointLocation = "xxxxx"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()
    //create source from kafka
    val dataFrame = spark.readStream
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("startingOffsets", "latest")
      .option("subscribe", kafkaTopic)
      .format("kafka")
      .option("failOnDataLoss", false)
      .load()
    dataFrame.selectExpr("CAST(value AS STRING) as value_").where("length(get_json_object(value_,'$.message.output.result.rec'))<5000").createOrReplaceTempView("my_stream")
    val data = spark.sql("select get_json_object(value_,'$.message.recordId')," +
      "get_json_object(value_,'$.message.input.app.productId')," +
      "get_json_object(value_,'$.message.output.result.rec')," +
      "get_json_object(value_,'$.message.clientIp')," +
      "get_json_object(value_,'$.message.timestamp') from my_stream")
    //sink
    data.writeStream
      .format("doris")
      .option("checkpointLocation", checkpointLocation)
      //doris sink options
      .option("doris.table.identifier", dorisTable)
      .option("doris.fenodes", feNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      ///////////////////
      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .start().awaitTermination()
  }
}