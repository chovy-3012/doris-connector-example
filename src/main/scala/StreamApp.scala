import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

import java.util.concurrent.TimeUnit

object StreamApp {
  val feNodes = "10.24.1.40:8030"
  val dorisUser = "root"
  val dorisPwd = ""
  val dorisTable = "test.test_stream"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .getOrCreate()
    //create source from kafka
    val dataFrame = spark.readStream
      .option("kafka.bootstrap.servers", "10.24.1.10:6667")
      .option("startingOffsets", "latest")
      .option("subscribe", "asrlite")
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
      .option("checkpointLocation", "/user/smart_etl/tmp/doris-test/ck")
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