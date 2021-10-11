import org.apache.spark.sql.SparkSession

object BatchApp {
  val feNodes = "10.24.1.40:8030"
  val dorisUser = "root"
  val dorisPwd = ""
  val dorisTable = "test.test_batch"

  def main(args: Array[String]): Unit = {
    val path = args(0)
    val spark = SparkSession.builder().getOrCreate()
    //create source from hdfs
    val df = spark.read.json(path)
    df.createOrReplaceTempView("test")
    val data = spark.sql("select message.recordId," +
      "message.input.app.productId," +
      "message.output.result.rec," +
      "message.clientIp," +
      "message.timestamp from test")
    //sink
    data.write.format("doris")
      //doris sink options
      .option("doris.table.identifier", dorisTable)
      .option("doris.fenodes", feNodes)
      .option("user", dorisUser)
      .option("password", dorisPwd)
      ///////
      .save()
  }
}