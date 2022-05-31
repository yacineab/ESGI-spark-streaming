import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IBAD3cours extends App {

  // /Users/yacine/IdeaProjects/ESGI-spark-streaming-bkp/data/retail-data/*.csv

  val sparkSession = SparkSession
    .builder()
    .appName("Spark Streaming IABD3")
    .master("local[*]")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")
  sparkSession.conf.set("spark.sql.shuffle.partitions", 5)

  val retailDF = sparkSession
    .read
    .option("header", true)
    .option("inferSchema", true)
    .csv("/Users/yacine/IdeaProjects/ESGI-spark-streaming-bkp/data/retail-data/by-day/*.csv")

  retailDF.printSchema()
  retailDF.show(false)

  // Créer une vue sql sur le dataframe
  retailDF.createOrReplaceTempView("retail_table")

  println("================= Spark SQL ================")
  val sqlDF = sparkSession.sql(
    """ select InvoiceNo, Description, CustomerID from retail_table
      |""".stripMargin)

 // sqlDF.show(false)

  val dfSchema = retailDF.schema

  println("================= Window ================")

  val maxPurchase = retailDF
  .selectExpr(
    "CustomerID",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate"
  )
    .groupBy(
      col("CustomerID"),
      window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")


  //maxPurchase.show(false)

  val retailStream = sparkSession
    .readStream
    .schema(dfSchema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", true)
    .load("/Users/yacine/IdeaProjects/ESGI-spark-streaming-bkp/data/retail-data/by-day/*.csv")


  println("======== Streaming ===========")
  println("spark is streaming : " + retailStream.isStreaming )


  val streamPurchase = retailStream
    .selectExpr(
      "CustomerID",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate"
    )
    .groupBy(
      col("CustomerID"),
      window(col("InvoiceDate"), "10 minutes"))
    .sum("total_cost")


  // Streaming Action
  streamPurchase
    .writeStream
    .format("memory")
    .queryName("stream_table")
    .outputMode("complete")
    .start()

for (i <- 1 to 50 ) {
  val memoryStreamdf = sparkSession.sql(
    """
      | select * from stream_table
      |""".stripMargin
  )

  memoryStreamdf.show(false)
  Thread.sleep(1000)
}

  System.in.read
  sparkSession.stop()
}
