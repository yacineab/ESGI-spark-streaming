import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object IABD2stream extends App {

val sparkSession = SparkSession
  .builder()
  .master("local[*]")
  .appName("streaming")
  .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  sparkSession.conf.set("spark.sql.shuffle.partitions","5")

  val retaildf = sparkSession
    .read
    .format("csv")
    .option("inferSchema","true")
    .option("header","true")
    .load("/Users/yacine/IdeaProjects/ESGI-spark-streaming-bkp/data/retail-data/by-day/*.csv")


  // retaildf.printSchema()
  // retaildf.show(false)

  // create Temp view to use Spark SQL
  retaildf.createOrReplaceTempView("retail_table")
  val sqlDF = sparkSession.sql(
    """
      |select InvoiceNo, Description, InvoiceDate from retail_table
      |""".stripMargin)

  // sqlDF.show(false)

  // calculer total depense par jour et par client

  val pruchasePerDay = retaildf
    .selectExpr(
      "CustomerID",
      "(Quantity * UnitPrice) as total_cost",
      "InvoiceDate"
    )
    .groupBy(
      col("CustomerID"), window(col("InvoiceDate"), "1 hour")
    )
    .sum("total_cost")

// pruchasePerDay.show(false)


  val maxPerDay = retaildf
    .selectExpr(
      "CustomerID",
      "((Quantity * UnitPrice)) as total_cost",
      "InvoiceDate"
    ).agg(max("total_cost") as "max_per_day")

  // maxPerDay.show()


  /**
   * STREAMING
   */
  // definir le schema pour le streaming
  val retailSchema = retaildf.schema

  val retailStream = sparkSession.readStream
    .schema(retailSchema)
    .option("maxFilesPerTrigger",1)
    .format("csv")
    .option("header","true")
    .load("/Users/yacine/IdeaProjects/ESGI-spark-streaming/data/by-day/*.csv")

 println("retail is streaming: " + retailStream.isStreaming)


  val purchaseByCustomerPerDay = retailStream
    .selectExpr(
      "CustomerID",
      "(Quantity * UnitPrice) as total_cost",
      "InvoiceDate"
    )
    .groupBy(
      col("CustomerID"), window(col("InvoiceDate"), "1 hour")
    )
    .sum("total_cost")

  purchaseByCustomerPerDay.writeStream
    .format("memory") // memory = sotre in-memory table
    .queryName("purchase_stream") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()


  for (i <- 1 to 50 ) {
    sparkSession.sql(

      """
        |select * from purchase_stream
        |order by `sum(total_cost)` DESC
        |""".stripMargin)
      .show(false)

    Thread.sleep(1000)
  }

// Aggregation
  retaildf.select(count("StockCode")).show(false)
  retaildf.select(countDistinct("StockCode")).show(false)
  retaildf.select(approx_count_distinct("StockCode", 0.02)).show(false)
  retaildf.select(first("StockCode"), last("StockCode")).show(false)

  retaildf.select(
    count("Quantity").alias("total_transaction"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean (Quantity)").alias("mean_purchases")
  )
    .selectExpr(
      "total_purchases/total_transaction",
      "avg_purchases",
      "mean_purchases"
    )
    .show(false)

  retaildf.groupBy("InvoiceNo", "CustomerId").count().show(false)

  retaildf.groupBy("InvoiceNo")
    .agg(
      count("Quantity").alias("quant"),
      expr("count(Quantity)")
    ).show(false)

  // Group By avec MAP
  retaildf.groupBy("InvoiceNo")
    .agg("Quantity" -> "avg")
    .show(false)

  // delete null value

  val dfNoNull = retaildf.drop()

  dfNoNull.show(false)
  /*System.in.read
  sparkSession.stop()*/

}
