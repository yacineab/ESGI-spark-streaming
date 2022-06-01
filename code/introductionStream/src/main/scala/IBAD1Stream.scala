import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object IBAD1Stream extends App {

  // definition Spark Session
  val sparkSession = SparkSession.
    builder()
    .master("local[*]")
    .appName("First app Streaming")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  sparkSession.conf.set("spark.sql.shuffle.partitions","5")
  val retailDF = sparkSession
    .read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("/Users/yacine/IdeaProjects/ESGI-spark-streaming-bkp/data/retail-data/by-day/*.csv")

  //retailDF.printSchema()
 // retailDF.show()
  /**
   * Create Temp view in order to use Spark SQL
   */

  retailDF.createOrReplaceTempView("retail_table")


  /**
   * select customer id with total cost
   */

  val costDF = retailDF.selectExpr(
    "CustomerID",
  "(UnitPrice * Quantity) as total_cost",
  "InvoiceDate")

//  costDF.show(false)
/*
  val maxPurchase = retailDF.selectExpr(
    "CustomerID",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")
    .groupBy(
      col("CustomerID"),
      window(col("InvoiceDate"), "1 hour") as "invoiceDate"
    ).sum("total_cost")*/


 // maxPurchase.show(false)


  /**
   * Streaming
   */

  // Récuperation le schema static

  val retailschema = retailDF.schema

  // Lecture en streaming

  val retailStream = sparkSession
    .readStream
    .schema(retailschema)
    .format("csv")
    .option("maxFilesPerTrigger","1")
    .option("header","true")
    .load("/Users/yacine/IdeaProjects/ESGI-spark-streaming/data/by-day/*.csv")

//  println("spark is streaming " + retailStream.isStreaming)

 /* val maxPurchasePerHour = retailStream.selectExpr(
    "CustomerID",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")
    .groupBy(
      col("CustomerID"),
      window(col("InvoiceDate"), "1 hour") as "invoiceDate"
    ).sum("total_cost")*/

  // Streaming Action

 /* maxPurchasePerHour.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("customer_table")  // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start
*/
//  println(sparkSession.streams.active)

  /*for (i <- 1 to 50) {
    sparkSession.sql(
      """
        |Select * from customer_table
        |ORDER BY `sum(total_cost)` DESC
        |""".stripMargin
    ).show(false)

    Thread.sleep(1000)
  }*/

  /**
   * Aggregation
   */

  retailDF.select(count("StockCode")).show(false)
  retailDF.select(countDistinct("StockCode")).show(false)
  retailDF.select(approx_count_distinct("StockCode", 0.1)).show(false)
  retailDF.select(first("StockCode"), last("StockCode")).show(false)

  retailDF.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean (Quantity)").alias("mean_purchases"))
    .selectExpr(
      "total_purchases/total_transactions",
      "avg_purchases",
      "mean_purchases"
  ).show(false)

  retailDF
    .groupBy("InvoiceNo", "CustomerID")
    .count()
    .show(false)

  // Grouping
  retailDF
    .groupBy("InvoiceNo")
    .agg(count("Quantity").alias("quan"),
  expr("count (Quantity)"))
    .show(false)


  // grouping with Map
  // Map => Clé, valuer : clés -> valeur
  retailDF
    .groupBy("InvoiceNo")
    .agg("Quantity" -> "avg",
      "Quantity" -> "stddev_pop"
    )
    .show(false)

  val dfNoNull = retailDF.drop()
  dfNoNull.show()


  for (i <- 1 to 50) {
    sparkSession.sql(
      """
        |Select * from impression
        |""".stripMargin
    ).show(false)

    Thread.sleep(1000)
  }
}
