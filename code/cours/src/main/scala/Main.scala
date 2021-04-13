import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}

object Main extends App {

  val spark = SparkSession
    .builder()
    .appName("CoursSparkStreaming")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  // Create DataFrame
  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("../../data/retail-data/by-day/*.csv")

  // Create tempTable
  staticDataFrame.createOrReplaceTempView("retail_data")

  // Create staticSchema
  val staticSchema = staticDataFrame.schema

  val largePurchase = staticDataFrame
    .selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(
      col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")

  //largePurchase.show(false)

  val streamingDataFrame = spark.readStream
    .schema(staticSchema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")
    .load("../../data/retail-data/by-day/*.csv")

  println("is streaming " + streamingDataFrame.isStreaming)

  val purchaseByCustomerPerHour = streamingDataFrame .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(
      col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")

  purchaseByCustomerPerHour.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("customer_purchases") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start


  for (i <- 1 to 50) {
  spark.sql("""
      SELECT *
      FROM customer_purchases_2
      ORDER BY `sum(total_cost)` DESC """)
    .show(5, false)
    Thread.sleep(3000)
  }
}
