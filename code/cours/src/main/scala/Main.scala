import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, window}

object Main extends App {

  val spark = SparkSession
    .builder()
    .config("spark.sql.shuffle.partitions","5")
    .appName("CoursSparkStreaming")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Create DataFrame
  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("../../data/retail-data/by-day/")

  //staticDataFrame.printSchema()
 // staticDataFrame.show(10, false)

  // Create tempTable
  staticDataFrame.createOrReplaceTempView("retail_data")
 // spark.sql("select InvoiceNo,Description from retail_data").show(false)

  // Create staticSchema
   val staticSchema = staticDataFrame.schema

  val largePurchase = staticDataFrame
    .selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(
      col("CustomerId"), window(col("InvoiceDate"),
        "1 day","1 day","14 hour"))
    .sum("total_cost")

  // show static dataframe
  //largePurchase.show(false)

  // set default partition conf
  val streamingDataFrame = spark.readStream
    .schema(staticSchema)
    .option("maxFilesPerTrigger", 1)
    .format("csv")
    .option("header", "true")
    .load("../../data/retail-data/by-day/*.csv")

  println("is streaming " + streamingDataFrame.isStreaming)

  //streamingDataFrame.printSchema()

  val purchaseByCustomerPerHour = streamingDataFrame.selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost", "InvoiceDate")
    .groupBy(
      col("CustomerId"), window(col("InvoiceDate"), "10 minutes"))
    .sum("total_cost")

  purchaseByCustomerPerHour.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("customer_purchases") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start

  for (i <- 1 to 50) {
  spark.sql("""
      SELECT *
      FROM customer_purchases
      ORDER BY `sum(total_cost)` DESC """)
    .show( false)
    Thread.sleep(500)
  }



}
