import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object Cours2 extends App {
  val spark = SparkSession
    .builder()
    .appName("CoursSparkStreaming")
    .master("local[*]")
    .getOrCreate()


  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.streaming.schemaInference","true")
  spark.conf.set("spark.sql.shuffle.partitions","5")
  // Create DataFrame
  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("../../data/retail-data/all/")

  //staticDataFrame.show(false)

  println("=== Count ====")
  staticDataFrame.select(count("StockCode")).show()
  println("=== CountDistinct ====")
  staticDataFrame.select(countDistinct("StockCode")).show()
  println("=== Count Approc ====")
  staticDataFrame.select(approx_count_distinct("StockCode", 0.3)).show()
  staticDataFrame.select(first("StockCode"),last("StockCode")).show()

  staticDataFrame.groupBy("InvoiceNo","CustomerId").count().show()
  staticDataFrame.groupBy("InvoiceNo").agg(count("Quantity")).show()

  staticDataFrame.groupBy("InvoiceNo").agg("Quantity" -> "avg", "CustomerId" -> "count").show()

  println(staticDataFrame.count())
  println(staticDataFrame.drop().count())

  //.select(count("StockCode"))

}
