import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Iabd2StreamJoin extends App {
  val sparkSession = SparkSession.
    builder()
    .master("local[*]")
    .appName("First app Streaming")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")

  sparkSession.conf.set("spark.sql.shuffle.partitions","5")

  /**
   * Simulation trafic Web
   * impressions stream = trafic de visite page de pub
   *  Renvoi deux colonnes: adId et impressionTime
   */
  val impressions = sparkSession
    .readStream
    .format("rate")  // créé un Stream DF avec 2 colonnes : Value et timestamp
    .option("rowsPerSecond", "5")
    .option("numPartitions", "1")
    .load()
    .select(
      col("value").as("adId"),
      col("timestamp").as("impressionTime")
    )

  /**
   * Simulation trafic Web
   * clicks stream = clicks sur les pubs
   * Renvoi deux colonnes: adId et clickTime
   */
  val clicks = sparkSession
    .readStream
    .format("rate")  // deux colonnes : Value et Timestamp
    .option("rowsPerSecond", "5")
    .option("numPartitions", "1")
    .load()
    .where((rand() * 100).cast("integer") < 10)
    // 10 out of every 100 impressions result in a click
    .select(
      expr("value - 50").as("adId"),
      col("timestamp").as("clickTime"))
    // -100 so that a click with same id as impression is generated much later.
    .where("adId > 0")


}
