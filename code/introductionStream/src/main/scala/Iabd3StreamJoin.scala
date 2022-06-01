import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Iabd3StreamJoin extends App {
  val sparkSession = SparkSession.
    builder()
    .master("local[*]")
    .appName("First app Streaming")
    .getOrCreate()

  sparkSession.sparkContext.setLogLevel("ERROR")
  sparkSession.conf.set("spark.sql.shuffle.partitions","5")

  /**
   * DataFrame Stream qui simule le trafic sur une page WEB :
   * Deux colonnes: adId et impressionTime
   */
  val impressions = sparkSession
    .readStream
    .format("rate") // rate génere des données en streaming avec deux colonnes:
                            // col1 = value : incrémental (0,1,2....)
                            // col2 = timestamp : temps de la génération de la données
                            // | value | timestamp |
                            // ---------------------
                            // | 0     | 2022-06-01 14:14:32.234 |
                            // | 1     | 2022-06-01 14:15:33.234 |
    .option("rowsPerSecond", "5")  // genere 5 lignes par seconde
    .option("numPartitions", "1") // nombre de partitions du DF
    .load()
    .select(
      col("value").as("adId"),
      col("timestamp").as("impressionTime")
    )


  // clicks on ad stream DF
  val clicks = sparkSession
    .readStream
    .format("rate")
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



  /**
   * Jointure de Stream
   */

  val joinStream = impressions.join(clicks, "adId")


  /**
   * Add waterMarks to Stream DataFrames
   */

  val impressionsWithWaterMarks =
    impressions
      .withColumnRenamed("adId", "impressionAdId")
      .withWatermark("impressionTime", "10 seconds")

  val clicksWithWaterMarks =
    clicks
      .withColumnRenamed("adId", "clicksAdId")
      .withWatermark("clickTime", "20 seconds")


  // Jointure avec Watermarks

  /**
   * Inner Join with watermarks
   */

  val joinWithWaterMarks = impressionsWithWaterMarks.join(
    clicksWithWaterMarks,
    expr("""
      clicksAdId = impressionAdId AND
      clickTime >= impressionTime AND
      clickTime <= impressionTime + interval 1 minutes
      """
    )
    // on accepte que les clicks arrivent au max 1 min après l'impression
  )

  /**
   * Left outer Join
   */

  val leftOuterjoinWithWaterMarks = impressionsWithWaterMarks.join(
    clicksWithWaterMarks,
    expr("""
      clicksAdId = impressionAdId AND
      clickTime >= impressionTime AND
      clickTime <= impressionTime + interval 1 minutes
      """
    ),
    "leftOuter"
  )

  /**
   * Write Streams
   */
  leftOuterjoinWithWaterMarks.writeStream
    .format("memory")
    .queryName("leftOuterjoinWithWaterMarks")  // dataframe en memoire à interroger avec Spark SQL
    .outputMode("append")
    .start()

  for (i <- 1 to 50 ) {
    sparkSession.sql(
      """
        | select * from leftOuterjoinWithWaterMarks
        |""".stripMargin).show(100,false)
    Thread.sleep(1000)
  }
}
