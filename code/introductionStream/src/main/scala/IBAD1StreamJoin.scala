import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, rand}

object IBAD1StreamJoin extends App {
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
   * Stream JOIN
   */

  val streamJoin = impressions.join(clicks, "adId")

  /**
   * With watermarks
   */

  val impressionsWithWatermark = impressions
    .select(
      col("adId").as("impressionAdId"),
      col("impressionTime"))
    .withWatermark("impressionTime", "10 seconds")
  // watermak 10 secondes => accepte 10 secondes de retard

  val clicksWithWatermark = clicks
    .select(
      col("adId").as("clickAdId"),
      col("clickTime"))
    .withWatermark("clickTime", "20 seconds")
  // watermak 20 secondes => accepte 20 secondes de retard

  // Jointure avec Watermarks

  /**
   * Inner Join with watermarks
   */

  val joinWithWaterMarks = impressionsWithWatermark.join(
    clicksWithWatermark,
    expr("""
      clickAdId = impressionAdId AND
      clickTime >= impressionTime AND
      clickTime <= impressionTime + interval 1 minutes
      """
    )
    // on accepte que les clicks arrivent au max 1 min après l'impression
  )

  /**
   * Left outer Join
   */

  val leftOuterjoinWithWaterMarks =impressionsWithWatermark.join(
    clicksWithWatermark,
    expr("""
      clickAdId = impressionAdId AND
      clickTime >= impressionTime AND
      clickTime <= impressionTime + interval 1 minutes
      """
    ),
    "leftOuter"
  )
  leftOuterjoinWithWaterMarks
    .writeStream
    .format("memory")
    .queryName("impressionsWithWatermark")  // dataframe en memoire à interroger avec Spark SQL
    .outputMode("append")
    .start()

  for (i <- 1 to 50 ) {
    sparkSession.sql(
      """
        | select * from impressionsWithWatermark
        |""".stripMargin).show(false)
    Thread.sleep(1000)
  }
}
