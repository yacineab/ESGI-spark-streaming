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

  val impressions = sparkSession
    .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
    .select(col("value").as("adId"), col("timestamp").as("impressionTime"))

  val clicks = sparkSession
    .readStream.format("rate").option("rowsPerSecond", "5").option("numPartitions", "1").load()
    .where((rand() * 100).cast("integer") < 10)
    // 10 out of every 100 impressions result in a click
    .select(expr("value - 50").as("adId"), col("timestamp").as("clickTime"))
    // -100 so that a click with same id as impression is generated much later.
    .where("adId > 0")


  /**
   * Jointure entre deux Stream
   */
  val joinStream = impressions.join(clicks, "adId")


  /**
   * Ajout de watermak aux deux stream
   *
   */

  val impressionsWithWatermark = impressions
    .select(
      col("adId").as("impressionAdId"),
      col("impressionTime"))
    .withWatermark("impressionTime", "10 seconds ")
  // watermak 10 secondes => accepte 10 secondes de retard

  val  clicksWithWatermark = clicks
    .select(
      col("adId").as("clickAdId"),
      col("clickTime"))
    .withWatermark("clickTime", "20 seconds")
  // watermak 20 secondes => accepte 20 secondes de retard


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
  /**
   * Affichage des streams
   */
  leftOuterjoinWithWaterMarks.writeStream
    .format("memory") // memory = sotre in-memory table
    .queryName("leftOuterjoinWithWaterMarks") // the name of the in-memory table
    .outputMode("append") //
    .start()

  for (i <- 1 to 50 ) {
    sparkSession.sql(

      """
        |select * from leftOuterjoinWithWaterMarks
        |""".stripMargin)
      .show(100, false)

    Thread.sleep(1000)
  }
}
