# Databricks notebook source
# MAGIC %md # Structured Streaming using Python DataFrames API
# MAGIC 
# MAGIC Apache Spark 2.0 adds the first version of a new higher-level stream processing API, Structured Streaming. In this notebook we are going to take a quick look at how to use DataFrame API to build Structured Streaming applications. We want to compute real-time metrics like running counts and windowed counts on a stream of timestamped actions.
# MAGIC 
# MAGIC To run this notebook, import it to Databricks Community Edition and attach it to a **Spark 2.1 (Scala 2.10)** cluster.

# COMMAND ----------

# MAGIC %md ## Data
# MAGIC We will be using the same flights datasets as the previous sections.
# MAGIC The first few cells are simply the same data loading and cleaning you have already seen.

# COMMAND ----------

# To make sure that this notebook is being run on a Spark 2.0 cluster, let's see if we can access the SparkSession - the new entry point of Apache Spark 2.0.
# If this fails, then you are not connected to a Spark 2.0 cluster. Please recreate your cluster and select the version to be "Spark 2.0 (Scala 2.10)".
spark

# COMMAND ----------

# Set File Paths
tripdelaysFilePath = "/databricks-datasets/flights/departuredelays.csv"
airportsnaFilePath = "/databricks-datasets/flights/airport-codes-na.txt"

# Obtain airports dataset
airportsna = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true', delimiter='\t').load(airportsnaFilePath)
airportsna.registerTempTable("airports_na")
airportsna.show()

# Obtain departure Delays data
departureDelay = sqlContext.read.format("com.databricks.spark.csv").options(header='true').load(tripdelaysFilePath)
departureDelay.show()
departureDelays = sqlContext.read.format("com.databricks.spark.csv").schema(departureDelay.schema).options(header='true').load(tripdelaysFilePath)
departureDelays.show()
departureDelays.registerTempTable("departureDelays")
departureDelays.cache()


# COMMAND ----------



# Available IATA codes from the departuredelays sample dataset
tripIATA = sqlContext.sql("select distinct iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a")
tripIATA.registerTempTable("tripIATA")
tripIATA.show()
# Only include airports with atleast one trip from the departureDelays dataset
airports = sqlContext.sql("select f.IATA, f.City, f.State, f.Country from airports_na f join tripIATA t on t.IATA = f.IATA")
airports.registerTempTable("airports")
airports.cache()
airports.show()

# COMMAND ----------

# Build `departureDelays_geo` DataFrame
#  Obtain key attributes such as Date of flight, delays, distance, and airport information (Origin, Destination)  
departureDelays_geo = sqlContext.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat('2014-', concat(concat(substr(cast(f.date as string), 1, 2), '-')), substr(cast(f.date as string), 3, 2)), ' '), substr(cast(f.date as string), 5, 2)), ':'), substr(cast(f.date as string), 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int), cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src, d.state as state_dst from departuredelays f join airports o on o.iata = f.origin join airports d on d.iata = f.destination") 

# RegisterTempTable
#departureDelays_geo.repartition(50).write.json("departureDelays_json")
departureDelays_geo.show()
# Cache and Count
departureDelays_geo.cache()
departureDelays_geo.count()



# COMMAND ----------

# MAGIC %md ## Sample Data
# MAGIC We are now going to save the contents of the departureDelays_geo dataframe to JSON files, and use it to simulate streaming data.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "200")  # Use 200 partitions for shuffling
#departureDelays_geo.orderBy("localdate").write.json("departureDelays_json")
departureDelays_geo_schema = departureDelays_geo.schema
departureDelays_geo_schema

# COMMAND ----------

# MAGIC %fs head /departureDelays_json/part-00015-tid-3892456853287454747-116e3f75-1d66-48b6-a3fc-2ac7fdf6cc59-8512-1-c000.json

# COMMAND ----------

# MAGIC %md There are 200 JSON files in the directory. Let's see what each JSON file contains.

# COMMAND ----------

# MAGIC %fs ls /departureDelays_json/part-00015-tid-3892456853287454747-116e3f75-1d66-48b6-a3fc-2ac7fdf6cc59-8512-1-c000.json

# COMMAND ----------

# MAGIC %md ## Batch/Interactive Processing
# MAGIC The usual first step in attempting to process the data is to interactively query the data. Let's define a static DataFrame on the files, and give it a table name.

# COMMAND ----------

from pyspark.sql.types import *

inputPath = "/departureDelays_json"
#    .schema(departureDelays_geo_schema)


# Static DataFrame representing data in the JSON files
staticInputDF = (
  spark
    .read
    .json(inputPath)
)

display(staticInputDF)

# COMMAND ----------

# MAGIC %md Now we can compute the number of flights leaving from each state, with 5 hour time windows. To do this, we will group by the `state_src` column and 5 hour windows over the `localdate` column.

# COMMAND ----------

from pyspark.sql.functions import *      # for window() function

staticCountsDF = (
  staticInputDF
    .groupBy(
      staticInputDF.state_src, 
      window(staticInputDF.localdate, "5 hours"))    
    .count()
)

staticCountsDF.printSchema()
staticCountsDF.sort(desc("count")).show()
staticCountsDF.cache()

# Register the DatFrame as table 'static_counts'
staticCountsDF.createOrReplaceTempView("static_counts")

# COMMAND ----------

# MAGIC %md Now we can directly use SQL to query the table. For example, here is a timeline of the windowed counts of flights leaving from each state.

# COMMAND ----------

# MAGIC %sql select state_src, date_format(window.end, "MMM-dd HH:mm") as time, count from static_counts order by count desc, time, state_src

# COMMAND ----------

# MAGIC %md ## Stream Processing 
# MAGIC Now that we have analyzed the data interactively, let's convert this to a streaming query that continuously updates as data comes. Since we just have a static set of files, we are going to emulate a stream from them by reading one file at a time, in the chronological order they were created. The query we have to write is pretty much the same as the interactive query above.

# COMMAND ----------

from pyspark.sql.functions import *

inputPath = "/departureDelays_json"
departureDelays_geo_schema = departureDelays_geo.schema
# Similar to definition of staticInputDF above, just using `readStream` instead of `read`
streamingInputDF = (
  spark
    .readStream                       
    .schema(departureDelays_geo_schema) # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)

# Same query as staticInputDF
streamingCountsDF = (                 
  streamingInputDF
    .groupBy(
      streamingInputDF.state_src, 
      window(streamingInputDF.localdate, "5 hours"))    
    .count()
)

# Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

# COMMAND ----------

# MAGIC %md As you can see, `streamingCountsDF` is a streaming Dataframe (`streamingCountsDF.isStreaming` was `true`). You can start streaming computation, by defining the sink and starting it. 
# MAGIC In our case, we want to interactively query the counts (same queries as above), so we will set the complete set of 1 hour counts to be in a in-memory table (note that this for testing purpose only in Spark 2.0).

# COMMAND ----------

from pyspark.sql.streaming import *

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")  # keep the size of shuffles small

query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

# COMMAND ----------

# MAGIC %md `query` is a handle to the streaming query that is running in the background. This query is continuously picking up files and updating the windowed counts. 
# MAGIC 
# MAGIC Note the status of query in the above cell. Both the `Status: ACTIVE` and the progress bar shows that the query is active. 
# MAGIC Furthermore, if you expand the `>Details` above, you will find the number of files they have already processed. 
# MAGIC 
# MAGIC Let's wait a bit for a few files to be processed and then interactively query the in-memory `counts` table.

# COMMAND ----------

from time import sleep
sleep(5)  # wait a bit for computation to start

# COMMAND ----------

# MAGIC %sql select state_src, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by count desc, time, state_src

# COMMAND ----------

# MAGIC %md We see the timeline of windowed counts (similar to the static one ealrier) building up. If we keep running this interactive query repeatedly, we will see the latest updated counts which the streaming query is updating in the background.

# COMMAND ----------

sleep(5)  # wait a bit more for more data to be computed

# COMMAND ----------

# MAGIC %sql select state_src, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, state_src

# COMMAND ----------

# MAGIC %md If you keep running the above query repeatedly, you will observe that earlier dates don't appear after later dates have been observed, as expected in a data stream where data appears in time-sorted order. This shows that Structured Streaming ensures **prefix integrity**. Structured streaming also has settings to control how out-of-order data is handled. Read the blog posts linked below if you want to know more.
# MAGIC 
# MAGIC Note that there are only a few files, so after consuming all of them there will be no updates to the counts. Rerun the query if you want to interact with the streaming query again.
# MAGIC 
# MAGIC Finally, you can stop the query running in the background, either by clicking on the 'Cancel' link in the cell of the query, or by executing `query.stop()`. Either way, when the query is stopped, the status of the corresponding cell above will automatically update to `TERMINATED`.

# COMMAND ----------


