// Databricks notebook source
// MAGIC %md 
// MAGIC # Spark Streaming
// MAGIC ## Structured Streaming

// COMMAND ----------

// MAGIC %md
// MAGIC ### version Straming du data Set
// MAGIC 1. Aller dans l'onglet ```data``` et vérifier le contenu du répertoire  ```/FileStore/tables/FileStore/streaming/inputs/activityData/```
// MAGIC 1. ouvrir une version de l'un de ces fichier sur la machine local pour voir à quoi le fichier ressemble

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creating RDD from source
// MAGIC #### Heterogeneity Human Activity Recognition Dataset
// MAGIC The data consists of smartphones and smartwatch sensor reading from a variety of devices
// MAGIC 
// MAGIC ##### Lire une version static du Dataset
// MAGIC 1. Create dataframe from json source  ```/FileStore/tables/FileStore/streaming/inputs/activityData/``` 
// MAGIC 
// MAGIC  - NB: le activityData est un répertoire qui contient plusieurs fichiers json (verifier dans le DBFS). Spark se charge de lire tous les fichiers contenu dans ce répertoire comme un seul DataFrame 
// MAGIC 1. créer une variable qui contient le schema du Dataframe Static ``` val dataSchema = static.schema ``` . ce Schema sera utilisé pour l'inférer au stream par la suite
// MAGIC 1. faire printSchema et show() pour voir à quoi la donnée ressemble
// MAGIC 1. farie un select ``` distinct ``` sur la colone ``` gt ``` et ensuite un ```show() ```
// MAGIC ##### The ``` gt ``` column: montre l'activité du user à un moment donné (i.e: sit, stand,walk, bike, stairsup, stairsdown) 

// COMMAND ----------

// votre code ici
//import org.apache.spark.sql.functions._
val activityDataDF = spark.read.json("/FileStore/tables/streaming/activityData")
//activityDataDF.printSchema()
activityDataDF.show()

activityDataDF.groupBy("gt").count().show()

val dataSchema = activityDataDF.schema

//activityDataDF.select('gt).distinct.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ### Straming du DataFrame
// MAGIC 1. Pour pouvoir inférer le schema définie plus haut ```dataSchema``` à notre stream:
// MAGIC Setté la conf ``` spark.sql.streaming.schemaInference ```à ``` true ```
// MAGIC 1. Pour créer un strcutured stream ```streaming``` qui va utiliser commee input source les fichiers json du répertoire ```/FileStore/tables/FileStore/streaming/inputs/activityData/```
// MAGIC    
// MAGIC     - lire le stream avec la fonction ```readStream``` disponible depuis le sparkSession ```spark```
// MAGIC     - utiliser l'option ```option("maxFilesPerTrigger",1)``` pour dire à spark que vous allez processer votre streaming 1 seul fichier à la fois

// COMMAND ----------

// votre code ici
spark.conf.set("spark.sql.streaming.schemaInference", true)

// declaration du stream

val dataStreaming = spark.readStream.schema(dataSchema).option("maxFilesPerTrigger", 1).json("/FileStore/tables/streaming/activityData")
val withEventTimeStream = dataStreaming.selectExpr(
  "*", 
    "cast (cast(Creation_Time as double)/1000000000 as timestamp) as eventTime")

// COMMAND ----------

import org.apache.spark.sql.functions._
withEventTimeStream.groupBy(window(col("eventTime"), "10 minutes", "5 minutes")).count
.writeStream.queryName("eventtimetb")
.format("memory")
.outputMode("complete").start


// COMMAND ----------

for (i <- 1 to 100) {
  spark.sql(" select * from eventtimetb ").show(false)
  Thread.sleep(3000)
}

// COMMAND ----------

 spark.sql(" select * from eventtimetb ").printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Streaming Transformation
// MAGIC 1. à partir du Dataframe ```streaming``` créer un dataframe ```activityCounts``` qui group et compte les activité de la colonne ```gt```   
// MAGIC   - utiliser ```groupBy```
// MAGIC   

// COMMAND ----------

// votre code ici
val activityCounts = dataStreaming.groupBy("gt").count()


// COMMAND ----------

// MAGIC %md
// MAGIC ### Streaming Action
// MAGIC La transformation définie plus haut est lazy, il faut spécifier une ```action```pour commencé la requete
// MAGIC 
// MAGIC Pour cela on doit spécifier:
// MAGIC 
// MAGIC   - le ```format```: output destination ou l'output sink pour cette query. Pour notre exemple on va travailler avec le sink ```memory```
// MAGIC   - outputMode : complete pour notre exemple, ce mode rewrite toutes les clés et leurs count à chaque trigger ( un trigger est defini à un fichier plus haut, par  ```.option("maxFilesPerTrigger",1)```)
// MAGIC   - queryName("activity_count")
// MAGIC 
// MAGIC 1. à partir du stream ```activitycount``` créer ```val activitycount```pour ecrire chaque stream dans le sink ```memory```
// MAGIC   - utiliser la fonction ```writeStream``` disponible sur les structured streams
// MAGIC   - faire un ```.start()``` pour démarrer le stream
// MAGIC   
// MAGIC 2. specifiy ```activitycount.awaitTermination``` pour prévenir que le driver exit pendant que la query tourne
// MAGIC   

// COMMAND ----------

// votre code ici

val stream = activityCounts.writeStream.format("memory").queryName("activity_count").outputMode("complete").start


// COMMAND ----------

// MAGIC %md
// MAGIC ### Check for active streams
// MAGIC 
// MAGIC Spark liste les streams actifs dans l'objet ```streams```du sparkSession ```spark```
// MAGIC 1. Afficher les active stream dans le sparkSession en appelant ```streams.active```

// COMMAND ----------

// votre code ici
spark.streams.active


// COMMAND ----------

// MAGIC %md
// MAGIC ### requete l'active stream
// MAGIC Le stream est envoyé en sink en mémoire où il est considérer comme une table sous le nom ```activity_counts```.
// MAGIC 
// MAGIC On peut donc la requeter:
// MAGIC  - On peut faire une simple loop sur les streams en sinks et show le resultat (on a créé un fichier par stream, on doit avoir autant de stream en sink que de fichier en source) 
// MAGIC  - attendre 1 seconde entre chaque resultat.
// MAGIC  
// MAGIC 1. Completer cette fonction pour faire cela: 
// MAGIC ```
// MAGIC for (i<- 1 to 15) {
// MAGIC  spark.sql(" SELECT... ").show()
// MAGIC  Thread.sleep(1000)
// MAGIC }```

// COMMAND ----------

//votre code ici

for (i <- 1 to 100) {
  spark.sql(" select * from activity_count ").show()
  Thread.sleep(3000)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Transformations on Streams
// MAGIC 
// MAGIC 1. faite un ```import org.apache.spark.sql.functions.expr``` pour pouvoir utiliser les expression
// MAGIC 1. Transformation à faire :
// MAGIC   - ajouter une colonne "stairs" qui est à true si la colonne ```gt``` contient le mot ```stairs```
// MAGIC   - filtrer sur la nouvelle colonne ```stairs``` (sa valeur est à true)
// MAGIC   - selectionner les colonne suivante: ```"gt","model","arrival_time","creation_time","stairs" ```
// MAGIC 1. faite un ```writeStream```
// MAGIC   - queryName : ```simple_transformation```
// MAGIC   - format: ```memory```
// MAGIC   - outputMode: ``append```
// MAGIC   - démarrer le stream avec le ```start```
// MAGIC   

// COMMAND ----------

// votre code ici
import org.apache.spark.sql.functions.expr
//val dataframeshow = activityDataDF.withColumn("stairs", expr("gt like '%stairs%'")).where("stairs").select("gt","model","arrival_time","creation_time","stairs")
//dataframeshow.show()

val streamingTransformation = dataStreaming
  .withColumn("stairs", expr("gt like '%stairs%'"))
  .where("stairs")
  .select("gt","model","arrival_time","creation_time","stairs")


val newStream = streamingTransformation.writeStream.format("memory").queryName("transformation").outputMode("append").start

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC #### requeter un stream
// MAGIC 1. Requeter comme précedement le stream ```simple_transformation```
// MAGIC en utilisant:
// MAGIC ```for (i<- 1 to 15) {
// MAGIC spark.sql(" SELECT... ").show()
// MAGIC Thread.sleep(1000)
// MAGIC }```

// COMMAND ----------

// votre code ici simple_transformation
%scala 
for (i <- 1 to 300) {
  spark.sql(" select * from transformation ").show(300)
  println("-------- New Data Stream ---------")
  Thread.sleep(3000)
}

// COMMAND ----------

spark.streams.active

// COMMAND ----------


