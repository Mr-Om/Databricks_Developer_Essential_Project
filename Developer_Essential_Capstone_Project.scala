// Databricks notebook source


// # Databricks Partner Capstone Project
// This capstone is designed to review and validate key topics related to Databricks, Structured Streaming, and Delta. 
// Upon successful completion of the capstone, you will receive a certificate of accreditation. Successful completion will be tracked alongside your partner profile, and will help our team identify individuals qualified for additional advanced training opportunities. 
// Certificates should arrive within a week of successful completion. **All tests must be passed successfully for certification**. If you have questions about your completion status, please email [training-enb@databricks.com](mailto:training-enb@databricks.com).

// COMMAND ----------

// # Capstone Overview
// 
// In this project you will implement a multi-hop Delta Lake architecture using Spark Structured Streaming.
// This architecture allows actionable insights to be derived from validated data in a data lake. Because Delta Lake provides ACID transactions and enforces schema, customers can build systems around reliable, available views of their data stored in economy cloud object stores.
//  
//  ## Scenario:
//  
//  A video gaming company stores historical data in a data lake, which is growing exponentially. 
//  
//  The data isn't sorted in any particular way (actually, it's quite a mess) and it is proving to be _very_ difficult to query and manage this data because there is so much of it.
// 
// Your goal is to create a Delta pipeline to work with this data. The final result is an aggregate view of the number of active users by week for company executives. You will:
// * Create a streaming Bronze table by streaming from a source of files
// * Create a streaming Silver table by enriching the Bronze table with static data
// * Create a streaming Gold table by aggregating results into the count of weekly active users
// * Visualize the results directly in the notebook
// 
// MAGIC ## Testing your Code
// MAGIC There are 4 test functions imported into this notebook:
// MAGIC * realityCheckBronze
// MAGIC * realityCheckStatic
// MAGIC * realityCheckSilver
// MAGIC * realityCheckGold
// MAGIC 
// MAGIC To run automated tests against your code, you will call a `realityCheck` function and pass the function you write as an argument. The testing suite will call your functions against a different dataset so it's important that you don't change the parameters in the function definitions. 
// MAGIC 
// MAGIC To test your code yourself, simply call your function, passing the correct arguments. 

// MAGIC ## Getting Started
// MAGIC 
// MAGIC Run the following cell to configure our environment.

// COMMAND ----------

// MAGIC %run "./Includes/Capstone-Setup"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Configure shuffle partitions
// MAGIC 

// COMMAND ----------

sqlContext.setConf("spark.sql.shuffle.partitions", "8")

// COMMAND ----------

// %md-sandbox
// 
// ### Set up paths
// 
// The cell below sets up relevant paths in DBFS.
// 
// It also clears out this directory (to ensure consistent results if re-run). This operation can take several minutes.


val inputPath = userhome + "/source"

val basePath = userhome + "/capstone"
val outputPathBronze = basePath + "/gaming/bronze"
val outputPathSilver = basePath + "/gaming/silver"
val outputPathGold   = basePath + "/gaming/gold"

dbutils.fs.rm(basePath, true)


// MAGIC ### Step 1: Prepare Schema and Read Streaming Data from input source


// COMMAND ----------

import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType, IntegerType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

lazy val eventSchema = StructType(List(
  StructField("eventName", StringType, true),
  StructField("eventParams", StructType(List(
    StructField("game_keyword", StringType, true),
    StructField("app_name", StringType, true),
    StructField("scoreAdjustment", IntegerType, true),
    StructField("platform", StringType, true),
    StructField("app_version", StringType, true),
    StructField("device_id", StringType, true),
    StructField("client_event_time", TimestampType, true),
    StructField("amount", DoubleType, true)
  )), true)
))

var gamingEventDF = (spark
  .readStream
  .schema(eventSchema) 
  .option("streamName","mobilestreaming_demo") 
  .option("maxFilesPerTrigger", 1)                // treat each file as Trigger event
  .json(inputPath) 
) 

// COMMAND ----------

// %md-sandbox
// ### Step 2: Write Stream to Bronze Table
// 
// Complete the `writeToBronze` function to perform the following tasks:
// 
// * Write the stream from `gamingEventDF` -- the stream defined above -- to a bronze Delta table in path defined by `outputPathBronze`.
// * Convert the (nested) input column `client_event_time` to a date format and rename the column to `eventDate`
// * Filter out records with a null value in the `eventDate` column
// * Make sure you provide a checkpoint directory that is unique to this stream
// 
//  Using `append` mode when streaming allows us to insert data indefinitely without rewriting already processed data.


import org.apache.spark.sql.functions.to_date

def writeToBronze(sourceDataframe: org.apache.spark.sql.DataFrame, bronzePath: String, streamName: String): Unit = {
  (sourceDataframe
  .withColumn("eventParams",$"eventParams".withField("client_event_time",to_date($"eventParams.client_event_time")))
  .withColumn("eventDate",$"eventParams.client_event_time")
  .withColumn("eventParams",$"eventParams".withField("client_event_time",to_timestamp($"eventParams.client_event_time")))
  .filter($"eventDate".isNotNull)
  .writeStream
  .format("delta")
  .option("checkpointLocation", bronzePath + "/_checkpoint")
  .queryName(streamName)
  .outputMode("append")
  .start(bronzePath))
}

// COMMAND ----------

dbutils.fs.rm(outputPathBronze + "/_checkpoint",true)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Call your writeToBronze function
// MAGIC 
// MAGIC To start the stream, call your `writeToBronze` function in the cell below.

// COMMAND ----------

writeToBronze(gamingEventDF, outputPathBronze, "bronze_stream")

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Check your answer 
// MAGIC 
// MAGIC Call the realityCheckBronze function with your writeToBronze function as an argument.

// COMMAND ----------

realityCheckBronze(writeToBronze)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step 3a: Load static data for enrichment
// MAGIC 
// MAGIC Complete the `loadStaticData` function to perform the following tasks:
// MAGIC 
// MAGIC * Register a static lookup table to associate `deviceId` with `deviceType` (android or ios).
// MAGIC * While we refer to this as a lookup table, here we'll define it as a DataFrame. This will make it easier for us to define a join on our streaming data in the next step.
// MAGIC * Create `deviceLookupDF` by calling your loadStaticData function, passing `/mnt/training/gaming_data/dimensionData` as the path.

// COMMAND ----------

val correctLookupDF = (spark.read
    .format("delta")
    .load("/mnt/training/enb/capstone-data/lookup"))
correctLookupDF.show

// COMMAND ----------

// TODO
val lookupPath = "/mnt/training/gaming_data/dimensionData"
//
def loadStaticData(path: String): org.apache.spark.sql.DataFrame = {
  val lookup = spark.read
    .format("delta")
    .load(path)
    .dropDuplicates()
  
  return lookup
}
  
val deviceLookupDF = loadStaticData(lookupPath)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ##Check your answer
// MAGIC 
// MAGIC Call the reaityCheckStatic function, passing your loadStaticData function as an argument. 

// COMMAND ----------

realityCheckStatic(loadStaticData)

// COMMAND ----------

// %md-sandbox
// ### Step 3b: Create a streaming silver Delta table
// 
// A silver table is a table that combines, improves, or enriches bronze data. 
// 
// In this case we will join the bronze streaming data with some static data to add useful information. 
// 
// #### Steps to complete
// 
// Complete the `bronzeToSilver` function to perform the following tasks:
// * Create a new stream by joining `deviceLookupDF` with the bronze table stored at `outputPathBronze` on `deviceId`.
// * Make sure you do a streaming read and write
// * Your selected fields should be:
//   - `device_id`
//   - `eventName`
//   - `client_event_time`
//   - `eventDate`
//   - `deviceType`
// * **NOTE**: some of these fields are nested; alias them to end up with a flat schema
// * Write to `outputPathSilver`
// 
//  Don't forget to checkpoint your stream!

// COMMAND ----------

// TODO

def bronzeToSilver(bronzePath: String, silverPath: String, streamName: String, lookupDF: org.apache.spark.sql.DataFrame): Unit = {
  var bronzeDF = spark.readStream
  .format("delta")
  .load(bronzePath)
  
  var joinDF = bronzeDF.join(lookupDF,bronzeDF("eventParams.device_id")===lookupDF("device_id"),"inner")
  joinDF=joinDF.withColumn("eventParams",$"eventParams".dropFields("device_id"))
  def flattenStructSchema(schema: StructType, prefix: String = null) : Array[Column] = {
    schema.fields.flatMap(f => {
      val columnName = if (prefix == null) f.name else (prefix + "." + f.name)
 
      f.dataType match {
        case st: StructType => flattenStructSchema(st, columnName)
        case _ => Array(col(columnName))
      }
    })
  }
 
  joinDF = joinDF.select(flattenStructSchema(joinDF.schema):_*)
  joinDF=joinDF.select("device_id","eventName","client_event_time","eventDate","deviceType")
  //joinDF.printSchema
   
joinDF
  .writeStream 
  .format("delta")
  .option("checkpointLocation",silverPath + "/_checkpoint")
  .queryName(streamName)
  .outputMode("append")
  .start(silverPath)
}

/*.writeStream
  .format("delta")
  .option("checkpointLocation", bronzePath + "/_checkpoint")
  .queryName(streamName)
  .outputMode("append")
  .start(bronzePath))*/

// COMMAND ----------

// %md
// 
// ## Call your bronzeToSilver function
// 
// To start the stream, call your `bronzeToSilver` function in the cell below.

// COMMAND ----------

bronzeToSilver(outputPathBronze, outputPathSilver, "silver_stream", deviceLookupDF)

// COMMAND ----------

// %md
// 
// ## Check your answer 
// 
// Call the realityCheckSilver function with your bronzeToSilver function as an argument.

// COMMAND ----------

realityCheckSilver(bronzeToSilver)

// COMMAND ----------

// MAGIC ### Step 4a: Batch Process a Gold Table from the Silver Table
// MAGIC 
// MAGIC The company executives want to look at the number of **distinct** active users by week. They use SQL so our target will be a SQL table backed by a Delta Lake. 
// MAGIC 
// MAGIC The table should have the following columns:
// MAGIC - `WAU`: count of weekly active users (distinct device IDs grouped by week)
// MAGIC - `week`: week of year (the appropriate SQL function has been imported for you)
// MAGIC 
// MAGIC  There are *at least* two ways to successfully calculate weekly average users on streaming data. If you choose to use `approx_count_distinct`, note that the optional keyword `rsd` will need to be set to `.01` to pass the final check `Returns the correct DataFrame`.

// COMMAND ----------

// TODO
import org.apache.spark.sql.functions._

def silverToGold(silverPath: String, goldPath: String, queryName:String): Unit = {
  var silverDF = spark.readStream
  .format("delta")
  .load(silverPath)
  .withColumn("week",weekofyear($"eventDate"))
  
  silverDF=silverDF.groupBy("week").agg(approx_count_distinct("device_id",0.01).alias("WAU"))
  
  silverDF
  .writeStream
  .format("delta")
  .option("checkpointLocation", goldPath + "/_checkpoint")
  .queryName(queryName)
  .outputMode("complete")
  .start(goldPath)
}

// COMMAND ----------

// %md
// 
// ## Call your silverToGold function
// 
// To start the stream, call your `silverToGold` function in the cell below.

// COMMAND ----------

silverToGold(outputPathSilver, outputPathGold, "gold_stream")

// COMMAND ----------

// %md
// 
// ##Check your answer
// 
// Call the reaityCheckGold function, passing your silverToGold function as an argument. 

// COMMAND ----------

realityCheckGold(silverToGold)

// COMMAND ----------

// MAGIC %md-sandbox
// MAGIC 
// MAGIC ### Step 4b: Register Gold SQL Table
// MAGIC 
// MAGIC By linking the Spark SQL table with the Delta Lake file path, we will always get results from the most current valid version of the streaming table.
// MAGIC 
// MAGIC It may take some time for the previous streaming operations to start. 
// MAGIC 
// MAGIC Once they have started register a SQL table against the gold Delta Lake path. 
// MAGIC 
// MAGIC * tablename: `mobile_events_delta_gold`
// MAGIC * table Location: `outputPathGold`

// COMMAND ----------

// TODO
spark.sql(s"""
   CREATE TABLE IF NOT EXISTS mobile_events_delta_gold
   USING DELTA 
   LOCATION '$outputPathGold' """)

// COMMAND ----------

// %md-sandbox
// ### Step 4c: Visualization
// 
// The company executives are visual people: they like pretty charts.
// 
// Create a bar chart out of `mobile_events_delta_gold` where the horizontal axis is month and the vertical axis is WAU.


//  order by `week` to seek time-based patterns.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM mobile_events_delta_gold order by week

// COMMAND ----------

// %md
// ### Step 5: Wrap-up
// 
// * Stop streams

// COMMAND ----------

for (s <- spark.streams.active)
  s.stop()
