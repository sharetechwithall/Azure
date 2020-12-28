// Databricks notebook source
import org.apache.spark.eventhubs._
import org.apache.spark.sql.types.{StructType, StructField, LongType, StringType, TimestampType, DateType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

// COMMAND ----------

val eventHubOrders = ConnectionStringBuilder("Endpoint=sb://eventhubnamespace2020.servicebus.windows.net/;SharedAccessKeyName=neweventpolicy;SharedAccessKey="+dbutils.secrets.get(scope = "eventscope", key = "event-key")).setEventHubName("myfirsteventhub").build

// COMMAND ----------

val startPostionEventHub = EventHubsConf(eventHubOrders).setStartingPosition(EventPosition.fromEndOfStream)

// COMMAND ----------

val eventStreamOrders = spark.readStream.option("multiline", "true")
  .format("eventhubs")
  .options(startPostionEventHub.toMap)
  .load()

// COMMAND ----------

val eventOrderSchema = StructType(List(
    StructField("orderID", LongType, false),
    StructField("orderDesc", StringType, false),
    StructField("region", StringType, false)
))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://databrickscontainer@mystoragedatabricks.blob.core.windows.net",
  mountPoint = "/mnt/mymount",
  extraConfigs = Map("fs.azure.account.key.mystoragedatabricks.blob.core.windows.net" -> dbutils.secrets.get(scope = "blobscope", key = "azure-blob-key")))

// COMMAND ----------

val eventStreamOrderOut = eventStreamOrders
  .withColumn("messageBody", from_json($"body".cast(StringType), eventOrderSchema)) 
  .select("messageBody.*")
  .writeStream                                                           
  .format("delta")                                                      
  .partitionBy("region")                        
  .option("checkpointLocation", "/mnt/mymount/checkpoint/Orders.checkpoint")  
  .option("path", "/mnt/mymount/fact/Orders")                          
  .outputMode("append")                                                                        
  .start()

// COMMAND ----------


