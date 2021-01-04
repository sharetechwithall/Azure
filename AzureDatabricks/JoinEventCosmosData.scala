// Databricks notebook source
// cosmos libs
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming._

// spark libs
import org.apache.spark.sql.types.{StructType, StructField, LongType, StringType, TimestampType, DateType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

// eventhub libs
import org.apache.spark.eventhubs._




// COMMAND ----------

print(sc.defaultParallelism)
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
print("partitions are : ", spark.conf.get("spark.sql.shuffle.partitions"))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<Blobcontainername>@<storageaccname>.blob.core.windows.net",
  mountPoint = "/mnt/mymount",
  extraConfigs = Map("fs.azure.account.key.mystoragedatabricks.blob.core.windows.net" -> dbutils.secrets.get(scope = "blobscope", key = "azure-blob-key")))

// COMMAND ----------

var cosmosConfig = Map(
"Endpoint" -> "https://<yourcosmosaccName>.documents.azure.com:443/",
"Database" -> "cosmosdb",
"Collection" -> "cont001",
"MasterKey" -> dbutils.secrets.get(scope= "cosmosscope", key = "azure-cosmos-key" ),
"ConnectionMode" -> "Gateway",
"ChangeFeedCheckpointLocation" -> "/mnt/mymount/cosmoscheck",
"changefeedqueryname" -> "Cosmos DB Feed Query")

// COMMAND ----------

var cosmosStream = spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(cosmosConfig).load()

// COMMAND ----------

val eventHubOrders = ConnectionStringBuilder("Endpoint=sb://<eventhubNamespaceName>.servicebus.windows.net/;SharedAccessKeyName=azureeventpolicy;SharedAccessKey="+dbutils.secrets.get(scope = "eventscope", key = "azure-event-key")).setEventHubName("myfirsteventhub").build

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
    StructField("region", StringType, false),
    StructField("createdDate", StringType, false)
))

// COMMAND ----------

val cosmosStreamOut = cosmosStream
  .withColumn("SplitID",$"orderSplitID".cast(LongType))  
  .withColumn("SaleDate",to_date($"OrderSalesDate","yyyy-MM-dd").cast(DateType))  
  .withColumn("SaleTime",to_timestamp($"OrderSalesDate","yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType)) 
  .withColumn("EnqueuedTime", $"IotHub.EnqueuedTime".cast(TimestampType))  
  .withColumn("LoadTime",current_timestamp)  
  .withColumn("orderSalesID",$"orderID".cast(LongType)) 
  .select("SplitID","orderSalesID","firstName","lastName","Product","SaleDate","SaleTime","EnqueuedTime","LoadTime")                   
  .writeStream                                                                  
  .format("delta")                                                              
  .partitionBy("Product")                                                         
  .option("checkpointLocation", "/mnt/mymount/checkpoint/salesSplit.checkpoint")      
  .option("path", "/mnt/mymount/salesSplitdelta")                               
  .outputMode("append")                                                         
  .start()

// COMMAND ----------

val eventStreamOrderOut = eventStreamOrders
  .withColumn("messageBody", from_json($"body".cast(StringType), eventOrderSchema)) 
  .select("messageBody.*")
  .withColumn("createdDateNew",to_timestamp($"createdDate","yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType))
  .writeStream                                                           
  .format("delta")                                                      
  .partitionBy("region")                        
  .option("checkpointLocation", "/mnt/mymount/checkpoint/Orders.checkpoint")  
  .option("path", "/mnt/mymount/fact/Orders")                          
  .outputMode("append")                                                                        
  .start()

// COMMAND ----------

val ordersDF = spark.readStream
  .format("delta")
  .load("/mnt/mymount/fact/Orders")
  .withColumn("ordersProcessingTime",current_timestamp) // append time column to timestamp this join process

val orderSplitDF = spark.readStream
  .format("delta")
  .load("/mnt/mymount/salesSplitdelta")
  .withColumn("orderSplitProcessingTime",current_timestamp) // append time column to timestamp this join process


// COMMAND ----------

val ordersWatermark = ordersDF.withWatermark("createdDateNew","1 hour")
val orderSplitsWatermark = orderSplitDF.withWatermark("SaleTime","2 hours")

// COMMAND ----------

val joinedOrderFact = ordersWatermark.join(
  orderSplitsWatermark,
  expr("""orderID = orderSalesID and 
      SaleTime >= createdDateNew """))

// COMMAND ----------

val factOrders = joinedOrderFact.writeStream                     
  .format("delta")                                                       
  .partitionBy("Product")                        
  .option("checkpointLocation", "/mnt/mymount/checkpoint/factOrders.checkpoint")  
  .option("path", "/mnt/mymount/factOrders")                          
  .outputMode("append")                                                  
  .trigger(Trigger.ProcessingTime("60 seconds"))                         
  .start()  

// COMMAND ----------

val factorders = "/mnt/mymount/factOrders"
spark.sql(s"""
CREATE TABLE IF NOT EXISTS factorderSQL
  USING DELTA 
  OPTIONS (path = "$factorders")
  """)


// COMMAND ----------

display(spark.sql("""select * from factorderSQL """))

// COMMAND ----------


