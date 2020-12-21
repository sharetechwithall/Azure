// Databricks notebook source
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import com.microsoft.azure.cosmosdb.spark.streaming._
import org.apache.spark.sql.types.{StructType, StructField, LongType, StringType, TimestampType, DateType}
import org.apache.spark.sql.functions._

// COMMAND ----------


print(sc.defaultParallelism)
spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)
print("partitions are : ", spark.conf.get("spark.sql.shuffle.partitions"))

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://databrickscontainer@mystoragedatabricks.blob.core.windows.net",
  mountPoint = "/mnt/mymount",
  extraConfigs = Map("fs.azure.account.key.mystoragedatabricks.blob.core.windows.net" -> dbutils.secrets.get(scope = "blobscope", key = "azure-blob-key")))

// COMMAND ----------

var cosmosConfig = Map(
"Endpoint" -> "https://mycosmosacc2020.documents.azure.com:443/",
"Database" -> "cosmosdb",
"Collection" -> "cont001",
"MasterKey" -> dbutils.secrets.get(scope= "cosmosscope", key = "cosmosdb" ),
"ConnectionMode" -> "Gateway",
"ChangeFeedCheckpointLocation" -> "/mnt/mymount/cosmoscheck",
"changefeedqueryname" -> "Cosmos DB Feed Query")

// COMMAND ----------

var cosmosStream = spark.readStream.format(classOf[CosmosDBSourceProvider].getName).options(cosmosConfig).load()

// COMMAND ----------

val cosmosStreamOut = cosmosStream
  .withColumn("SplitID",$"orderSplitID".cast(LongType))  
  .withColumn("SaleDate",to_date($"OrderSalesDate","yyyy-MM-dd").cast(DateType))  
  .withColumn("SaleTime",to_timestamp($"OrderSalesDate","yyyy-MM-dd'T'HH:mm:ss").cast(TimestampType)) 
  .withColumn("EnqueuedTime", $"IotHub.EnqueuedTime".cast(TimestampType))  
  .withColumn("LoadTime",current_timestamp)  
  .select("SplitID","firstName","lastName","Product","SaleDate","SaleTime","EnqueuedTime","LoadTime")                   
  .writeStream                                                                  
  .format("delta")                                                              
  .partitionBy("Product")                                                         
  .option("checkpointLocation", "/mnt/mymount/checkpoint/salesSplit.checkpoint")      
  .option("path", "/mnt/mymount/salesSplitdelta")                               
  .outputMode("append")                                                         
  .start()

// COMMAND ----------

display(sql("select * from delta.`/mnt/mymount/salesSplitdelta`"))

// COMMAND ----------

dbutils.fs.ls("/mmt/mymount/salesSplitdelta")

// COMMAND ----------


