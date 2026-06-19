# Databricks notebook source
# MAGIC %md
# MAGIC ### Parametry

# COMMAND ----------

dbutils.widgets.removeAll()
# Źródło danych
dbutils.widgets.text("source_name",defaultValue="")
source_name = dbutils.widgets.get("source_name")
# Typ pliku
dbutils.widgets.text("param_file_type",defaultValue="json")
param_file_type = dbutils.widgets.get("param_file_type")
# catalog
dbutils.widgets.text("catalog",defaultValue="bronze")
catalog = dbutils.widgets.get("catalog")
# schema
dbutils.widgets.text("schema",defaultValue="dbo")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql.functions import lit, col, current_timestamp

# COMMAND ----------

errors_path = f"/Volumes/workspace/default/lakehouse/meta/errors/{source_name}"
checkpoint_path = f"/Volumes/workspace/default/lakehouse/meta/checkpoints/{source_name}"
source_path = f"/Volumes/workspace/default/lakehouse/data/{source_name}"
destination_path = f"{catalog}.{schema}.{source_name}"
cloudfile = dict(ignoreCorruptFiles=True)

# COMMAND ----------

print(f"errors_path: {errors_path}")
print(f"checkpoint_path: {checkpoint_path}")
print(f"source_path: {source_path}")
print(f"destination_path: {destination_path}")

# COMMAND ----------

if param_file_type == 'json':
    cloudfile["cloudFiles.format"] = "json"
    cloudfile["pathGlobFilter"] = "*.json"
    cloudfile["cloudFiles.inferColumnTypes"] = "true"

cloudfile["cloudFiles.schemaLocation"] = checkpoint_path

# COMMAND ----------

try:
    df_raw_files = (
        spark.readStream.format("cloudFiles")
                .options(**cloudfile)
                .option("inferSchema",True)
                .option("checkpointLocation",checkpoint_path)
                .option("badRecordsPath", errors_path)
                .option("multiline", True)
                .load(source_path)
                .selectExpr("*", "_metadata")
                .withColumn("source_system", lit(source_name))
                .withColumn("file_path", col("_metadata.file_path"))
                .withColumn("inserted_at", lit(current_timestamp()))
    )
except Exception as e:
    raise Exception(f"Nb failed. Error: {str(e)}")

# COMMAND ----------

# display(df_raw_files, checkpointLocation="/Volumes/workspace/default/lakehouse/meta/checkpoints/books")

# COMMAND ----------

app_id = f"app_{source_name}"

def write_data(df, batch_id):
    df_clean = df.drop("_rescued_data","_metadata")
    
    (df_clean
        .write
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(destination_path))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader
# MAGIC [Autoloader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
# MAGIC ### Spark Structure Streaming
# MAGIC [Autoloader](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#programming-model)
# MAGIC
# MAGIC ### Java Deamon Threads
# MAGIC [Deamon Threads](https://www.baeldung.com/java-daemon-thread#differences-between-daemon-and-user-threads)

# COMMAND ----------

try:
    (
        df_raw_files.writeStream
            .format("delta")
            .trigger(availableNow=True)
            .outputMode("append")
            .option("checkpointLocation",checkpoint_path)
            .option("badRecordsPath", errors_path)
            .foreachBatch(write_data)
            .start()
            .awaitTermination()
    )
except Exception as e:
    raise Exception(f"Nb failed. Error: {str(e)}")

# COMMAND ----------

spark.sql(f"SELECT * FROM {destination_path}").count()

# COMMAND ----------

# dbutils.fs.rm(errors_path,True)
# dbutils.fs.rm(checkpoint_path,True)
