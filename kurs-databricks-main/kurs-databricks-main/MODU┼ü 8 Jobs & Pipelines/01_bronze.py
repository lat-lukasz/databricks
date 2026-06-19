# Databricks notebook source
# MAGIC %md
# MAGIC ### Parametry

# COMMAND ----------

# Schemat
dbutils.widgets.text("schema", "dbo")
schema = dbutils.widgets.get("schema")
# Źródło danych
dbutils.widgets.text("param_source_name",defaultValue="books")
param_source_name = dbutils.widgets.get("param_source_name")
# Typ pliku
dbutils.widgets.text("param_file_type",defaultValue="json")
param_file_type = dbutils.widgets.get("param_file_type")
# Typ pliku
dbutils.widgets.text("param_storage",defaultValue="sadevtraining")
param_storage = dbutils.widgets.get("param_storage")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Funkcji/Bibliotek

# COMMAND ----------

from pyspark.sql.functions import lit, col, current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definicja zmiennych

# COMMAND ----------

# Ścieżka dla plików
errors_path = f"abfss://raw@{param_storage}.dfs.core.windows.net/errors"
checkpoint_path = f"abfss://raw@{param_storage}.dfs.core.windows.net/checkpoint"
source_path = f"abfss://raw@{param_storage}.dfs.core.windows.net/Dane"
destination_path = f"bronze.{schema}.{param_source_name}"
cloudfile = dict(ignoreCorruptFiles=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### cloudFile konfiguracje dla Autoloadera

# COMMAND ----------

if param_file_type == 'json':
    cloudfile["cloudFiles.format"] = "json"
    cloudfile["pathGlobFilter"] = "*.json"
    cloudfile["cloudFiles.inferColumnTypes"] = "true"

cloudfile["cloudFiles.schemaLocation"] = checkpoint_path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Stream Autoloader

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
                .withColumn("source_system", lit(param_source_name))
                .withColumn("file_path", col("_metadata.file_path"))
                .withColumn("inserted_at", lit(current_timestamp()))
    )
except Exception as e:
    raise Exception(f"Nb failed. Error: {str(e)}")

# COMMAND ----------

# df_raw_files.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Zapis Streamu Autoloader

# COMMAND ----------

app_id = f"hive_metastore_{param_source_name}"
def write_data(df, batch_id):
    df_clean = df.drop("_rescued_data","_metadata")
    
    (df_clean
        .write
        .option("txnAppId", app_id)
        .option("txnVersion", batch_id)
        .mode("append")
        .saveAsTable(destination_path))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Autoloader
# MAGIC [Autoloader](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/)
# MAGIC ### Spark Structure Streaming
# MAGIC [Autoloader](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#programming-model)
# MAGIC
# MAGIC ### Java Deamon Threads
# MAGIC [Deamon Threads](https://www.baeldung.com/java-daemon-thread#differences-between-daemon-and-user-threads)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wywołanie funkcji zapisu Autoloader

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

# dbutils.fs.rm(errors_path,True)
# dbutils.fs.rm(checkpoint_path,True)
# dbutils.fs.rm(destination_path,True)
# dbutils.fs.rm('/FileStore/Raw/dev/books/',True)
