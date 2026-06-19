# Databricks notebook source
# MAGIC %md
# MAGIC # Podstawy procesu DLT
# MAGIC
# MAGIC Notatnik Delta Live Tables (DLT) przetwarza pliki źródłowe JSON i używa funkcjonalnośći autoloadera, którą znasz z poprzednich etapów kursu. 
# MAGIC
# MAGIC * Ten notatnik to  piersza część medalionu zasilająca warstwę bonze. Zawiera dane nie przetworzone.
# MAGIC
# MAGIC
# MAGIC Celem notanika jest:
# MAGIC * Definicja Delta Live Tables
# MAGIC * Ładowanie danych z Auto Loader
# MAGIC * Użycie parametrów DLT Pipelines
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notatnik DLT
# MAGIC
# MAGIC Żeby uruchiomić DLT musisz mieć nowszy runtime, od 16.4. ⚠ Uwaga serverless nie uruchomi DLT.
# MAGIC
# MAGIC ## Parametry
# MAGIC
# MAGIC Podczas tworzenia pipeline możesz podać paramety. Mają one postać słownika czyli klucz-wartość. Znajdują się one w sekcji Advance > Configuration
# MAGIC
# MAGIC Ciekawostka te parametry są ustawione jako konfiguracje Spark.
# MAGIC
# MAGIC W Python, możesz je podejżeć używając **`spark.conf.get()`**.
# MAGIC
# MAGIC ## Imports
# MAGIC
# MAGIC Moduł **`dlt`** musi zostać zaimportowany do notatnika.

# COMMAND ----------

# import dlt
from pyspark import pipelines as dp
from pyspark.sql.functions import lit, col, current_timestamp

# COMMAND ----------

param_environment = spark.conf.get("param_environment", "dev")
param_storage = spark.conf.get("param_storage", "sadevtraining")
param_source_name = spark.conf.get("param_source_name", "")
schema = spark.conf.get("schema", "")

# COMMAND ----------

# To mogłoby być w Volumes, i ścieżka powinna być sparametryzowana
errors_path = f"abfss://raw@{param_storage}.dfs.core.windows.net/errors"
checkpoint_path = f"abfss://raw@{param_storage}.dfs.core.windows.net/checkpoint"
source_path = f"abfss://raw@{param_storage}.dfs.core.windows.net/Dane"

# COMMAND ----------

# Definicja tabeli
@dp.table(
    name=f"bronze.{schema}.{param_source_name}", 
    comment="Raw data ingested from cloud storage"
)
# Autoloader
def load_raw_files():
    cloudfile = {
        "cloudFiles.format": "json",
        "pathGlobFilter": "*.json",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaLocation": checkpoint_path
    }
    return (
        spark.readStream.format("cloudFiles")
            .options(**cloudfile)
            .option("checkpointLocation", checkpoint_path)
            .option("badRecordsPath", errors_path)
            .option("multiline", True)
            .load(source_path)
            .selectExpr("*", "_metadata")
            .withColumn("source_system", lit(param_source_name))
            .withColumn("file_path", col("_metadata.file_path"))
            .withColumn("inserted_at", lit(current_timestamp()))
    )
