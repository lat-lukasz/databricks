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
# Typ SCD
dbutils.widgets.dropdown("param_scd_type", "scd1", ["scd1", "scd2"])
param_scd_type = dbutils.widgets.get("param_scd_type")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Funkcji/Bibliotek

# COMMAND ----------

from pyspark.sql.functions import  current_date, expr, col, lit, hash, regexp_extract
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definicja zmiennych

# COMMAND ----------

source_path = f"bronze.{schema}.{param_source_name}"
destination_path = f"silver.{schema}.{param_source_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definicja Funkcji

# COMMAND ----------

def merge_silver_SCD1(df):
    
    # Dodaję hash
    df_bronze = df.dropDuplicates()
    df_bronze_delta = (df_bronze
                    .withColumn("hash_value", hash(*df_bronze.columns))
                    .withColumn("rating_value",regexp_extract('rating', r'(\d+\.?\d*)', 1).cast("double"))
                    .withColumn("item_weight",regexp_extract('item_weight', r'(\d+\.?\d*)', 1).cast("double")))

    # Dodaje kolumny techniczne
    df_bronze_delta = (df_bronze_delta
        .withColumn("inserted_at", F.lit(F.current_timestamp()))
        .withColumn("updated_at", F.lit(F.current_timestamp()))
        .withColumn("scd_is_current", F.lit(True))
        .withColumn("scd_start_date", F.lit(current_date()))
        .withColumn("scd_end_date", F.lit('9999-12-31')))
    
    if DeltaTable.isDeltaTable(spark, destination_path):

        target_table = DeltaTable.forPath(spark, destination_path)
        
        (target_table.alias("target")
            .merge(df_bronze_delta.alias("source"), "source.hash_value = target.hash_value")
            .whenMatchedUpdateAll(condition = "target.hash_value <> source.hash_value")
            .whenNotMatchedInsertAll()
            .execute())
    else:
        df_bronze_delta.write.format("delta").mode("overwrite").save(destination_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### [Funkcje hashujące](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.hash.html#)
# MAGIC
# MAGIC

# COMMAND ----------

# df_bronze = DeltaTable.forName(spark, source_path).toDF()
# df_bronze = df_bronze.withColumn("hash_value", hash(*df_bronze.columns))
# df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logika wywołania funkcji

# COMMAND ----------

try:

    if(param_scd_type == "scd1"):
        df_bronze = DeltaTable.forPath(spark, source_path).toDF()
        merge_silver_SCD1(df_bronze)
    
except Exception as e:
    print(e)
