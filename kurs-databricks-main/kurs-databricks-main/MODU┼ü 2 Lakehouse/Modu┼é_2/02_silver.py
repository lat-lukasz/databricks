# Databricks notebook source
dbutils.widgets.removeAll()
# Źródło danych
dbutils.widgets.text("source_name",defaultValue="")
source_name = dbutils.widgets.get("source_name")
# Typ SCD
dbutils.widgets.dropdown("scd_type", "scd1", ["scd1", "scd2"])
scd_type = dbutils.widgets.get("scd_type")
# source catalog
dbutils.widgets.text("source_catalog",defaultValue="bronze")
source_catalog = dbutils.widgets.get("source_catalog")
# destination catalog
dbutils.widgets.text("destination_catalog",defaultValue="silver")
destination_catalog = dbutils.widgets.get("destination_catalog")
# schema
dbutils.widgets.text("schema",defaultValue="dbo")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql.functions import  current_date, expr, col, lit, hash, regexp_extract
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

source_path = f"{source_catalog}.{schema}.{source_name}"
destination_path = f"{destination_catalog}.{schema}.{source_name}"

# COMMAND ----------

print(f"source_path: {source_path}")
print(f"destination_path: {destination_path}")

# COMMAND ----------

def merge_silver_SCD1(df):
    
    # Dodaję hash
    df_bronze = df.dropDuplicates()
    columns_for_hash = df_bronze.columns
    df_bronze_delta = (df_bronze
                    .withColumn("source_hash", hash(*df_bronze.columns))
                    .withColumn("value_hash", hash(*[col for col in columns_for_hash if col != 'inserted_at']))
                    .withColumn("rating_value",regexp_extract('rating', r'(\d+\.?\d*)', 1).cast("double"))
                    .withColumn("item_weight",regexp_extract('item_weight', r'(\d+\.?\d*)', 1).cast("double")))

    # Dodaje kolumny techniczne
    df_bronze_delta = (df_bronze_delta
        .withColumn("inserted_at", F.lit(F.current_timestamp()))
        .withColumn("updated_at", F.lit(F.current_timestamp()))
        .withColumn("scd_is_current", F.lit(True))
        .withColumn("scd_start_date", F.lit(current_date()))
        .withColumn("scd_end_date", F.lit('9999-12-31')))
    
    if spark.catalog.tableExists(destination_path):

        target_table = DeltaTable.forName(spark, destination_path)
        
        (target_table.alias("target")
            .merge(df_bronze_delta.alias("source"), "source.source_hash = target.source_hash")
            .whenMatchedUpdateAll(condition = "target.value_hash <> source.value_hash")
            .whenNotMatchedInsertAll()
            .execute())
    else:
        df_bronze_delta.write.format("delta").mode("overwrite").saveAsTable(destination_path)

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

try:

    if(scd_type == "scd1"):
        df_bronze = DeltaTable.forName(spark, source_path).toDF()
        merge_silver_SCD1(df_bronze)
    
except Exception as e:
    print(e)

# COMMAND ----------

spark.sql(f"SELECT * FROM {destination_path}").display()
