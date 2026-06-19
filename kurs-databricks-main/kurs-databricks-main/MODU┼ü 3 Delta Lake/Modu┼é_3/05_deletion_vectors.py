# Databricks notebook source
# MAGIC %md
# MAGIC ## Deletion Vectors
# MAGIC Przy zapisie do dużych tabel możesz się zetknąć z problemami ... może bardzo wolno działać (wolny zapis)<br>
# MAGIC Najwolniejsze operacje to update, oraz delete (merge). Wymagają one przepisania całej tabeli co jest bardzo czasochłonne.<br>
# MAGIC Ale wymyślili rozwiązanie .....
# MAGIC [Deletion Vectors](https://learn.microsoft.com/en-us/azure/databricks/delta/deletion-vectors) <br>
# MAGIC tylko odznacza wiersze w plikach jako zmienione lub usunięte, ale przy zapisie ich nie przepisuje.<br>
# MAGIC
# MAGIC Zmiana są inicjowane przez 3 operacje
# MAGIC - OPTIMIZE
# MAGIC - Auto-compaction (spark.databricks.delta.autoCompact.enabled)
# MAGIC - REORG TABLE.... APPLY(PURGE)
# MAGIC

# COMMAND ----------

table_path = '/Volumes/workspace/default/optimize/'

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit, expr

df = (
    spark.range(90000000)
    .withColumn("number_col", col("id"))
    .withColumn("text_col", concat(lit("text_"), col("id")))
)

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path)

# COMMAND ----------

display(dbutils.fs.ls(table_path))

# COMMAND ----------

spark.sql(f"SELECT * FROM delta.`{table_path}`").display() 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sprawdź czy są włączone

# COMMAND ----------

table_properties = spark.sql(f"DESCRIBE DETAIL '{table_path}'")
display(table_properties)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Włączamy deletion vectors

# COMMAND ----------

spark.sql(f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")

# COMMAND ----------

spark.sql(f"DELETE FROM delta.`{table_path}` WHERE text_col = 'text_456'")

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY '{table_path}'").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE '/Volumes/workspace/default/optimize/'

# COMMAND ----------

# dbutils.fs.rm(table_path,True)
