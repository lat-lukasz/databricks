# Databricks notebook source
# MAGIC %md
# MAGIC ## Zmiana schematu
# MAGIC Dostałeś wymagania od firmy trzeba zmienić tabele
# MAGIC - Dodanie nowej kolumny
# MAGIC - Usunięcie kolumny 
# MAGIC

# COMMAND ----------

# Źródło danych
dbutils.widgets.text("source_name",defaultValue="books")
source_name = dbutils.widgets.get("source_name")
# Schema
dbutils.widgets.text("schema",defaultValue="dbo")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql.functions import lit
from delta import DeltaTable

# COMMAND ----------

gold_all_books = f'gold.{schema}.all_{source_name}'

# COMMAND ----------

# MAGIC %md
# MAGIC Pracując z tabelą w unity czy hive możesz użyc SQL do zmiany schematu.<br>
# MAGIC My teraz popracujemy w pysparku a reszte operacji będziemy wykonywać juz na Unity Catalog

# COMMAND ----------

df = DeltaTable.forName(spark,gold_all_books).toDF()

df = df.drop("Discount")
df_with_new_column = df.withColumn("Promotion", lit(10))

#Chce dodać kolumne w wybranej 
columns = df.columns
column_index = columns.index('Initial_Price') + 1

new_column_order = columns[:column_index] + ["Promotion"] + columns[column_index:]

# Select columns in new order
df_reordered = df_with_new_column.select(*new_column_order)

(df_reordered.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_all_books))

# COMMAND ----------

spark.sql(f"SELECT * FROM {gold_all_books}").display()

# COMMAND ----------

spark.sql(f"SELECT * FROM {gold_all_books}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dokumentacja
# MAGIC Dodatkowe materiały warte twojej uwagi.<br>
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema
