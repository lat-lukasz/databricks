# Databricks notebook source
# MAGIC %md
# MAGIC #### Wersjonowanie danych i Time Travel
# MAGIC

# COMMAND ----------

from delta import DeltaTable

# COMMAND ----------

bronze_delta_path = f'/Volumes/workspace/default/delta'

# COMMAND ----------

# data = [("1.0", "pierwszy wpis")]
# data = [("2.0", "drugi wpis")]
# data = [("3.0", "trzeci wpis")]
data = [("4.0", "czwarty wpis")]
columns = ["Wersja", "Komentarz"]
df_sample = spark.createDataFrame(data, columns)
df_sample.write.format("delta").mode("append").save(bronze_delta_path)
display(df_sample)

# COMMAND ----------

delta_table = DeltaTable.forPath(spark, bronze_delta_path)
delta_table.delete("Wersja = '3.0'")

# COMMAND ----------

# MAGIC %md
# MAGIC Każde uruchomienie zapisu kiedy dodawałeś/łaś dane do tabel bronze, silver czy gold powoduje stwożenie nowej wersji tabeli. Informacje te są przetrzymywane w plikach metadanych<br>
# MAGIC W każdej chwili możesz sprawdzić wszystkie wersje i wrocić do wybranej.
# MAGIC
# MAGIC #### DESCRIBE HISTORY (nazwa tabeli)

# COMMAND ----------

bronze_books_df = spark.sql(f"DESCRIBE HISTORY '{bronze_delta_path}'")
bronze_books_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### PODRÓŻ W CZASIE
# MAGIC
# MAGIC Chcesz sprawdzić jak wyglądała tabla dwie wersje temu?<br>
# MAGIC Są dwie metody na podejżenie poprzedniej wersji (Timestamp) lub (numer wersji)<br>
# MAGIC Skoro nie pracuję z tabelami w Unity Catalog użyję spark API żeby wczytać dane z konkretnej wersji
# MAGIC

# COMMAND ----------

df = spark.read.format("delta").option("versionAsOf", 3).load(bronze_delta_path)
display(df)

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2025-09-12T06:19:42.000+00:00").load(bronze_delta_path)
display(df)

# COMMAND ----------

delta_table = DeltaTable.forPath(spark, f"{bronze_delta_path}")
delta_table.delete()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sprawdź tabelę
# MAGIC Sprawdź czy udalo sie przywrócić poprzednia wersję

# COMMAND ----------

spark.read.format("delta").load(bronze_delta_path).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RESTORE TABLE 
# MAGIC Skoro już wiesz gdzie są dane to teraz przywrócić wersję nr 3 <br>
# MAGIC Są dwie opcje restore, przez SQL lub operacje na Delta API, skoro nie mam porządnej tabeli w Unity lub Hive to użyję Delta API <br>
# MAGIC
# MAGIC `RESTORE TABLE <table_name> VERSION AS OF <number>`

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, bronze_delta_path)
deltaTable.restoreToVersion(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Metadane
# MAGIC Każda tabela delta ma swoje metadane, czyli pełny log informacji.<br> 
# MAGIC Podejrzyj metadane może jest tam coś ciekawego.

# COMMAND ----------

display(dbutils.fs.ls(bronze_delta_path + "/_delta_log"))

# COMMAND ----------

spark.read.json(f'{bronze_delta_path}/_delta_log/00000000000000000002.json').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Metadane możesz również podejrzeć przez SQL

# COMMAND ----------

spark.sql(f"DESCRIBE DETAIL '{bronze_delta_path}'").display()

# COMMAND ----------

# dbutils.fs.rm(bronze_delta_path, True)
