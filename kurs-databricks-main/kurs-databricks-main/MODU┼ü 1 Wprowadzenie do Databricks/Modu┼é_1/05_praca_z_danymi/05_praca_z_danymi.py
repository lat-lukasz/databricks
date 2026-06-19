# Databricks notebook source
# MAGIC %md
# MAGIC # 1. Praca z danymi w dbfs
# MAGIC
# MAGIC Databricks udostępnia dane typu open source, każdy workspace ma dostęp do danych w dbfs (databricks file system).
# MAGIC
# MAGIC ##### Zadania
# MAGIC 1. Przeglądamy dane urzywając dbutils
# MAGIC 1. Załadujemy dane do Dataframe 
# MAGIC 1. Zapisz dane do tabel
# MAGIC 1. Wizualizacja danych przy użyciu SQL

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/songs/data-001"))

# COMMAND ----------

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField


schema = StructType(
  [
    StructField("artist_id", StringType(), True),
    StructField("artist_lat", DoubleType(), True),
    StructField("artist_long", DoubleType(), True),
    StructField("artist_location", StringType(), True),
    StructField("artist_name", StringType(), True),
    StructField("duration", DoubleType(), True),
    StructField("end_of_fade_in", DoubleType(), True),
    StructField("key", IntegerType(), True),
    StructField("key_confidence", DoubleType(), True),
    StructField("loudness", DoubleType(), True),
    StructField("release", StringType(), True),
    StructField("song_hotnes", DoubleType(), True),
    StructField("song_id", StringType(), True),
    StructField("start_of_fade_out", DoubleType(), True),
    StructField("tempo", DoubleType(), True),
    StructField("time_signature", DoubleType(), True),
    StructField("time_signature_confidence", DoubleType(), True),
    StructField("title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("partial_sequence", IntegerType(), True)
  ]
)

df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .option("delimiter", "\t") \
    .load("dbfs:/databricks-datasets/songs/data-001")


# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Stwórz table przy użyciu sql

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN default;
# MAGIC

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("default.songs")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.songs
