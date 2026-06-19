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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import Funkcji/Bibliotek

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, current_timestamp, lit, hash
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definicja zmiennych

# COMMAND ----------

silver_path = f"silver.{schema}.{param_source_name}"
gold_path_all_books = f"gold.{schema}.all_{param_source_name}"
gold_path_best_books = f"gold.{schema}.best_{param_source_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definicja Funkcji

# COMMAND ----------

def get_all_books(df):
    df_all_books = (df
        .select(col("ISBN10").alias("International_Standard_Book_Number"),
                col("asin").alias("Amazon_Standard_Identification_Number"),
                col("title").alias("Title"),
                col("brand").alias("Author"),
                col("availability").alias("Availability"),
                col("currency").alias("Currency"),
                col("discount").alias("Discount"),
                col("final_price").alias("Final_Price"),
                col("initial_price").alias("Initial_Price"),
                col("rating_value").alias("Rating"),
                col("reviews_count").alias("Reviews_Count"),
                col("seller_name").alias("Seller_Name"),
                col("item_weight").alias("Item_Weight"))
        .filter("ISBN10 is not null")).dropDuplicates()
    return df_all_books

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definicja Funkcji

# COMMAND ----------

def get_best_books(df):
        df_best_books = (df
                .select(
                col("title").alias("Title"),
                col("brand").alias("Author"),
                col("rating_value").alias("Rating"),
                col("reviews_count").alias("Reviews_Count"),
                col("final_price").alias("Final_Price"))
                .filter("Rating >= 4.9")).dropDuplicates()
        return df_best_books

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definicja funkcji merge
# MAGIC Poznaj mechanism upsert czyli mechanizm merge do aktualizacji bądź dokładania nowych danych.
# MAGIC https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge

# COMMAND ----------

def merge_silver_to_gold(df_silver,destination_path):
    
    df_silver = (df_silver
        .withColumn("hash_value", hash(*df_silver.columns))
        .withColumn("inserted_at", lit(current_timestamp()))
        .withColumn("updated_at", lit(current_timestamp())))
    
    if DeltaTable.isDeltaTable(spark, destination_path):

        target_table = DeltaTable.forName(spark, destination_path)
        
        (target_table.alias("target")
            .merge(df_silver.alias("source"), "source.hash_value = target.hash_value")
            .whenMatchedUpdateAll(condition = "target.hash_value <> source.hash_value")
            .whenNotMatchedInsertAll()
            .execute())
    else:
        df_silver.write.format("delta").mode("overwrite").save(destination_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Logika wywołania funkcji

# COMMAND ----------

try:
    # Wczytuję tabele silver
    df_silver = DeltaTable.forName(spark, silver_path).toDF()
    # Ładowanie do tabeli Wszystkie Książki
    df_all_books = get_all_books(df_silver)
    merge_silver_to_gold(df_all_books,gold_path_all_books)
    # Ładowanie do tabeli Najlepsze Książki
    df_best_books = get_best_books(df_silver)
    merge_silver_to_gold(df_best_books,gold_path_best_books)
except Exception as e:
    print(e)

# COMMAND ----------

spark.sql(F"SELECT * FROM {gold_path_all_books}").display()

# COMMAND ----------

spark.sql(F"SELECT * FROM {gold_path_best_books}").display()

# COMMAND ----------

# cnt_all = spark.read.format("delta").load(gold_path_all_books).count()
# cnt_best = spark.read.format("delta").load(gold_path_best_books).count()
# print(cnt_all)
# print(cnt_best)
