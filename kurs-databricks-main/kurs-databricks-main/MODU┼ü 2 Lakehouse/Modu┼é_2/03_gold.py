# Databricks notebook source
dbutils.widgets.removeAll()
# Źródło danych
dbutils.widgets.text("source_name",defaultValue="")
source_name = dbutils.widgets.get("source_name")
# source catalog
dbutils.widgets.text("source_catalog",defaultValue="silver")
source_catalog = dbutils.widgets.get("source_catalog")
# destination catalog
dbutils.widgets.text("destination_catalog",defaultValue="gold")
destination_catalog = dbutils.widgets.get("destination_catalog")
# schema
dbutils.widgets.text("schema",defaultValue="dbo")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, current_timestamp, lit, hash
from delta.tables import DeltaTable

# COMMAND ----------

source_path = f"{source_catalog}.{schema}.{source_name}"
destination_path_best_books = f"{destination_catalog}.{schema}.best_{source_name}"
destination_path_all_books = f"{destination_catalog}.{schema}.all_{source_name}"

# COMMAND ----------

print(f"source_path: {source_path}")
print(f"destination_path_best_books: {destination_path_best_books}")
print(f"destination_path_all_books: {destination_path_all_books}")

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
# MAGIC Poznaj mechanism upsert czyli mechanizm merge do aktualizacji bądź dokładania nowych danych.
# MAGIC https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge

# COMMAND ----------

def merge_silver_to_gold(df_silver,destination_path):
    
    columns_for_hash = df_silver.columns
    df_silver = (df_silver
        .withColumn("source_hash", hash(*df_silver.columns))
        .withColumn("value_hash", hash(*[col for col in columns_for_hash if col != 'inserted_at']))
        .withColumn("inserted_at", lit(current_timestamp()))
        .withColumn("updated_at", lit(current_timestamp())))
    
    if spark.catalog.tableExists(destination_path):

        target_table = DeltaTable.forName(spark, destination_path)
        
        (target_table.alias("target")
            .merge(df_silver.alias("source"), "source.source_hash = target.source_hash")
            .whenMatchedUpdateAll(condition = "target.value_hash <> source.value_hash")
            .whenNotMatchedInsertAll()
            .execute())
    else:
        df_silver.write.format("delta").mode("overwrite").saveAsTable(destination_path)

# COMMAND ----------

try:
    # Wczytuję tabele silver
    df_silver = DeltaTable.forName(spark, source_path).toDF()
    # Ładowanie do tabeli Wszystkie Książki
    df_all_books = get_all_books(df_silver)
    merge_silver_to_gold(df_all_books,destination_path_all_books)
    # Ładowanie do tabeli Najlepsze Książki
    df_best_books = get_best_books(df_silver)
    merge_silver_to_gold(df_best_books,destination_path_best_books)
except Exception as e:
    print(e)

# COMMAND ----------

spark.sql(f"SELECT * FROM {destination_path_all_books}").display()

# COMMAND ----------

spark.sql(f"SELECT * FROM {destination_path_best_books}").display()
