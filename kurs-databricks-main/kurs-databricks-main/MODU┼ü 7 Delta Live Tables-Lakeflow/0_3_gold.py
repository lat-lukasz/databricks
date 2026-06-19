# Databricks notebook source
import dlt
from pyspark.sql.functions import col, hash, current_timestamp, lit, expr
from pyspark.sql import functions as F

# COMMAND ----------

param_environment = spark.conf.get("param_environment", "dev")
param_source_name = spark.conf.get("param_source_name", "")
schema = spark.conf.get("schema", "dbo")
param_scd_type = spark.conf.get("param_scd_type", "scd1")

# COMMAND ----------

@dlt.view
def all_books_source():
    df = dlt.read_stream(f"silver.{schema}.{param_source_name}")
    return (
        df.select(
            col("ISBN10").alias("International_Standard_Book_Number"),
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
            col("item_weight").alias("Item_Weight")
        )
        .filter("International_Standard_Book_Number is not null")
        .withColumn("inserted_at", current_timestamp())
    )

dlt.create_streaming_table(
    name=f"gold.{schema}.all_books"
)

dlt.create_auto_cdc_flow(
    target=f"gold.{schema}.all_books",
    source="all_books_source",
    keys=["International_Standard_Book_Number"],
    sequence_by=col("inserted_at"),
    apply_as_deletes="false",
    stored_as_scd_type=1
)

# COMMAND ----------

@dlt.view
def best_books_source():
    df = dlt.read_stream(f"silver.{schema}.{param_source_name}")
    return (
        df.select(
            col("title").alias("Title"),
            col("brand").alias("Author"),
            col("rating_value").alias("Rating"),
            col("reviews_count").alias("Reviews_Count"),
            col("final_price").alias("Final_Price")
        )
        .filter(col("rating_value") >= 4.9)
        .withColumn("inserted_at", current_timestamp())
    )

dlt.create_streaming_table(
    name=f"gold.{schema}.best_books"
)


dlt.create_auto_cdc_flow(
    target=f"gold.{schema}.best_books",
    source="best_books_source",
    keys=["Title","Author","Rating","Reviews_Count","Final_Price"],
    sequence_by=col("inserted_at"),
    apply_as_deletes="false",
    stored_as_scd_type=1
)
