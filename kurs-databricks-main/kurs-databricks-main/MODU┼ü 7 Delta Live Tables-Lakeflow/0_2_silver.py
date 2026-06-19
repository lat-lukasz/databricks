# Databricks notebook source
# MAGIC %md
# MAGIC #### Import DLT Wymagane biblioteki
# MAGIC
# MAGIC

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_date, expr, col, lit, hash, regexp_extract, current_timestamp
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametry

# COMMAND ----------

param_environment = spark.conf.get("param_environment", "dev")
param_source_name = spark.conf.get("param_source_name", "")
schema = spark.conf.get("schema", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funkcje Delta Live Tables
# MAGIC
# MAGIC Funkcja dekorowana `@dlt.table` tworzy w potoku **Delta Live Tables** (DLT) tabelę warstwy *silver* o nazwie `silver.<schema>.<param_source_name>`.
# MAGIC
# MAGIC 1. `@dlt.expect` Expectations do kontroli jakości
# MAGIC 2. `bronze_source()`
# MAGIC    - Odczyt danych z warstwy *bronze*  
# MAGIC    - df_bronze = dlt.read_stream(f"bronze.{schema}.{param_source_name}") <br>
# MAGIC    - dodatnie kolumn technicznych
# MAGIC 3. `create_streaming_table()`
# MAGIC    - Tworzy tabelę docelową jeśli jej nie ma
# MAGIC 4. `create_auto_cdc_flow()`
# MAGIC    - Mechanizm odpowiedzialny za aktualizację tabeli docelowej, insert i updates oraz deletes
# MAGIC    - Zawiera mechanizm Slowly Changing Dimentions `stored_as_scd_type = 1`
# MAGIC

# COMMAND ----------

# 1: Tworzę widok
@dlt.view
@dlt.expect("valid_rating", "rating_value >= 0 AND rating_value <= 5")
@dlt.expect("non_null_item_weight_or_drop", "item_weight IS NOT NULL")
@dlt.expect("non_null_hash", "hash_value IS NOT NULL")
@dlt.expect("valid_inserted_at", "inserted_at IS NOT NULL")

def silver_books():
    df_bronze = dlt.read_stream(f"bronze.{schema}.{param_source_name}")

    return (
        df_bronze
        .dropDuplicates()
        .withColumn(
            "hash_value",
            hash(*[col(c) for c in df_bronze.columns if c != '_rescued_data'])
        )
        .withColumn(
            "rating_value",
            regexp_extract("rating", r"(\d+\.?\d*)", 1).cast("double")
        )
        .withColumn(
            "item_weight",
            regexp_extract("item_weight", r"(\d+\.?\d*)", 1).cast("double")
        )
        .withColumn("inserted_at", current_timestamp())
        .withColumn("updated_at", current_timestamp())
        .withColumn("scd_is_current", lit(True))
        .withColumn("scd_start_date", current_date())
        .withColumn("scd_end_date", lit("9999-12-31"))
    )

# 2: Deklaracja tabeli silver
dlt.create_streaming_table(
    name = f"silver.{schema}.{param_source_name}"
)

# 3: Logika CDC(SCD Type 1)
dlt.create_auto_cdc_flow(
    source = "silver_books", 
    target = f"silver.{schema}.{param_source_name}",
    keys = ["hash_value"],
    sequence_by = col("inserted_at"),
    apply_as_deletes = expr("false"),
    stored_as_scd_type = 1
)

