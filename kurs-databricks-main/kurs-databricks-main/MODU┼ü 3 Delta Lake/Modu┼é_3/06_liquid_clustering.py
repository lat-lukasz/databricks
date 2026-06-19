# Databricks notebook source
# MAGIC %md
# MAGIC ## Liquid Clustering
# MAGIC - Zastępuje partycjonowanie i mechanizm Z-Order
# MAGIC - Upraszcza ułożenie danych: przyspiesza zapytania
# MAGIC - Nie wymaga przepisywania danych
# MAGIC - Możesz zmieniać klucze tabeli bez jej przepisywania
# MAGIC - Musisz zadać sobie pytanie czy każda tabele potrzebuje LQ ?<br>
# MAGIC Przeczytaj [dokumentację](https://learn.microsoft.com/en-us/azure/databricks/delta/clustering) zwłaszcza sekcję Note 

# COMMAND ----------

# MAGIC %md
# MAGIC Wszystkie operacje możesz wykonać w PySpark lub SQL<br>
# MAGIC
# MAGIC ### Użycie
# MAGIC `df.write.clusterBy("col0").saveAsTable("table2")`
# MAGIC
# MAGIC ### Uzycie SQL
# MAGIC `ALTER TABLE table_name CLUSTER BY (new_column1, new_column2);`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wybór kolumn do klastrowania
# MAGIC Co warto wiedzieć;
# MAGIC Delta zbiera [statystyki kolumn](https://learn.microsoft.com/en-us/azure/databricks/delta/data-skipping#specify-delta-statistics-columns) z tabeli tylko pierwsze 32 kolumny<br>
# MAGIC Jest to potrzebne do wyboru optymalnego planu wykonania oraz omijania danych ([data skipping](https://learn.microsoft.com/pl-pl/azure/databricks/delta/data-skipping))

# COMMAND ----------

# MAGIC %md
# MAGIC LC może być [automatyczne](https://learn.microsoft.com/en-us/azure/databricks/delta/clustering#auto-liquid), Databricks postara się dobrac klucze pod optymalizacje 
# MAGIC - włączasz `CLUSTER BY AUTO`
# MAGIC - Automatyczny wybór kluczy będzie dobrany na podstawie historycznych danych 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### --------------------- Tabela bez Liquid Clustering ---------------------

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.dbo.cluster_demo AS
# MAGIC SELECT id, id % 10 AS grp, CAST(rand()*1000 AS INT) AS val
# MAGIC FROM RANGE(100000000);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL silver.dbo.cluster_demo;   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from silver.dbo.cluster_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver.dbo.cluster_demo; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC     COUNT(*) as record_count,
# MAGIC     AVG(val) as avg_value,
# MAGIC     SUM(val) as total_value,
# MAGIC     MIN(val) as min_value,
# MAGIC     MAX(val) as max_value
# MAGIC
# MAGIC from silver.dbo.cluster_demo
# MAGIC where grp = 9
# MAGIC group by grp

# COMMAND ----------

# MAGIC %md 
# MAGIC ### --------------------- Tabela z Liquid Clustering ---------------------

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.dbo.cluster_demo_with_lc AS
# MAGIC SELECT id, id % 10 AS grp, CAST(rand()*1000 AS INT) AS val
# MAGIC FROM RANGE(100000000);
# MAGIC
# MAGIC ALTER TABLE silver.dbo.cluster_demo_with_lc CLUSTER BY (grp, val);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL silver.dbo.cluster_demo_with_lc;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver.dbo.cluster_demo_with_lc; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC     COUNT(*) as record_count,
# MAGIC     AVG(val) as avg_value,
# MAGIC     SUM(val) as total_value,
# MAGIC     MIN(val) as min_value,
# MAGIC     MAX(val) as max_value
# MAGIC from silver.dbo.cluster_demo_with_lc
# MAGIC where grp = 9
# MAGIC group by grp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table silver.dbo.cluster_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table silver.dbo.cluster_demo_with_lc
