# Databricks notebook source
# MAGIC %md
# MAGIC ## Optymalizacja oraz czyszczenie
# MAGIC  - OPTIMIZE + VACUUM

# COMMAND ----------

optimize_path = '/Volumes/workspace/default/optimize/'

# COMMAND ----------

data1 = [{"name": "Kris Kelvin", "age": 35, "book": "Solaris", "role": "Psychologist", "species": "Human", "planet": "Solaris Station"}]
df1 = spark.createDataFrame(data1)
data2 = [{"name": "Hal Bregg","age": 40,"book": "Return from the Stars", "role": "Cosmonaut", "species": "Human", "planet": "Earth"}]
df2 = spark.createDataFrame(data2)
data3 = [{"name": "Trurl","age": 100, "book": "The Cyberiad", "role": "Constructor", "species": "Machine", "planet": "Pinta"}]
df3 = spark.createDataFrame(data3)
data4 = [{"name": "Dr. Snaut","age": 50, "book": "Solaris", "role": "Scientist", "species": "Human","planet": "Quinta"}]
df4 = spark.createDataFrame(data4)

# COMMAND ----------

(df1
.write.format('delta')
.mode('append')
.option("overwriteSchema", "true")
.save(optimize_path))

# COMMAND ----------

(df2
.write.format('delta')
.mode('append')
.option("overwriteSchema", "true")
.save(optimize_path))

# COMMAND ----------

(df3
.write.format('delta')
.mode('append')
.option("overwriteSchema", "true")
.save(optimize_path))

# COMMAND ----------

(df4
.write.format('delta')
.mode('append')
.option("overwriteSchema", "true")
.save(optimize_path))

# COMMAND ----------

display(dbutils.fs.ls(optimize_path))

# COMMAND ----------

spark.read.json(f'{optimize_path}_delta_log/00000000000000000001.json').display()

# COMMAND ----------

spark.read.format("delta").load(optimize_path).selectExpr('*','_metadata.*').display()

# COMMAND ----------

spark.sql(f"DESCRIBE HISTORY '{optimize_path}'").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE '/Volumes/workspace/default/optimize/'

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled","false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM '/Volumes/workspace/default/optimize/' RETAIN 0 HOURS 

# COMMAND ----------

display(dbutils.fs.ls(optimize_path))
        

# COMMAND ----------

# MAGIC %md
# MAGIC Za usuwanie niepotrzebnych logów odpowiedzialna jest konfiguracja `delta.logRetentionDuration`

# COMMAND ----------

display(dbutils.fs.ls(f'{optimize_path}/_delta_log/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES delta.`/Volumes/workspace/default/optimize/` 

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/Volumes/workspace/default/optimize/` 
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.deletedFileRetentionDuration' = 'interval 0 days',
# MAGIC   'delta.logRetentionDuration' = 'interval 0 hours'
# MAGIC )

# COMMAND ----------

# dbutils.fs.rm(optimize_path,True)
