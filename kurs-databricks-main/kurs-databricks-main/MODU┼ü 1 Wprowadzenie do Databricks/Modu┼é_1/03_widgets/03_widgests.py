# Databricks notebook source
# MAGIC %md
# MAGIC #### Widgets
# MAGIC
# MAGIC Widżety pozwalają na dodanie parametrów do notatnika.
# MAGIC
# MAGIC Jest kilka typów widżetów: 
# MAGIC * text
# MAGIC * combobox
# MAGIC * dropdown
# MAGIC * multiselect
# MAGIC
# MAGIC Najczęstrze użycie jest przy twożeniu workflows. 
# MAGIC Kiedy chcecz wywołać inny notatnik i przekazać parametry

# COMMAND ----------

dbutils.widgets.help("dropdown")

# COMMAND ----------

dbutils.widgets.text("env", "test")

# COMMAND ----------

dbutils.widgets.dropdown("laptops","Lenovo",("Lenovo","Apple", "Dell", "HP"))

# COMMAND ----------

dbutils.widgets.combobox("books","",["Kongres Futurologiczny", "Kroniki Marsjańskie", "Mieć i nie Mieć"])

# COMMAND ----------

dbutils.widgets.multiselect("games", "Call of Duty", ["Call of Duty","Fortnite", "Minecraft", "ROBLOX", "Rocket League"])

# COMMAND ----------

# Pobranie danych z widgets i przypisanie do notatnika
env  = dbutils.widgets.get("env")
laptops = dbutils.widgets.get("laptops")
books = dbutils.widgets.get("books")
games = dbutils.widgets.get("games")

# COMMAND ----------

# Usunięcie wszystkich widgetów
dbutils.widgets.removeAll()

# COMMAND ----------

print(env)
print(laptops)
print(books)
print(games)
