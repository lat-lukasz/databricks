# Databricks notebook source
# MAGIC %md
# MAGIC ### DBFS - Databricks File System 
# MAGIC * Pliki w DBFS są zapisane na dysku
# MAGIC * Bardzo ważne !!! Dane w DBFS są przypisane do Databricks Workspace, dopóki środowisko robocze istnieje, dopóty dane będą bezpiecznie. Usunięcie środowiska wykasuje konto magazynu połączone z Workspace. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Narzędzie Databricks czyli dbutils
# MAGIC Jest to funkcjonalność dostarczona przez Databricks i ułatwia prace w notatnikach. Pozwala na użycie wielu funkcji. Podstawowe ich opcje to:
# MAGIC
# MAGIC * Praca z obiektami na koncie magazynu, czyli blob lub data lake.
# MAGIC * Łączenie wielu notebooków i przekazywanie parametrów pomiędzy nimi.
# MAGIC * Praca z sekretami (secrets) stosuje się je do przechowywania kluczy lub haseł.
# MAGIC
# MAGIC Szczegóły znajdziesz w
# MAGIC <a href="https://docs.microsoft.com/pl-pl/azure/databricks/dev-tools/databricks-utils" target="_blank">dokumentacji.</a>
# MAGIC
# MAGIC Dodatkową pomoc możesz uzyskać przy użyciu poniższych komend.
# MAGIC #### Sysem plików
# MAGIC * `dbutils.fs.help()`
# MAGIC #### Bezpośrednia praca z plikami
# MAGIC * `dbutils.meta.help()`
# MAGIC #### Narzędzia notaników
# MAGIC * `dbutils.notebook.help()`
# MAGIC #### Pola wprowadzania danych
# MAGIC * `dbutils.widgets.help()`

# COMMAND ----------

# Dostęp do pomocy
dbutils.help()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### dbutils.fs

# COMMAND ----------

# Najczcęściej używane do pracy z systemem plików
dbutils.fs.help()

# COMMAND ----------

# Przeglądanie zawartości konta magazynu, wystarczy podać scieżkę i otrzymasz listę elementów w wybranej lokalizacji. Osobiście lubię metodę display(), która bardzo upiększa wyświetlane listy, szczególnie jak się pracuj z systemem plików.
display(dbutils.fs.ls("dbfs:/databricks-datasets/retail-org/"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### dbutils.fs.mkdirs

# COMMAND ----------

# Tworzy ścieżkę
spark.sql("""
CREATE VOLUME IF NOT EXISTS workspace.default.filestore
COMMENT 'This is my example managed volume'
""")

dbutils.fs.mkdirs("/Volumes/workspace/default/filestore/nowy/folder")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### dbutils.fs.put

# COMMAND ----------

# Zapisuje string do pliku
dbutils.fs.put("/Volumes/workspace/default/filestore/nowy/folder/nowyplik.txt", "zawartość nowego pliku", True)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### dbutils.fs.head

# COMMAND ----------

# Wyświetla zawartość pliku, ilość danych podajesz w bajtach
dbutils.fs.head("/Volumes/workspace/default/filestore/nowy/folder/nowyplik.txt", 256)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### dbutils.fs.cp

# COMMAND ----------

# Kopjuje pliki lub foldery z opcja rekursywna
dbutils.fs.cp("/Volumes/workspace/default/filestore/nowy/folder/nowyplik.txt","/Volumes/workspace/default/filestore/stary/folder/nowyplik.txt", False)


# COMMAND ----------

# MAGIC %md 
# MAGIC #### dbutils.fs.mv

# COMMAND ----------

# Przesuwanie plików z jednej ścieżki do drugiej
dbutils.fs.mv("/Volumes/workspace/default/filestore/nowy/folder/nowyplik.txt", "/Volumes/workspace/default/filestore/stary/folder/nowyplik.txt", False)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### dbutils.fs.rm

# COMMAND ----------

# Usuwa pliki luż ścieżki
dbutils.fs.rm("/Volumes/workspace/default/filestore/stary/folder", True)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Wyświetlanie zawartości obiektów display(..)
# MAGIC Bardzo użyteczna funkcja pozwala na wyświetlenie zawartości Dataframe, dane wyglądają znacznie ładniej niż `show()`
# MAGIC

# COMMAND ----------

kolumny = ["Spark Version","Scala Version", "Repository", "Usages", "Date", "Updated"]
dane = [
  ("3.2.0", "2.13", "central", 15, "Oct, 2021", 20211026),
  ("3.1.2", "2.12", "central", 77, "May, 2021", 20211026),
  ("3.1.1", "2.12", "central", 102, "Jan, 2021", 20211026)]
 
df = spark.createDataFrame(dane).toDF(*kolumny)
display(df)
# df.show()

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/workspace/default/filestore/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Magic Command: &percnt;fs
# MAGIC
# MAGIC Komenda `%fs` pozwala na wyświetlenie zawartości systemu plików w wybranej ścieżce. Jest ona równoznaczna z `dbutils.fs` 

# COMMAND ----------

# MAGIC %fs ls 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Dokumentacja
# MAGIC
# MAGIC Jeśli chcesz wiedzieć więcej oto linki do oficjalnej dokumentacji
# MAGIC
# MAGIC * <a href="https://docs.microsoft.com/pl-pl/azure/databricks/" target="_blank">User Guide</a>
# MAGIC * <a href="https://docs.microsoft.com/pl-pl/azure/databricks/notebooks/" target="_blank">User Guide / Notebooks</a>
