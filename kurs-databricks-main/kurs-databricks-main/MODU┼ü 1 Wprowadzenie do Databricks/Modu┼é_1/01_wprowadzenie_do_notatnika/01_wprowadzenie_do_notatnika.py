# Databricks notebook source
# MAGIC %md
# MAGIC ### Tytuł

# COMMAND ----------

# MAGIC %md ##Navigacja po notatniku
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Magiczna komenda %sh
# MAGIC Ta komenta pozwala na wykonanie skryptów basha. Możesz jej używać w notatniku Databricks
# MAGIC np. `%sh ls logs` wylistuje pliki z logami 
# MAGIC

# COMMAND ----------

ls -l /

# COMMAND ----------

# MAGIC %md
# MAGIC ### Magiczne Komendy dla wykonania kodu w innych językach
# MAGIC Masz możliwość w obrębie jednego notatnika wykonać kod w kilku językach. Wystarczy dodać jedną z poniższych komend i dana komórka wykona się w wybranym języku. Twój notatnik może być w scali, a poszególne komórki w pythonie lub sql
# MAGIC * **&percnt;python** 
# MAGIC * **&percnt;scala** 
# MAGIC * **&percnt;sql** 
# MAGIC * **&percnt;r** 

# COMMAND ----------

print("Hello world")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select "Hello SQL"

# COMMAND ----------

# MAGIC %scala
# MAGIC println("Hello world")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Magiczna komenda %md
# MAGIC Pozwala na dodanie nagłówków i opisu w poszczególnych komórkach
# MAGIC Przykłady
# MAGIC
# MAGIC # Nagłówek pierwszy
# MAGIC ## Nagłówek drugi
# MAGIC ### Nagłówek trzeci
# MAGIC
# MAGIC Można wyświetlać obrazy
# MAGIC <br/>
# MAGIC ![dddd](https://www.cegladanych.pl/wp-content/uploads/2020/11/Architektura-aplikacji-Spark.png)
# MAGIC <br/>
# MAGIC
# MAGIC Możesz wyświetlać listy
# MAGIC * Jeden
# MAGIC * Dwa
# MAGIC * Trzy
# MAGIC
# MAGIC Możesz robić ładne tabelki
# MAGIC
# MAGIC | Spark Version  | Scala Version | Date    |
# MAGIC |-------|-----|--------|
# MAGIC | 3.2.0   | 2.13  | Oct, 2021  |
# MAGIC | 3.1.2  | 2.12  | May, 2021 |
# MAGIC | 3.1.1  | 2.12  | Jan, 2021   |
# MAGIC
# MAGIC Jest to bardzo przydatne przy opisywaniu notatnika i poszczególnych elementów
# MAGIC <br/>
# MAGIC Tutaj link do ściągawki <a href="https://www.markdownguide.org/cheat-sheet/" target="_blank">Markdown Cheat Sheet</a>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Magic Command: &percnt;fs
# MAGIC
# MAGIC Komenda `%fs` pozwala na wyświetlenie zawartości systemu plików w wybranej ścieżce.

# COMMAND ----------

# MAGIC %fs ls dbfs:/databricks-datasets

# COMMAND ----------

# MAGIC %md
# MAGIC #### Wyświetlanie HTML `displayHTML(..)`
# MAGIC
# MAGIC Ta funkcja może się przydać kiedy chcesz wyświetlić HTML i użyć go do opisania notatnika
# MAGIC

# COMMAND ----------

html = """
    <html>
    <head>
    <title>Page Title</title>
    </head>
    <body>

    <h1>This is a Heading</h1>
    <p>This is a paragraph.</p>

    </body>
    </html>
    </body>
"""

displayHTML(html)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Dokumentacja
# MAGIC
# MAGIC Jeśli chcesz wiedzieć więcej oto linki do oficjalnej dokumentacji
# MAGIC
# MAGIC * <a href="https://docs.microsoft.com/pl-pl/azure/databricks/" target="_blank">User Guide</a>
# MAGIC * <a href="https://docs.microsoft.com/pl-pl/azure/databricks/notebooks/" target="_blank">User Guide / Notebooks</a>
