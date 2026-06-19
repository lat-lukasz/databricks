# Databricks notebook source
# MAGIC %md
# MAGIC ### Workflow - uruchomienie innego notatnika z obecnego 
# MAGIC Dzięki komendzie `%run` możesz twożyć workflowy składające się z wielu notatników. Komenda `%run "./3. Pobierz Dane"` uruchomi notatnik "3. Pobierz Dane" w tej samej ścieżce, w której znajduje się notatnik wywołujący.
# MAGIC <br/>
# MAGIC Najważniejszą zaletą tej funkcji jest możliwość użycia zmiennych lub innych obiektów z wykonanego notatnikia. Oba notatniki będą miały dostęp do wszystkich swoich obiektów. Dzięki temu możesz twożyć zniemme, klasy i metody i organizować je w kilka notatników. 
# MAGIC <a href="https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebook-workflows" target="_blank"> Dokumentacja notebook-workflows.</a>
# MAGIC

# COMMAND ----------

# MAGIC %run "./04a_workflow_funkcje"

# COMMAND ----------

suma = add_numbers(1,2)
print(suma)

# COMMAND ----------

print(zmienna)
