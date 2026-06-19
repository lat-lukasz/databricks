# Databricks notebook source
# MAGIC %md
# MAGIC ### Wprowadzenie do Delta Lake
# MAGIC
# MAGIC Databricks Delta to system zarządzania danymi, który zapewnia niezawodność i wydajność (od 10 do 100 razy szybszy niż Spark na formacie Parquet). Podstawą w tabel Delta jest wbudowany mechanizm zapewniający niezawodność i wydajności. Jest on podobny do mechanizmu tabel relacyjnych w bazach danych.
# MAGIC
# MAGIC Możesz odczytywać i zapisywać dane przechowywane w Databricks Delta, korzystając z tych samych znanych interfejsów API Apache Spark SQL — zarówno wsadowych, jak i strumieniowych — które są używane do pracy z tabelami Hive lub katalogami DBFS. Databricks Delta oferuje następującą funkcjonalność:
# MAGIC
# MAGIC Podstawowe Funkcje Delta Lake
# MAGIC 1. **Transakcje ACID**
# MAGIC  
# MAGIC * **Atomowość**: Wszystkie zmiany w ramach transakcji albo są wykonywane w całości, albo wcale
# MAGIC * **Spójność**: Dane pozostają w prawidłowym stanie przed i po transakcjach
# MAGIC * **Izolacja**: Równoległe transakcje nie zakłócają się wzajemnie, wielu użytkowników może równoczeście modyfikować dane.
# MAGIC * **Trwałość**: Zatwierdzone zmiany są trwałe i odporne na awarie systemu
# MAGIC
# MAGIC 2. **Skalowalna Obsługa Metadanych**
# MAGIC
# MAGIC * Świetnie współpracują z rozproszonym przetwarzaniem.
# MAGIC * Efektywnie obsługuje duże ilości metadanych
# MAGIC * Umożliwia operacje na dużych zbiorach danych w skali PB
# MAGIC * Odczyty są szybsze dzięki statystykom plików. Zawierają one metadane o tym co jest w każdym pliku. Dzięki temu nie trzeba czytać całego pliku.
# MAGIC
# MAGIC 3. **Podróż w Czasie** (Wersjonowanie Danych)
# MAGIC
# MAGIC * Możesz odczytać historyczne wersje każdej tabeli.
# MAGIC * Audyt zmian danych w czasie
# MAGIC * Odtwarzanie eksperymentów lub raportów na podstawie danych historycznych
# MAGIC
# MAGIC 4. **Ewolucja Schematu**
# MAGIC
# MAGIC * Egzekwowanie Schematu: Zapobiega dodawaniu rekordów niezgodnych ze schematem tabeli
# MAGIC * Ewolucja Schematu: Bezpieczne dodawanie, zmiana lub usuwanie kolumn w miarę zmian tabeli
# MAGIC
# MAGIC 5. **Ujednolicone Przetwarzanie Wsadowe i Strumieniowe**
# MAGIC
# MAGIC * Te same tabele Delta Lake mogą być używane do zadań wsadowych i strumieniowych
# MAGIC * Zapewnia spójność między operacjami strumieniowymi i wsadowymi
# MAGIC * Obsługuje semantykę przetwarzania exactly-once
# MAGIC
# MAGIC 6. **Otwarty Format**
# MAGIC
# MAGIC * Zbudowany na bazie plików Parquet
# MAGIC * Wszystkie dane są przechowywane w otwartym formacie
# MAGIC * Dostępne dla każdego kompatybilnego narzędzia, nie tylko Apache Spark
# MAGIC
# MAGIC 7. **DELETES/UPDATES/UPSERTS**
# MAGIC
# MAGIC * Wspiera mechanizmy aktualizacji tabel
# MAGIC * Deletes: usuwanie danych
# MAGIC * Updates: Aktualizacja rekordów
# MAGIC * Upserts: Czyli insert lub update

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC
# MAGIC ## Dodatkowe materiały
# MAGIC Dokumentacja Databricks Delta Azure?  
# MAGIC - <a href="https://learn.microsoft.com/pl-pl/azure/databricks/delta/" target="_blank">Co to jest Delta Lake?</a>
# MAGIC
# MAGIC Dokumentacja Delta Lake?    
# MAGIC - <a href="https://docs.delta.io/latest/index.html" target="_blank">Delta Lake documentation</a>.