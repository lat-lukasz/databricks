# Databricks notebook source
# MAGIC %md
# MAGIC ### Stara metoda połączenia do konta ❗❗❗
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Metody połączenia do Storage Account
# MAGIC
# MAGIC 1. [Mnt](https://techcommunity.microsoft.com/blog/azurepaasblog/mount-adls-gen2-or-blob-storage-in-azure-databricks/3802926)
# MAGIC 2. **[OAuth 2.0](https://learn.microsoft.com/pl-pl/azure/databricks/connect/storage/azure-storage#connect-to-azure-data-lake-storage-or-blob-storage-using-azure-credentials) przez Azure Entra ID i Service Principal**
# MAGIC 3. SAS Shared Access Signatues (tokeny). 
# MAGIC 4. Account Keys (klucze konta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Proces
# MAGIC
# MAGIC 1. [Rejestracja](https://learn.microsoft.com/en-us/azure/databricks/connect/storage/aad-storage-service-principal) aplikacji Entra ID
# MAGIC 2. Przypisanie roli w Storage Account
# MAGIC 3. Dodanie klucza Service Principal do Databricks Workspace (#secrets/createScope) [Secrets](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/)
# MAGIC 4. Konfiguracja Sparka

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wymagane informacje
# MAGIC ### Storage Account
# MAGIC - storage-account name
# MAGIC - storage-account container name
# MAGIC ### Service Principal
# MAGIC - application-id
# MAGIC - directory-id
# MAGIC - service-principal-sekret

# COMMAND ----------

# DBTITLE 1,Tworzymy secret scope
# databricks secrets delete-scope <nazwa wymyślona>
databricks secrets create-scope <nazwa wymyślona>
databricks secrets put-secret --json '{
  "scope": "",
  "key": "",
  "string_value": ""
}'

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="",key="")

spark.conf.set("fs.azure.account.auth.type.<nazwa storage account>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<nazwa storage account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<nazwa storage account>.dfs.core.windows.net", "<app id>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<nazwa storage account>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<nazwa storage account>.dfs.core.windows.net", "https://login.microsoftonline.com/<directory id>/oauth2/token")

# COMMAND ----------

service_credential2 = dbutils.secrets.get(scope="",key="")
for char in service_credential2:
    print(char, end=" ")

# COMMAND ----------

display(dbutils.fs.ls("abfss://<kontener>@<nazwa storage account>.dfs.core.windows.net/"))

# COMMAND ----------

# MAGIC %md
# MAGIC
