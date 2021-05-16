# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake
# MAGIC ![test](https://delta.io/wp-content/uploads/2020/03/home-page-social.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Montando o acesso para o data lake (Azure blob storage)

# COMMAND ---------- 

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<Application (client) ID>",
           "fs.azure.account.oauth2.client.secret":"<Application (secret) ID>",   
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<Directory (tenant) ID>/oauth2/token",
           "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

if not any(mount.mountPoint == '/mnt/<container-name>' for mount in dbutils.fs.mounts()):
      dbutils.fs.mount(
        source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",
        mount_point = "/mnt/<container-name>",
        extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC # Exibindo pastas e arquivos do mount criado

# COMMAND ----------

display(dbutils.fs.ls("dbfs:///mnt/<container-name>"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Usando DataFrame para armazenar e ler as informações da Landing Zone

# COMMAND ----------

dfPerson = spark.read.parquet("dbfs:/mnt/<container-name>/1.landing_zone/person.parquet")

display(dfPerson)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Criando o primeiro delta lake 
# MAGIC ## Da lading zone para a bronze zone (Primeiro passo do delta lake)

# COMMAND ----------

dfPerson.write.format("delta").save("dbfs:/mnt/<container-name>/2.bronze_zone/person")
spark.sql("CREATE TABLE person USING DELTA LOCATION 'dbfs:/mnt/<container-name>/2.bronze_zone/person'")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Lendo as informações da bronze zone em delta lake 
# MAGIC ## ACID Transaction na veia

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM person

# COMMAND ----------

# MAGIC %md 
# MAGIC # Crie a camada Silver zone usando MERGE no SQL 
# MAGIC ### Não se esqueça de apontar para a pasta Silver_zone no Azure Data Lake

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Faça aqui o seu código

# COMMAND ----------

# MAGIC %md 
# MAGIC # Agora crie uma tabela dimensional dentro da Gold Zone usando SQL 
# MAGIC ### Lembre-se de apontar para a pasta gold_zone no Azure Data Lake

# COMMAND ----------

# MAGIC %sql 
# MAGIC --Faça aqui o seu código
