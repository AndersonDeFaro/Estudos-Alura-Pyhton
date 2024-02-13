# Databricks notebook source
# MAGIC %md
# MAGIC ## Eliminando arquivos caso existam nas pastas, pois serão destinados as tabelas Delta

# COMMAND ----------

# MAGIC %fs rm -r /tmp/delta/events

# COMMAND ----------

# MAGIC %fs rm -r /tmp/delta/checkpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listando os diversos arquivos Json para carga no Delta Lake

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibindo o conteúdo de 1 arquivo Json

# COMMAND ----------

#Lendo um dos arquivos JSON
dataf3 = spark.read.json("/databricks-datasets/structured-streaming/events/file-1.json")
dataf3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando um banco de dados em separado e uma tabela Delta que irá receber os dados do Json em Streaming

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS db_stream;
# MAGIC USE db_stream;
# MAGIC
# MAGIC DROP TABLE IF EXISTS db_stream.tab_stream;
# MAGIC
# MAGIC CREATE TABLE db_stream.tab_stream(
# MAGIC action STRING,
# MAGIC time STRING
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION "/tmp/delta/events"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executando a carga na pasta do Delta Lake, onde serão armazenados os dados

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Streaming reads and append into delta table (Start !)
read_schema = StructType([
StructField("action", StringType(), False),
StructField("time", StringType(), True)
])
df2 = (spark.readStream
.option("maxFilesPerTrigger", "1")
.schema(read_schema)
.json("/databricks-datasets/structured-streaming/events/"))
(df2.writeStream
.format("delta")
.outputMode("append")
.option("checkpointLocation", "/tmp/delta/checkpoint")
.option("path", "/tmp/delta/events").start())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibindo os dados em tempo real oriunda da tabela Delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct action, count(*) from db_stream.tab_stream
# MAGIC group by action

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listando os históricos registrados na tabela Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY '/tmp/delta/events'

# COMMAND ----------

