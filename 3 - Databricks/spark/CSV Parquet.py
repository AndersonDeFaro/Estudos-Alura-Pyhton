# Databricks notebook source
dbutils.fs.mkdirs('/FileStore/tables/arquivo')

#dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

#Leitura de arquivo CSV
dataframesp= spark.read.format("csv").option("header","true").load("/FileStore/tables/arquivo/Datafiniti_Hotel_Reviews_Jun19.csv")
dataframesp.show()

# COMMAND ----------

#criando o arquivo parquet
dataframesp.write.parquet("/FileStore/tables/parquet/csvparquet.parquet")

# COMMAND ----------

#Realizando uma leitura do arquivo parquet
datafleitura=spark.read.parquet("/FileStore/tables/parquet/csvparquet.parquet")
datafleitura.show()

# COMMAND ----------

