# Databricks notebook source
import pandas as pd
import pyspark as ps

# COMMAND ----------

spark = ps.sql.SparkSession.builder.getOrCreate()

# COMMAND ----------

# Criar um DataFrame
dados = spark.read.csv('/FileStore/tables/arquivos_curso/amazon.csv', header=True)
dados.toPandas().head(5)

# COMMAND ----------

dados.toPandas().info()

# COMMAND ----------

dados['discounted_price'] = dados['discounted_price'].to_numeric()
dados['actual_price'] = dados['actual_price'].to_numeric()
dados


# COMMAND ----------

