# Databricks notebook source
display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/arquivos_curso')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/arquivos_curso'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Realizar Leitura JSON

# COMMAND ----------

df = spark.read.json('/FileStore/tables/arquivos_curso/PNSB.json')
display(df)

# COMMAND ----------

df = df.withColumnRenamed('D1C', 'cod_regiao')\
    .withColumnRenamed('D1N', 'regiao')\
    .withColumnRenamed('D2C', 'cod_variavel')\
    .withColumnRenamed('D2N', 'variavel')\
    .withColumnRenamed('D3C', 'cod_ano')\
    .withColumnRenamed('D3N', 'ano')\
    .withColumnRenamed('D4C', 'cod_doenca')\
    .withColumnRenamed('D4N', 'doenca')\
    .withColumnRenamed('MC', 'cod_medida')\
    .withColumnRenamed('MN', 'medida')\
    .withColumnRenamed('NC', 'cod_nivel_territorial')\
    .withColumnRenamed('NN', 'nivel_territorial')\
    .withColumnRenamed('V', 'valor')

display(df)

# COMMAND ----------

df = df.filter(df.valor != 'Valor')
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.