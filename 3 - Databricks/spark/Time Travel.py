# Databricks notebook source
# MAGIC %md
# MAGIC ## Antes de começar rodar o script:
# MAGIC ##### Notebook - Carga Dados Compras

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo a primeira versão via Python

# COMMAND ----------

df = spark.read \
.format("delta") \
.option("versionAsOf", "1") \
.load("/user/hive/warehouse/compras")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contando a quantidade de registros na terceira versão via SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM compras VERSION AS OF 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contando a quantidade de registros na terceira versão via SQL - Outra forma de realizar a tarefa

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM compras@v3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contando a quantidade de registros na terceira versão via SQL - Outra forma de realizar a tarefa

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/user/hive/warehouse/compras@v3`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Descrevendo o histórico dos dados para verificar a quantidade de versões

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY '/user/hive/warehouse/compras'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vamos reinserir o registro com ID=1 que eliminamos, uma forma de realizar o Delta Time Travel

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO compras
# MAGIC SELECT * FROM compras VERSION AS OF 1
# MAGIC WHERE Id = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibindo os dados atualizados, após retorno da versão 1, ou seja o registro deletado foi restaurado

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM compras

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mostrando as versões agora, depois do insert

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY '/user/hive/warehouse/compras'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificando quantos registro é a diferença da versão atual, para a versão 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(distinct ID) - (SELECT count(distinct ID) FROM compras
# MAGIC VERSION AS OF 3) as `Diferença de registros` FROM compras

# COMMAND ----------

