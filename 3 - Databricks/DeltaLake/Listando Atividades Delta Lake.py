# Databricks notebook source
# MAGIC %md
# MAGIC ### Para esse script rodar a seguinte estrutura precisa executar antes:
# MAGIC ####### Notepad: Carga Dados Compras 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrando o histórico das transações no Delta Lake - tabela Compras

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY '/user/hive/warehouse/compras'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mostrando os dados de criação da tabela compras

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL '/user/hive/warehouse/compras'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retornando(exibindo) a versão dos dados a posição do merge dos dados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta. `/user/hive/warehouse/compras` VERSION AS OF 2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mostrando os dados após o retorno das versões

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("select * from compras").show();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clonando os dados gerados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE delta. `/temporario/hive/clonado` CLONE delta. `/user/hive/warehouse/compras`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restaurando em definitivo os dados na tabela compras

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE compras TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mostrando os dados restaurados

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("select * from compras").show();

# COMMAND ----------

