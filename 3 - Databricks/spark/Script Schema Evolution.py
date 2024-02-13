# Databricks notebook source
# MAGIC %md
# MAGIC ## Criando o primeiro dataframe com dados, com os campos (Funcionario,Salario)

# COMMAND ----------

empresa1 = spark.createDataFrame(
[
("Joao Santos",2000),
("Carlos Fernandez",3400)
],['Funcionario', 'Salario'] )
display(empresa1)
empresa1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setado caminho, armazenando em um atributo

# COMMAND ----------

parquetpath = "dbfs:/FileStore/tables/delta/schema_evolution/parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando arquivos parquet com base no primeiro dataframe

# COMMAND ----------

(
empresa1
.write
.format("parquet")
.save("/FileStore/tables/delta/schema_evolution/parquet")
)
spark.read.parquet(parquetpath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando o segundo dataframe com dados, acrescentando novos campos (Setor,Comissao)

# COMMAND ----------

empresa2 = spark.createDataFrame(
[
("Financeiro",240),
("Marketing",540)
],['Setor', 'Comissao'] )
display(empresa2)
empresa2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apesar de colocar "append"no parquet, não houve evolução do esquema, as colunas foram substituídas

# COMMAND ----------

empresa2.write.mode("append").parquet(parquetpath)
spark.read.parquet(parquetpath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Vamos gerar o Schema Evolution com as tabelas Delta

# COMMAND ----------

deltapath = "/FileStore/tables/delta/schema_evolution/delta"
(
empresa1
.write
.format("delta")
.save("/FileStore/tables/delta/schema_evolution/delta")
)
spark.read.format("delta").load(deltapath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Vamos realizar um "merge" entre os dataframes, note que agora conseguirá realizar a junção entre os schemas. Os dados inexistentes foram acrescidos de nulos

# COMMAND ----------

(
empresa2
.write
.format("delta")
.mode("append")
.option("mergeSchema", "true")
.save(deltapath)
)
spark.read.format("delta").load(deltapath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Vamos inserir novos dados e aplicar o "append" para verificar como funcionará a inserção

# COMMAND ----------

empresa3 = spark.createDataFrame(
[
("Sandra Lemos",672),
("Carla Soares",966),
],
['Funcionario', 'Comissao']
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vamos acrescentar dados mais dados e verificar a inclusão apenas de alguns campos

# COMMAND ----------

(
empresa3
.write
.format("delta")
.mode("append")
.option("mergeSchema", "true")
.save(deltapath)
)
spark.read.format("delta").load(deltapath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Vamos fazer sobregravação dos formatos delta, variando os modos , quando o merge está ativo "option=mergeSchema, mode=overwrite"

# COMMAND ----------

(
empresa3
.write
.format("delta")
.mode("overwrite")
.option("mergeSchema", "true")
.save(deltapath)
)
spark.read.format("delta").load(deltapath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vamos sobrescrever toda a tabela, perceba as mudancas dos campos que ficaram e dos registros. "option=overwriteschema , mode=overwrite"

# COMMAND ----------

(
empresa3
.write
.format("delta")
.option("overwriteSchema", "true")
.mode("overwrite")
.save(deltapath)
)
spark.read.format("delta").load(deltapath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vamos criar uma tabela delta com referência aos parquet (delta) criados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE tab_empresa(
# MAGIC Funcionario STRING,
# MAGIC Comissao long
# MAGIC )
# MAGIC USING delta
# MAGIC LOCATION "/FileStore/tables/delta/schema_evolution/delta"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vamos listar o histórico gerado de todas as nossas mudanças

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY '/FileStore/tables/delta/schema_evolution/delta'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Listando todas as versões que podemos utilizar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta. `/FileStore/tables/delta/schema_evolution/delta`
# MAGIC VERSION AS OF 2

# COMMAND ----------

