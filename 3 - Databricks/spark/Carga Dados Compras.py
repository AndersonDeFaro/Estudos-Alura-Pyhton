# Databricks notebook source
# MAGIC %md
# MAGIC #######Primeiro Script , trabalhando com carga de dados e ajuste de registros no Delta Lake
# MAGIC ##Carrega os dados do arquivo Json

# COMMAND ----------

dfcliente = spark.read.json("/FileStore/tables/cliente/clientes.json");
dfcliente.printSchema()
dfcliente.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Cria a tabela temporária com os dos do arquivo json em memória

# COMMAND ----------

dfcliente.createOrReplaceTempView("compras_view");
saida =spark.sql("SELECT * FROM compras_view")
saida.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Carrega os dados no Delta Lake gerando uma tabela chamada compras, note USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE if exists compras;

# COMMAND ----------

# MAGIC %fs rm -r /user/hive/warehouse/compras

# COMMAND ----------

# MAGIC %scala
# MAGIC val scrisql = "CREATE OR REPLACE TABLE compras (id STRING, date_order STRING,customer STRING,product STRING,unit INTEGER,price DOUBLE) USING DELTA PARTITIONED BY (date_order) ";
# MAGIC
# MAGIC spark.sql(scrisql);

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lista os dados do Delta Lake, que estará vazia

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("select * from compras").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Criando um merge para carregar os dados da tabela temporário no Delta Lake

# COMMAND ----------

# MAGIC %scala
# MAGIC val mergedados = "Merge into compras " +
# MAGIC "using compras_view as cmp_view " +
# MAGIC "ON compras.id = cmp_view.id " +
# MAGIC "WHEN MATCHED THEN " +
# MAGIC "UPDATE SET compras.product = cmp_view.product," +
# MAGIC "compras.price = cmp_view.price " +
# MAGIC "WHEN NOT MATCHED THEN INSERT * ";
# MAGIC
# MAGIC spark.sql(mergedados);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibe os dados que foram carregados com o merge

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("select * from compras").show();

# COMMAND ----------

# MAGIC %md
# MAGIC ##Atualiza os dados do id=4 com o comando update

# COMMAND ----------

# MAGIC %scala
# MAGIC val atualiza_dados = "update compras " +
# MAGIC "set product = 'Geladeira' " +
# MAGIC "where id =4";
# MAGIC spark.sql(atualiza_dados);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibe os dados que foram carregados, note a atualização no id=4

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("select * from compras").show();

# COMMAND ----------

# MAGIC %md
# MAGIC ## Eliminação do registro cujo o id=1

# COMMAND ----------

# MAGIC %scala
# MAGIC val deletaregistro = "delete from compras where id = 1";
# MAGIC spark.sql(deletaregistro);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibe os dados que foram carregados

# COMMAND ----------

# MAGIC %scala
# MAGIC spark.sql("select * from compras").show();