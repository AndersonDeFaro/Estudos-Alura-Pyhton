-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.types import StructType, DataType, IntegerType, StringType, DecimalType, FloatType
-- MAGIC
-- MAGIC Record_schema = (StructType().
-- MAGIC                  add('registro', IntegerType()).
-- MAGIC                  add('pais', StringType()).
-- MAGIC                  add('descricao', StringType()).
-- MAGIC                  add('destino', StringType()).
-- MAGIC                  add('pontos', IntegerType()).
-- MAGIC                  add('preco', FloatType()).
-- MAGIC                  add('provincia', StringType()).
-- MAGIC                  add('regiao_1', StringType()).
-- MAGIC                  add('regiao_2', StringType()).
-- MAGIC                  add('somelier', StringType()).
-- MAGIC                  add('twitter_somelier', StringType()).
-- MAGIC                  add('endereco', StringType()).
-- MAGIC                  add('variantes', StringType()).
-- MAGIC                  add('vinicula', StringType())
-- MAGIC                  )
-- MAGIC
-- MAGIC # File location and type
-- MAGIC file_location = "/FileStore/tables/carga/vinhos_no_mundo.csv"
-- MAGIC file_type = "csv"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # CSV options
-- MAGIC infer_schema = "true"
-- MAGIC first_row_is_header = "true"
-- MAGIC delimiter = ","
-- MAGIC
-- MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
-- MAGIC df = spark.read.format(file_type) \
-- MAGIC   .schema(Record_schema) \
-- MAGIC   .option("inferSchema", infer_schema) \
-- MAGIC   .option("header", first_row_is_header) \
-- MAGIC   .option("sep", delimiter) \
-- MAGIC   .load(file_location)
-- MAGIC
-- MAGIC #  display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create a view or table
-- MAGIC
-- MAGIC temp_table_name = "vinhos_no_mundo"
-- MAGIC df.createOrReplaceTempView(temp_table_name)
-- MAGIC

-- COMMAND ----------

drop table if exists vinho

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dbutils.fs.rm('dbfs:/user/hive/warehouse/vinho', recurse = True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC permanent_table_name = "vinho"
-- MAGIC df.write.format('csv').saveAsTable(permanent_table_name)
-- MAGIC # display(df)

-- COMMAND ----------

select * from vinho

-- COMMAND ----------

select regiao_1, preco from vinho

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('/FileStore/tables/carga/clientes_cartao.csv'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clientes = spark.read.format('csv').options(header='true', inferSchema='true', delimiter=';').load('/FileStore/tables/carga/clientes_cartao.csv')
-- MAGIC
-- MAGIC display(clientes)

-- COMMAND ----------

select pais, sum(preco) total_vendido 
from vinho 
where preco > 0
group by  pais
order by total_vendido desc
limit 10

-- COMMAND ----------

select pais, variantes, sum(preco) total_vendido 
from vinho 
where preco > 0
group by  pais,variantes
order by total_vendido desc
limit 10

-- COMMAND ----------

select pontos, preco
from vinho 

-- COMMAND ----------

