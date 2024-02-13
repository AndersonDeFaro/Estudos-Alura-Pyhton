# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# display(dbutils.fs.ls('/FileStore/tables/carga'))

# COMMAND ----------

# dbutils.fs.rm('/FileStore/tables/carga/vinhos_no_mundo-1.csv')

# COMMAND ----------

from pyspark.sql.types import StructType, DataType, IntegerType, StringType, DecimalType, FloatType

Record_schema = (StructType().
                 add('registro', IntegerType()).
                 add('pais', StringType()).
                 add('descricao', StringType()).
                 add('destino', StringType()).
                 add('pontos', IntegerType()).
                 add('preco', FloatType()).
                 add('provincia', StringType()).
                 add('regiao_1', StringType()).
                 add('regiao_2', StringType()).
                 add('somelier', StringType()).
                 add('twitter_somelier', StringType()).
                 add('endereco', StringType()).
                 add('variantes', StringType()).
                 add('vinicula', StringType())
                 )

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/carga/vinhos_no_mundo.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .schema(Record_schema) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a view or table

temp_table_name = "vinhos_no_mundo"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vinhos_no_mundo

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "vinho"
 # df.write.saveAsTable(permanent_table_name)
#  
df.write.format('csv').saveAsTable(permanent_table_name)
# display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(registro) from vinho

# COMMAND ----------

