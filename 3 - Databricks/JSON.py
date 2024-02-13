# Databricks notebook source
# MAGIC %fs ls /databricks-datasets/structured-streaming/events

# COMMAND ----------

# MAGIC %fs head /databricks-datasets/structured-streaming/events/file-1.json

# COMMAND ----------

dataf = spark.read.json('/databricks-datasets/structured-streaming/events/file-1.json')
dataf.printSchema()
dataf.show()

# COMMAND ----------

dataf2 = spark.read.json(['/databricks-datasets/structured-streaming/events/file-1.json', '/databricks-datasets/structured-streaming/events/file-2.json'])
dataf2.printSchema()
dataf2.show()

# COMMAND ----------

dataf3 = spark.read.json('/databricks-datasets/structured-streaming/events/*.json')
dataf3.printSchema() 
dataf3.show()

# COMMAND ----------

dataf3.write.json('/FileStore/tables/JSON/eventos.json')

# COMMAND ----------

dataf3.createOrReplaceTempView('view_evento')

spark.sql('''
           select action from view_evento
           ''').show()

# COMMAND ----------

