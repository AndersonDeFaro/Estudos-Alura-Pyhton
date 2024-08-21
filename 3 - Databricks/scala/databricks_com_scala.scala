// Databricks notebook source
// MAGIC %md
// MAGIC ### Definição de Classe Scala

// COMMAND ----------

case class City(rank: Long, city: String, state: String, code: String, population: Long, price: Double)

// COMMAND ----------

val df1 = Seq(new City(295, "South Bend", "Indiana", "IN", 101190, 112.9)).toDF
display(df1)

// COMMAND ----------

val df2 = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("/databricks-datasets/samples/population-vs-price/data_geo.csv")

  display(df2)


// COMMAND ----------

//Combinando DataFrames
val df = df1.union(df2)
display(df)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Criar filtros no Dataframe

// COMMAND ----------

// Filtro usandop Filter
val filtro_df = df.filter(df("rank") < 6)
display(filtro_df)

// COMMAND ----------

val filtro_df = df.where( df("rank") < 6)
display(filtro_df)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Selecionar dados do DataFrame

// COMMAND ----------

val selecionar_df = df.select("City", "State")
display(selecionar_df)

// COMMAND ----------

// Filtra e Seleciona
val selecionar_df = df.where(df("Rank") < 11).select("City", "State", "Rank")
display(selecionar_df)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Salvar dados em varios formatos

// COMMAND ----------

// Escrevendo estrutura como tabela
df.write.saveAsTable("us_cities")

// COMMAND ----------

// Escrever a estrutura no formato JSON
df.write.json("/tmp/json/us_cities")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Ler dados JSON

// COMMAND ----------

val df3 = spark.read.format("json").json("/tmp/json/us_cities")
display(df3)
