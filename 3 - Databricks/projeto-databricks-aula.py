# Databricks notebook source
# MAGIC %md
# MAGIC # Utilizando o Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando tabela no notebook

# COMMAND ----------

dbutils.fs.rm('/user/hive/warehouse/data_csv', recurse=True)

# Para este procedimento funcionar o arquivo data.csv deve estar carregado no DBFS no endereço especificado na variável `file_location`
file_location = '/FileStore/tables/data.csv'
file_type = 'csv'
infer_schema = 'true'
first_row_is_header = 'true'
delimiter = ';'

df = spark\
    .read\
    .format(file_type)\
    .option('inferSchema', infer_schema)\
    .option('header', first_row_is_header)\
    .option('sep', delimiter)\
    .load(file_location)

table_name = 'data_csv'

df.write.format('parquet').saveAsTable(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comandos Databricks Utilities - `dbutils`

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.rm('/FileStore/data.csv', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manipulando arquivos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listar todos os arquivos dentro de uma pasta

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

for item in dbutils.fs.ls('/'):
    print(item.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Acessando os arquivos carregados no DBFS

# COMMAND ----------

dbutils.fs.ls('/FileStore/')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listando as primeiras linhas de um arquivo

# COMMAND ----------

dbutils.fs.head('/FileStore/tables/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removendo arquivos

# COMMAND ----------

dbutils.fs.rm('/FileStore/tables/data.csv')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Datasets
# MAGIC ##### [Wine Quality Data Set](http://archive.ics.uci.edu/ml/datasets/wine+quality)

# COMMAND ----------

for item in dbutils.fs.ls('/'): print(item.path)

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/wine-quality'))

# COMMAND ----------

dbutils.fs.head("/databricks-datasets/wine-quality/README.md")

# COMMAND ----------

dbutils.fs.head("/databricks-datasets/wine-quality/winequality-red.csv")

# COMMAND ----------

dbutils.fs.head("/databricks-datasets/wine-quality/winequality-white.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diretórios e arquivos

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/aula-databricks/vinhos')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks'))

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/aula-databricks/vinhos')

# COMMAND ----------

dbutils.fs.help('cp')

# COMMAND ----------

dbutils.fs.cp(
    '/databricks-datasets/wine-quality', 
    '/FileStore/tables/aula-databricks', 
    recurse=True
)

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/aula-databricks/vinhos')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/aula-databricks')

# COMMAND ----------

dbutils.fs.help('mv')

# COMMAND ----------

 dbutils.fs.mv(
     '/FileStore/tables/aula-databricks/', 
     '/FileStore/tables/aula-databricks/vinhos/', 
     recurse=True
 )

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks'))

# COMMAND ----------

for item in dbutils.fs.ls('/FileStore/tables/aula-databricks'):
    if item.size!=0:
        dbutils.fs.mv(
          f'/FileStore/tables/aula-databricks/{item.name}', 
          '/FileStore/tables/aula-databricks/vinhos/'
        )

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks/vinhos'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Exercicio 1

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/aula-databricks/projetoBike')
dbutils.fs.cp('/databricks-datasets/bikeSharing/', '/FileStore', recurse = True)

dbutils.fs.ls('/FileStore/tables/aula-databricks/projetoBike')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

for item in dbutils.fs.ls('/FileStore'):
    display(item)

# COMMAND ----------

for item in dbutils.fs.ls('/FileStore'):
    if item.size != 0:
        dbutils.fs.mv(
            f'/FileStore/{item.name}',
            '/FileStore/tables/aula-databricks/projetoBike',
            recurse=True
        )

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks/projetoBike'))

# COMMAND ----------

for item in dbutils.fs.ls('/databricks-datasets/bikeSharing/'):
    if item.size != 0:
        dbutils.fs.mv(
            f'/FileStore/{item.name}',
            '/FileStore/tables/aula-databricks/projetoBike',
            recurse=True
        )
    else:
        dbutils.mv(
            f'/FileStore/{item.name}',
            f'/FileStore/tables/aula-databricks/projetoBike/{item.name}',
            recurse = True
        )

# COMMAND ----------

# MAGIC %md
# MAGIC # Usando SQL no Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criando uma tabela

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando um database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS teste

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando uma tabela

# COMMAND ----------

# MAGIC %sql
# MAGIC USE teste

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE usuarios(
# MAGIC   idade int,
# MAGIC   estado string,
# MAGIC   salario float
# MAGIC )
# MAGIC   ROW FORMAT DELIMITED 
# MAGIC     FIELDS TERMINATED BY ','
# MAGIC     LINES TERMINATED BY '\n'
# MAGIC   STORED AS textfile
# MAGIC   LOCATION '/FileStore/tables/aula-databricks/usuarios/'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM usuarios

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inserindo registros em uma tabela

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO usuarios VALUES (25, 'SP', 5000)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM usuarios

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partições
# MAGIC
# MAGIC Existem duas maneiras de inserir dados na tabela de partição:
# MAGIC
# MAGIC **Estático:** precisamos especificar o valor da coluna de partição em cada instrução que será carregada.
# MAGIC
# MAGIC > `PARTITION(country="BR")`
# MAGIC
# MAGIC **Dinâmico:** Não precisamos especificar o valor da coluna da partição.
# MAGIC
# MAGIC > `PARTITION(country)`

# COMMAND ----------

# MAGIC %sql
# MAGIC SET hive.exec.dynamic.partition = true;
# MAGIC SET hive.exec.dynamic.partition.mode = nonstrict;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE usuariosPart(
# MAGIC   idade int,
# MAGIC   estado string,
# MAGIC   salario float
# MAGIC )
# MAGIC   ROW FORMAT DELIMITED 
# MAGIC     FIELDS TERMINATED BY ','
# MAGIC     LINES TERMINATED BY '\n'
# MAGIC   STORED AS textfile
# MAGIC   PARTITIONED BY (ano int)
# MAGIC   LOCATION '/FileStore/tables/aula-databricks/usuariosPart/'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO usuariosPart VALUES (25, 'SP', 5000, 2021)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM usuariosPart

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO usuariosPart 
# MAGIC   PARTITION (ano=2020)
# MAGIC     VALUES (30, 'SP', 6000)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM usuariosPart

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM usuariosPart
# MAGIC     WHERE ano=2020

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregando dados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS tabela_vinhos

# COMMAND ----------

# MAGIC %sql
# MAGIC USE tabela_vinhos

# COMMAND ----------

dbutils.fs.head('/FileStore/tables/aula-databricks/vinhos/winequality-red.csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE red_wine(
# MAGIC     fixed_acidity float,
# MAGIC     volatile_acidity float,
# MAGIC     citric_acid float,
# MAGIC     residual_sugar float,
# MAGIC     chlorides float,
# MAGIC     free_sulfur_dioxide int,
# MAGIC     total_sulfur_dioxide float,
# MAGIC     density float,
# MAGIC     pH float,
# MAGIC     sulphates float,
# MAGIC     alcohol float,
# MAGIC     quality float
# MAGIC     )
# MAGIC       USING CSV
# MAGIC         OPTIONS (
# MAGIC             path '/FileStore/tables/aula-databricks/vinhos/winequality-red.csv',
# MAGIC             header 'true',
# MAGIC             delimiter ';'
# MAGIC         )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM red_wine
# MAGIC     LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atividade - Faça como eu fiz
# MAGIC
# MAGIC Repita o mesmo procedimento feito para o arquivo de vinho tinto com o arquivo de vinho branco
# MAGIC - Chame a tabela de white_wine

# COMMAND ----------

dbutils.fs.head('/FileStore/tables/aula-databricks/vinhos/winequality-white.csv')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE white_wine(
# MAGIC     fixed_acidity float,
# MAGIC     volatile_acidity float,
# MAGIC     citric_acid float,
# MAGIC     residual_sugar float,
# MAGIC     chlorides float,
# MAGIC     free_sulfur_dioxide int,
# MAGIC     total_sulfur_dioxide float,
# MAGIC     density float,
# MAGIC     pH float,
# MAGIC     sulphates float,
# MAGIC     alcohol float,
# MAGIC     quality float
# MAGIC     )
# MAGIC     USING CSV
# MAGIC         OPTIONS (
# MAGIC             path '/FileStore/tables/aula-databricks/vinhos/winequality-white.csv',
# MAGIC             header 'true',
# MAGIC             delimiter ';'
# MAGIC         )

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM white_wine
# MAGIC     LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explorando os dados

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE red_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT (quality)
# MAGIC   FROM red_wine
# MAGIC     ORDER BY quality DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quality, COUNT (quality) AS freq
# MAGIC   FROM red_wine
# MAGIC     GROUP BY quality
# MAGIC       ORDER BY quality DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quality, MIN (pH) AS pH_mimino, MAX (pH) AS pH_maximo    
# MAGIC   FROM red_wine
# MAGIC     GROUP BY quality
# MAGIC       ORDER BY quality DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atividade - Faça como eu fiz
# MAGIC
# MAGIC Faz as mesmas análise com os dados de vinho branco e compare os resultados obtidos.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE white_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT (quality)
# MAGIC   FROM white_wine
# MAGIC     ORDER BY quality DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quality, COUNT (quality) AS freq
# MAGIC   FROM white_wine
# MAGIC     GROUP BY quality
# MAGIC       ORDER BY quality DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT quality, MIN (pH) AS pH_mimino, MAX (pH) AS pH_maximo    
# MAGIC   FROM white_wine
# MAGIC     GROUP BY quality
# MAGIC       ORDER BY quality DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Juntando os dados

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE new_red_wine 
# MAGIC   AS SELECT *, 'red' AS wine_type
# MAGIC     FROM red_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM new_red_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE new_white_wine 
# MAGIC   AS select *, 'white' AS wine_type
# MAGIC     FROM white_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM new_white_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE combined_wines 
# MAGIC   AS SELECT *
# MAGIC     FROM new_red_wine
# MAGIC       UNION ALL SELECT *
# MAGIC         FROM new_white_wine

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT wine_type, AVG (pH) AS pH_medio    
# MAGIC   FROM combined_wines
# MAGIC     GROUP BY wine_type
# MAGIC       ORDER BY wine_type

# COMMAND ----------

# MAGIC %md
# MAGIC # Apache Spark

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comunicação Hive-Spark

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

tabela = spark.table('tabela_vinhos.combined_wines')

# COMMAND ----------

tabela

# COMMAND ----------

tabela.show()

# COMMAND ----------

display(tabela)

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL com Spark
# MAGIC
# MAGIC ```spark.sql('query').show()```
# MAGIC
# MAGIC ou 
# MAGIC
# MAGIC ```display(spark.sql('query'))```
# MAGIC
# MAGIC Se quisermos pular linhas na query temos que utilizar 3 aspas simples:
# MAGIC ```
# MAGIC spark.sql( '''
# MAGIC   query
# MAGIC ''' ).show()
# MAGIC ```

# COMMAND ----------

spark.sql('''
    SELECT DISTINCT (quality)
        FROM combined_wines
            ORDER BY quality DESC 
''').show()

# COMMAND ----------

spark.sql('SELECT AVG (pH) FROM combined_wines').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Registrando uma tabela

# COMMAND ----------

resultado = spark.sql(''' 
    SELECT *
        FROM combined_wines
            WHERE pH < 3
''')

# COMMAND ----------

type(resultado)

# COMMAND ----------

resultado.createOrReplaceTempView('nova_tabela')

# COMMAND ----------

spark.sql('''
    SELECT quality, COUNT (quality) AS Freq
        FROM nova_tabela
            GROUP BY quality
''').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## PySpark

# COMMAND ----------

import pyspark
from pyspark.sql.functions import lit

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/wine-quality/'))

# COMMAND ----------

red_wine_df = spark.read.format('csv')\
    .option('inferSchema', 'true')\
    .option('sep', ';')\
    .option('header', 'true')\
    .load('/databricks-datasets/wine-quality/winequality-red.csv')

display(red_wine_df)

# COMMAND ----------

type(red_wine_df)

# COMMAND ----------

white_wine_df = (spark.read.format('csv')
    .option('inferSchema', 'true')
    .option('sep', ';')
    .option('header', 'true')
    .load('/databricks-datasets/wine-quality/winequality-white.csv')
)

display(white_wine_df)

# COMMAND ----------

red_wine_df = red_wine_df.withColumn('wine_type', lit('red'))
red_wine_df.show()

# COMMAND ----------

white_wine_df = white_wine_df.withColumn('wine_type', lit('white'))
white_wine_df.show()

# COMMAND ----------

combined_wines = red_wine_df.union(white_wine_df)
display(combined_wines)

# COMMAND ----------

combined_wines = combined_wines.withColumnRenamed('quality', 'nota')
display(combined_wines)

# COMMAND ----------

(
    combined_wines
        .select(['nota', 'wine_type'])
        .show()
)

# COMMAND ----------

(
    combined_wines
        .groupBy(['nota', 'wine_type'])
        .count()
        .show()
)

# COMMAND ----------

combined_wines.printSchema()

# COMMAND ----------

(
    combined_wines
        .write
        .option('header', True)
        .mode('overwrite')
        .csv('/FileStore/tables/aula-databricks/vinhos/pyspark')
)

# COMMAND ----------


