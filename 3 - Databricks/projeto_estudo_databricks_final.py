# Databricks notebook source
# MAGIC %md
# MAGIC # Projeto Final - Bike Sharing

# COMMAND ----------

# MAGIC %md
# MAGIC ## dbutils

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Localize o dataset bikeSharing dentro do diretório de datasets do Databricks

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/bikeSharing/'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) Acesse o diretório do projeto bikeSharing e avalie o seu conteúdo

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets/bikeSharing/data-001/'))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) Visualize o conteúdo dos arquivos do projeto

# COMMAND ----------

display(dbutils.fs.head('/databricks-datasets/bikeSharing/data-001/day.csv'))

# COMMAND ----------

display(dbutils.fs.head('/databricks-datasets/bikeSharing/data-001/hour.csv'))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) Crie uma cópia do projeto bikeSharing para o seu diretório particular
# MAGIC
# MAGIC - Dentro do diretório aula-databricks crie um diretório chamado "projeto_final"
# MAGIC - Dentro do diretório "projeto_final" crie o diretório "bikeSharing"
# MAGIC - Copie o conteúdo do diretório "/databricks-datasets/bikeSharing/" para o diretório "/FileStore/tables/aula-databricks/projeto_final/bikeSharing/"
# MAGIC - Visualize o conteúdo de "/FileStore/tables/aula-databricks/projeto_final/bikeSharing/"

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/tables/aula-databricks/projeto_final/bikeSharing')

# COMMAND ----------

dbutils.fs.cp(
    '/databricks-datasets/bikeSharing', 
    '/FileStore/tables/aula-databricks/projeto_final/bikeSharing/', 
    recurse=True
)

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/aula-databricks/projeto_final/bikeSharing'))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usando SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5) Crie um database chamado "projeto_final"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS projeto_final

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6) Altere o contexto do banco de dados para o database "projeto_final"

# COMMAND ----------

# MAGIC %sql
# MAGIC use projeto_final

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7) Crie uma tabela chamada "dados_dia" e preencha com o conteúdo do arquivo "/FileStore/tables/aula-databricks/projeto_final/bikeSharing/data-001/day.csv"
# MAGIC
# MAGIC - O dataset day.csv possui o seguinte conjunto de atributos:
# MAGIC
# MAGIC | nome | tipo |
# MAGIC |------|------|
# MAGIC | instant | int |
# MAGIC | dteday | timestamp |
# MAGIC | season | int |
# MAGIC | yr | int |
# MAGIC | mnth | int |
# MAGIC | holiday | int |
# MAGIC | weekday | int |
# MAGIC | workingday | int |
# MAGIC | weathersit | int |
# MAGIC | temp | float |
# MAGIC | atemp | float |
# MAGIC | hum | float |
# MAGIC | windspeed | float |
# MAGIC | casual | int |
# MAGIC | registered | int |
# MAGIC | cnt | int |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dados_dia(
# MAGIC   instant	int,
# MAGIC dteday	timestamp,
# MAGIC season	int,
# MAGIC yr	int,
# MAGIC mnth	int,
# MAGIC holiday	int,
# MAGIC weekday	int,
# MAGIC workingday	int,
# MAGIC weathersit	int,
# MAGIC temp	float,
# MAGIC atemp	float,
# MAGIC hum	float,
# MAGIC windspeed	float,
# MAGIC casual	int,
# MAGIC registered	int,
# MAGIC cnt	int
# MAGIC )
# MAGIC    USING CSV
# MAGIC         OPTIONS (
# MAGIC             path '/FileStore/tables/aula-databricks/projeto_final/bikeSharing/data-001/day.csv',
# MAGIC             header 'true',
# MAGIC             delimiter ','
# MAGIC         )

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8) Visualize os dez primeiros registros da tabela "dados_dia"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from dados_dia
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9) Crie uma tabela chamada "dados_hora" e preencha com o conteúdo do arquivo "/FileStore/tables/aula-databricks/projeto_final/bikeSharing/data-001/hour.csv"
# MAGIC
# MAGIC - O dataset hour.csv possui o seguinte conjunto de atributos:
# MAGIC
# MAGIC | nome | tipo |
# MAGIC |------|------|
# MAGIC | instant | int |
# MAGIC | dteday | timestamp |
# MAGIC | season | int |
# MAGIC | yr | int |
# MAGIC | mnth | int |
# MAGIC | hr | int |
# MAGIC | holiday | int |
# MAGIC | weekday | int |
# MAGIC | workingday | int |
# MAGIC | weathersit | int |
# MAGIC | temp | float |
# MAGIC | atemp | float |
# MAGIC | hum | float |
# MAGIC | windspeed | float |
# MAGIC | casual | int |
# MAGIC | registered | int |
# MAGIC | cnt | int |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dados_hora(
# MAGIC   instant	int,
# MAGIC dteday	timestamp,
# MAGIC season	int,
# MAGIC yr	int,
# MAGIC mnth	int,
# MAGIC hr	int,
# MAGIC holiday	int,
# MAGIC weekday	int,
# MAGIC workingday	int,
# MAGIC weathersit	int,
# MAGIC temp	float,
# MAGIC atemp	float,
# MAGIC hum	float,
# MAGIC windspeed	float,
# MAGIC casual	int,
# MAGIC registered	int,
# MAGIC cnt	int
# MAGIC )
# MAGIC    USING CSV
# MAGIC         OPTIONS (
# MAGIC             path '/FileStore/tables/aula-databricks/projeto_final/bikeSharing/data-001/hour.csv',
# MAGIC             header 'true',
# MAGIC             delimiter ','
# MAGIC         )

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10) Visualize os dez primeiros registros da tabela "dados_dia"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from dados_hora
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explorando os dados

# COMMAND ----------

# MAGIC %md
# MAGIC ### Informações dos atributos do dataset Bike Sharing
# MAGIC [Fonte](http://archive.ics.uci.edu/ml/datasets/Bike+Sharing+Dataset)
# MAGIC
# MAGIC Tanto hour.csv quanto day.csv têm os seguintes campos, exceto hr, que não está disponível em day.csv
# MAGIC
# MAGIC - **instant**: índice de registro
# MAGIC - **dteday** : data
# MAGIC - **season** : estação (1: inverno, 2: primavera, 3: verão, 4: outono)
# MAGIC - **yr**: ano (0: 2011, 1: 2012)
# MAGIC - **mnth**: mês (1 a 12)
# MAGIC - **hr** : hora (0 a 23)
# MAGIC - **holiday**  : se o dia é feriado ou não (extraído de [Web Link])
# MAGIC - **weekday** : dia da semana
# MAGIC - **workingday** : se o dia não for nem fim de semana nem feriado é 1, caso contrário é 0.
# MAGIC - **weathersit**: condições climáticas
# MAGIC     - 1: Claro, Poucas nuvens, Parcialmente nublado, Parcialmente nublado
# MAGIC     - 2: Névoa + Nublado, Névoa + Nuvens quebradas, Névoa + Poucas nuvens, Névoa
# MAGIC     - 3: Neve fraca, Chuva fraca + Trovoada + Nuvens dispersas, Chuva fraca + Nuvens dispersas
# MAGIC     - 4: Chuva Forte + Paletes de Gelo + Tempestade + Névoa, Neve + Neblina
# MAGIC - **temp** : Temperatura normalizada em Celsius. Os valores são derivados via (t-t_min)/(t_max-t_min), t_min=-8, t_max=+39 (somente na escala horária)
# MAGIC - **atemp**: Sensação térmica normalizada em Celsius. Os valores são derivados via (t-t_min)/(t_max-t_min), t_min=-16, t_max=+50 (somente na escala horária)
# MAGIC - **hum**: Umidade normalizada. Os valores são divididos por 100 (máx)
# MAGIC - **windspeed**: velocidade do vento normalizada. Os valores são divididos por 67 (máx)
# MAGIC - **casual**: contagem de usuários casuais
# MAGIC - **registered**: contagem de usuários registrados
# MAGIC - **cnt**: contagem do total de bicicletas alugadas, incluindo bicicletas casuais e registradas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11) Crie uma tabela de frequências com o somatório total de usuário casuais e registrados por estações do ano. Ordene o resultado segundo as estações do ano
# MAGIC - Tanto faz utilizar a tabel dados_dia ou dados_hora neste exercícios, o resultado seria o mesmo

# COMMAND ----------

# MAGIC %sql
# MAGIC select season, sum(casual) total
# MAGIC from dados_dia
# MAGIC group by season

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12) Crie uma tabela de frequências com o somatório total de usuário casuais e registrados por ano e estações do ano. Ordene o resultado segundo os atributos ano e estações do ano
# MAGIC - Tanto faz utilizar a tabel dados_dia ou dados_hora neste exercícios, o resultado seria o mesmo

# COMMAND ----------

# MAGIC %sql
# MAGIC select yr, season , sum(casual) vltotal_casual, sum(cnt) vltotal
# MAGIC from dados_dia
# MAGIC group by  yr, season

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13) Crie uma tabela de frequências com o somatório total de usuários (casuais e registrados) segundo o atributo hora. Ordene o resultado segundo o atributo hora
# MAGIC - Aqui devemos utilizar a tabela dados_hora

# COMMAND ----------

# MAGIC %sql
# MAGIC select hr, sum(cnt) vltotal
# MAGIC from dados_hora
# MAGIC group by hr
# MAGIC order by hr

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14) Explore o dataset livremente. Crie as elaborações que julgar interessante para entender melhor o dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15) Crie uma Temp View apenas com os registros da tabela "dados_dia" para o ano de 2011. Chame a nova Temp View de "nova_tabela"

# COMMAND ----------

resultado = spark.sql('''
          select *
          from dados_dia
          where yr = 0
          ''')

# COMMAND ----------

resultado.createTempView('nova_tabela')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16) Utilizando a TempView "nova_tabela" crie uma tabela de frequências que mostra o total de aluguéis de biciletas segundo os atributos "weathersit" e "workingday". Ordene o resultado segundos estes atributos também.

# COMMAND ----------

spark.sql('''
          select weathersit, workingday, sum(cnt) vltotal
          from nova_tabela 
          group by weathersit, workingday
          order by weathersit, workingday
          ''').show()

# COMMAND ----------


