# Databricks notebook source
#criando um dataframe com dados fixos
dados =[('Grimaldo ','Oliveira','Brasileira','Professor','M',3000),
('Ana ','Santos','Portuguesa','Atriz','F',4000),
('Roberto ','Carlos','Brasileira','Analista','M',4000),
('Maria ','Santanna','Italiana','Dentista','F',6000),
('Sonia ','Faro','Brasileira','Advogada','F',8000),
('Anderson ','Faro','Brasileira','Analista de Dados','M',16000),
('Jeane ','Andrade','Portuguesa','Medica','F',7000)]

colunas=['Primeiro_Nome','Ultimo_nome','Nacionalidade','Trabalho','Genero','Salario']

datafparquet=spark.createDataFrame(dados,colunas)
datafparquet.show()

# COMMAND ----------

#Particionando os dados em um arquivo parquet
datafparquet.write.partitionBy("Nacionalidade","salario").mode("overwrite").parquet("/FileStore/tables/parquet/pessoal.parquet")

# COMMAND ----------

#Consultando diretamente o arquivo parquet particionado via SQL
spark.sql( ''' 
            CREATE OR REPLACE TEMPORARY VIEW Cidadao USING parquet OPTIONS (path 
            '/FileStore/tables/parquet/pessoal.parquet/Nacionalidade=Brasileira')
            ''')
spark.sql("SELECT * FROM Cidadao where Ultimo_nome='Faro'").show()

# COMMAND ----------

spark.sql("SELECT * FROM Cidadao").show()

# COMMAND ----------

