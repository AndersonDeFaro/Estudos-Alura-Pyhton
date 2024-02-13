# Databricks notebook source
local_do_arquivo = '/FileStore/cursos_prouni.csv'

# COMMAND ----------

import pyspark.pandas as ps

df = ps.read_csv(local_do_arquivo, index_col='curso_id')

# COMMAND ----------

df.head()

# COMMAND ----------

df.shape

# COMMAND ----------

df['curso_busca'].equals(df['nome']).sum()

# COMMAND ----------

df = df.drop(['curso_busca'], axis=1)

# COMMAND ----------

df.head()

# COMMAND ----------

df = df.rename(columns={'nome':'nome_curso'})

# COMMAND ----------

df.head()

# COMMAND ----------

df[df['nome_curso']=='Medicina']['nome_curso'].count()

# COMMAND ----------

df['turno'].unique()

# COMMAND ----------

df[(df['turno'=='Integral']) & (df['nome_curso']=='Medicina')]['nome_curso'].count()

# COMMAND ----------

