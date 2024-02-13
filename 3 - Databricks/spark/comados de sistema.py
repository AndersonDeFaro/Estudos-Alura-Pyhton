# Databricks notebook source
# Todo comando linux para rodar no Databricks basta colocar
# o prefixo "%fs" ex: 
# %fs ls 
# %fs mkdir
# %fs rm OU %fs rm -r
# entre outros
%fs ls /FileStore/tables

# COMMAND ----------

# Execução de scripts linux
# Basta colocar a nomenclatura inicial: %%bash

%%bash 
find /databricks -name "*.csv" | grep "fa"

# COMMAND ----------

# MAGIC %%bash
# MAGIC crontab -l

# COMMAND ----------

# MAGIC %%bash
# MAGIC java -version

# COMMAND ----------

