# Databricks notebook source
dbutils.fs.ls("/mnt/dados/raw")

# COMMAND ----------

path = "dbfs:/mnt/dados/raw/dados_brutos_imoveis.json"
dados = spark.read.json(path)

# COMMAND ----------

display(dados)

# COMMAND ----------

dados = dados.drop("imagens", "usuario")
display(dados)

# COMMAND ----------

from pyspark.sql.functions import col, column   

# COMMAND ----------

dados = dados.withColumn("id", col("anuncio.id"))
display(dados)

# COMMAND ----------

path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
dados.write.format("delta").mode("overwrite").save(path)
