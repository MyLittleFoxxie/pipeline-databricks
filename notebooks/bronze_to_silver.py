# Databricks notebook source
dbutils.fs.ls("/mnt/dados/bronze")

# COMMAND ----------

path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df = spark.read.format("delta").load(path)

# COMMAND ----------

display(df)

# COMMAND ----------

display(
    df.select("anuncio.*", "anuncio.endereco.*")
)

# COMMAND ----------

df = df.select("anuncio.*", "anuncio.endereco.*")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("caracteristicas", "endereco")

# COMMAND ----------

display(df)

# COMMAND ----------

path = "/mnt/dados/silver/dataset_imoveis"
df.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/silver/dataset_imoveis")

# COMMAND ----------

df.columns

# COMMAND ----------


