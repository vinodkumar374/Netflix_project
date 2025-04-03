# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader Streaming Data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema netflix_net_schema

# COMMAND ----------

dbutils.fs.mount(
  source ="wasbs://raw@vinodstorage001.blob.core.windows.net",
  mount_point ="/mnt/raw/" ,
  extra_configs ={"fs.azure.account.key.vinodstorage001.blob.core.windows.net":"vfe/qBjIpFiEg1cUGft60SH3IAl90xnVV4gQ1MvZHz3JUBwt3NeAn5tqCNQhtDz8I3xjaCa6hN4r+AStLDXEwQ=="} )

# COMMAND ----------


# COMMAND ----------

checkpoint_path = "/mnt/silver/checkpoint_path"

# COMMAND ----------

df = spark.readStream.format("cloudfiles").option("cloudfiles.format", "csv")\
    .option("cloudFiles.schemaLocation", checkpoint_path)\
    .load("/mnt/raw/")
display(df)

# COMMAND ----------

df.writeStream.option("checkpointLocation", checkpoint_path)\
    .trigger(processingTime="10 seconds")\
    .start("/mnt/bronze/netflix_titles")