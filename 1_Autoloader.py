# Databricks notebook source
# MAGIC %md
# MAGIC # Autoloader Streaming Data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema netflix_net_schema

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