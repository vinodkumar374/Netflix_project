# Databricks notebook source
# MAGIC %md
# MAGIC ### silver notebook lookup tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameter

# COMMAND ----------

dbutils.widgets.text("source_folder", "netflix_directors")
dbutils.widgets.text("target_folder", "netflix_directors")

# COMMAND ----------

# MAGIC %md
# MAGIC #Variable

# COMMAND ----------

var_source_folder = dbutils.widgets.get("source_folder")
var_target_folder = dbutils.widgets.get("target_folder")

# COMMAND ----------

print(var_source_folder)

# COMMAND ----------

df = spark.read.csv(f"/mnt/bronze/{var_source_folder}", header = True)
display(df)

# COMMAND ----------

df.write.format("delta").option("mergeSchema", "true").mode("append").save(f"/mnt/silver/{var_target_folder}")