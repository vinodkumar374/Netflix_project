# Databricks notebook source
# MAGIC %md
# MAGIC # Array Parameter

# COMMAND ----------

files = [
    {
        "source_folder" : "netflix_directors",
        "target_folder" : "netflix_directors"
        
    },
    {
        "source_folder" : "netflix_cast",
        "target_folder" : "netflix_cast"
        
    },
    {
        "source_folder" : "netflix_category",
        "target_folder" : "netflix_category"
        
    },
    {
        "source_folder" : "netflix_countries",
        "target_folder" : "netflix_countries"
        
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC #Job utility to run array of files as parameter

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "My_array", value = files)
