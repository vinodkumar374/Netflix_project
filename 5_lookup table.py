# Databricks notebook source
dbutils.widgets.text("weekdays", "7")

# COMMAND ----------

var = int(dbutils.widgets.get("weekdays"))

# COMMAND ----------

print(var)

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "weekdaysoutput", value = var)