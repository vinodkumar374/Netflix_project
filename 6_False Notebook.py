# Databricks notebook source
var = dbutils.jobs.taskValues.get(
    taskKey="lookuptables", 
    key="weekdaysoutput", 
    debugValue="default_value"
)

# COMMAND ----------

print(var)  