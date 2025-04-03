# Databricks notebook source
lookuptables_rules = {
    "rule1": "show_id is not null"
}

# COMMAND ----------

import dlt

@dlt.table(
    name="gold_netflix_directors"
)
@dlt.expect_all_or_drop(lookuptables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("mnt/silver/netflix_directors")
    return df

# COMMAND ----------

import dlt

@dlt.table(
    name="gold_netflix_cast"
)
@dlt.expect_all_or_drop(lookuptables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("mnt/silver/netflix_cast")
    return df

# COMMAND ----------

import dlt

@dlt.table(
    name="gold_netflix_countries"
)
@dlt.expect_all_or_drop(lookuptables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("mnt/silver/netflix_countries")
    return df

# COMMAND ----------

import dlt

@dlt.table(
    name="gold_netflix_category"
)
@dlt.expect_all_or_drop(lookuptables_rules)
def myfunc():
    df = spark.readStream.format("delta").load("mnt/silver/netflix_category")
    return df

# COMMAND ----------

import dlt

@dlt.table(
    name="gold_stg_netflixtitles"
)

def myfunc():
    df = spark.readStream.format("delta").load("mnt/silver/netflix_titles")
    return df

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

@dlt.view

def gold_trans_netflixtitles():
    df = spark.readStream.table("LIVE.gold_stg_netflixtitles")
    df = df.withColumn("new_flag",lit(1))
    return df


# COMMAND ----------

master_rules = {
    "rule1": "show_id is not null",
    "rule2": "new_flag is not null"
}

# COMMAND ----------

import dlt

@dlt.table
@dlt.expect_all_or_drop(master_rules)
def gold_netflix_titles():
    df = spark.readStream.table("LIVE.gold_trans_netflixtitles")
    return df