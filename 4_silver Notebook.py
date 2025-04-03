# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Notebook Trasformations

# COMMAND ----------

df = spark.read.format("delta").load("/mnt/bronze/netflix_titles")
display(df) 

# COMMAND ----------

from pyspark.sql.functions import avg,floor,lpad

avg_duration = df.select(avg(df["duration_minutes"])).first()[0]
df1 = df.fillna({"duration_minutes": avg_duration, "duration_seasons": "1"})
display(df1)

# COMMAND ----------

df1 = df1.withColumn("duration_minutes", floor(df1["duration_minutes"]))

display(df1)

# COMMAND ----------

from pyspark.sql.functions import to_date

df2 = df1.withColumn("duration_seasons", df1.duration_seasons.cast("int")).withColumn("release_year", df1.release_year.cast("int")).withColumn("show_id", df1.show_id.cast("int"))
display(df2)

# COMMAND ----------

df3 = df2.fillna({"show_id": 80125464})
display(df3)

# COMMAND ----------

from pyspark.sql.functions import split, col
df4 = df3.withColumn("tile_page", split(col("title"),":").getItem(0))\
       .withColumn("rating",split(col("rating"),"-").getItem(0))
display(df4)

# COMMAND ----------

df5 = df4.drop("_rescued_data")
display(df5)

# COMMAND ----------

from pyspark.sql.functions import when
df6 = df5.withColumn("type_number", when(df5.type == "TV Show", 1).when(df5.type == "Movie", 2).otherwise(0).cast("int"))
display(df6)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, col

df7 = df6.withColumn("duration_ranking", dense_rank().over (Window.orderBy(col("duration_minutes").desc())))
display(df7)

# COMMAND ----------

from pyspark.sql.functions import count
df8 = df7.groupBy("type").agg(count("type").alias("total_count"))
display(df8)

# COMMAND ----------

df9 = df7.dropna()
display(df9)

# COMMAND ----------

df9.write.format("delta").mode("overwrite").save("/mnt/silver/netflix_titles")