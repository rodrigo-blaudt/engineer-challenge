# Databricks notebook source
file_path = "dbfs:/Volumes/workspace/default/vol/posts_creator.json.gz"
table_name = "default.posts_creator"

# COMMAND ----------

df = spark.read.json(file_path)

# COMMAND ----------

df.write.format("delta").saveAsTable(table_name)