# Databricks notebook source
file_path = "dbfs:/Volumes/workspace/default/vol/wiki_pages.json.gz"
table_name = "default.creators_scrape_wiki"

# COMMAND ----------

df = spark.read.json("dbfs:/Volumes/workspace/default/vol/wiki_pages.json.gz")

# COMMAND ----------

df.write.format("delta").saveAsTable(table_name)