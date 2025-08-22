# Databricks notebook source
import logging
import pandas as pd
import json
import re
import requests

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# COMMAND ----------

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# COMMAND ----------

endpoint = "https://en.wikipedia.org/w/api.php"

table_input = 'creators_scrape_wiki'
table_output = "users_yt"

youtube_user_id_regex_pattern = r"https://www.youtube.com/user/([\w-]+)"

# COMMAND ----------

def extract_youtube_user_id_from_text(regex_pattern, text):
    try:
        matches = re.findall(regex_pattern, text)
        if not matches:
            return None
        return matches[0]
    except Exception as e:
        logger.error(f"Extract youtube user_id error: {e}")
        return None

# COMMAND ----------

def extract_youtube_user_id(wiki_name):
    page_name = wiki_name
    params = {"action": "parse", "page": f"{page_name}", "format": "json"}

    try:
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        data = response.json()

        if "parse" in data and "text" in data["parse"]:
            html_text = json.dumps(data["parse"]["text"]["*"])
            user_id = extract_youtube_user_id_from_text(youtube_user_id_regex_pattern, html_text)
            return user_id

    except requests.exceptions.RequestException as e:
        logger.error(f"Request error {page_name}: {e}")
        return None

    except requests.exceptions.JSONDecodeError as e:
        logger.error(f"JSON error: '{page_name}': {e}")
        return None

# COMMAND ----------

df_wiki_page = spark.read.table(table_input)

youtube_user_id_udf = udf(extract_youtube_user_id, StringType())
df_users_yt = df_wiki_page.withColumn("user_id", youtube_user_id_udf(df_wiki_page["wiki_page"]))
df_users_yt.write.saveAsTable(table_output)

# COMMAND ----------

df_users_yt.show()

# COMMAND ----------

file_path = "dbfs:/Volumes/workspace/default/vol/wiki_page,user_id.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.write.mode("overwrite").saveAsTable(table_output)

# COMMAND ----------

cc = spark.read.table(table_output)
cc.show()