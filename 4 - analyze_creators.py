# Databricks notebook source
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import from_unixtime

# COMMAND ----------

# default.users_yt com a default.posts_creator
df_users_yt = spark.read.table("users_yt")
df_posts_creator = spark.read.table("posts_creator")
df_joined = df_users_yt.join(df_posts_creator, df_users_yt.user_id == df_posts_creator.yt_user, "inner")
display(df_joined)

# COMMAND ----------

# Mostrar o top 3 posts ordenado por likes de cada creator nos últimos 6 meses (user_id, title, likes, rank)

def filter_by_month_interval(df, month_interval):
    try:
        df = df.withColumn("published_at_ts", from_unixtime("published_at"))
        df_filtered = df.filter(F.col("published_at_ts") >= F.add_months(F.current_date(), month_interval))
        return df_filtered
    except Exception as e:
        print(f"Failed to filter df by month interval: {e}")


def get_top_3_posts_by_attribute_creator(df, attribute):
    try:
        window_by_creator = Window.partitionBy("user_id").orderBy(F.col(attribute).desc())
        df_ranked = df.withColumn("rank", F.rank().over(window_by_creator))
        df_ranked_top_3 = df_ranked.filter(F.col("rank") <= 3)
        return df_ranked_top_3.select("yt_user", "title", attribute, "rank")
    except Exception as e:
        print(f"Failed to get top 3 posts by view: {e}")

df_filtered = filter_by_month_interval(df_joined, -24)

df_top_3_posts_by_view_creator = get_top_3_posts_by_attribute_creator(df_filtered, "views")
df_top_3_posts_by_likes_creator = get_top_3_posts_by_attribute_creator(df_filtered, "likes")

# COMMAND ----------

# Mostrar os yt_user que estão na tabela default.post_creator mas não estão na tabela default.users_yt

def get_missing_users(df_posts_creator, df_users_yt):
    try:
        df_missing_users = df_posts_creator.join(df_users_yt, df_posts_creator.yt_user == df_users_yt.user_id, "left_anti")
        return df_missing_users.select("yt_user").distinct()
    except Exception as e:
        print(f"Failed to get missing_users: {e}")

df_missing_users = get_missing_users(df_posts_creator, df_users_yt)
display(df_missing_users)

# COMMAND ----------

# Mostrar a quantidade de publicações por mês de cada creator.

def get_count_creator_monthly_posts(df_posts_creator):
    try:
        df_posts_creator = df_posts_creator.withColumn("published_at_ts", from_unixtime("published_at"))
        df_count_creator_monthly_posts = df_posts_creator.groupBy("yt_user", F.month("published_at_ts")).count().orderBy("yt_user", F.month("published_at_ts"))
        return df_count_creator_monthly_posts
    except Exception as e:
        print(f"Failed to {e}")


df_count_creator_monthly_posts = get_count_creator_monthly_posts(df_posts_creator)
display(df_count_creator_monthly_posts)

# COMMAND ----------

# Exercício Extra 1: mostrar 0 nos meses que não tem video - V1

def get_creator_monthly_posts_with_zeros(df_posts_creator):
    try:
        df_posts_creator = df_posts_creator.withColumn("month", F.month(from_unixtime("published_at")))

        years_months = df_posts_creator.select("month").distinct()
        user_ids = df_posts_creator.select("yt_user").distinct()

        df_cross = user_ids.crossJoin(years_months)
        df_count_monthly_posts = df_posts_creator.groupBy("yt_user", "month").count()

        df_join = df_cross.join(df_count_monthly_posts, ["yt_user", "month"], "left_outer") \
                          .fillna(0) \
                          .orderBy("yt_user", "month")

        return df_join
    except Exception as e:
        print(f"Failed to get monthly posts with zeros:{e}")

df = get_creator_monthly_posts_with_zeros(df_posts_creator)
display(df)

# COMMAND ----------

# Exercício Extra 2: transformar a tabela no formato que a primeira coluna é o user_id e temos uma coluna para cada mês.
# ex:
# user_id, 2024/01, 2024/02, 2024/03
# felipeneto, 10, 20, 30
# lucasneto, 5, 10, 15

def transform_df_pivot(df):
    try:
        df_pivot = df.groupBy("yt_user") \
                    .pivot("month(published_at_ts)") \
                    .agg(F.first("count")) \
                    .fillna(0)

        return df_pivot
    except Exception as e:
        print(f"Failed to pivot: {e}")

df_pivot = transform_df_pivot(df_count_creator_monthly_posts)
display(df_pivot)

# COMMAND ----------

# Exercício Extra 1: mostrar 0 nos meses que não tem video

def get_creator_monthly_posts_with_zeros(df_posts_creator):
    try:
        df_pivot = df_posts_creator.groupBy("yt_user") \
                    .pivot("month(published_at_ts)") \
                    .agg(F.first("count")) \
                    .fillna(0)

        df_unpivot = df_pivot.selectExpr(
            "yt_user",
            """stack(12,
            '1', `1`, '2', `2`, '3', `3`, '4', `4`, 
            '5', `5`, '6', `6`, '7', `7`, '8', `8`,
            '9', `9`, '10', `10`, '11', `11`, '12', `12`) as (month, count)"""
        )

        return df_unpivot
    except Exception as e:
        print(f"Failed to pivot: {e}")


df_creator_monthly_posts_with_zeros = get_creator_monthly_posts_with_zeros(df_count_creator_monthly_posts)
display(df_creator_monthly_posts_with_zeros)

# COMMAND ----------

# Exercício Extra 3: Mostrar as 3 tags mais utilizadas por criador de conteúdo

def get_top_3_tags(df_posts_creator):
    df_exploded_tags = df_posts_creator.select("creator_id", F.explode("tags").alias("tag"))
    df_count_tag = df_exploded_tags.groupBy("creator_id", "tag").count()

    window = Window.partitionBy("creator_id").orderBy(F.desc("count"))
    df_top_3_tags = df_count_tag.withColumn("rank", F.rank().over(window))
    return df_top_3_tags.filter(F.col("rank") <= 3).orderBy("creator_id", "rank")

df_top_3_tags = get_top_3_tags(df_posts_creator)
display(df_top_3_tags) 