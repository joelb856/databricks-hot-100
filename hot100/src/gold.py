# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F

# COMMAND ----------
weekly_affect = (
    spark.table('hot100.processed.silver')
    .filter(F.col('language') == 'en')
    .groupBy('date')
    .agg(
        F.count('*').alias('n_songs'),
        F.avg('n_words').alias('avg_n_words'),
        F.avg('positive').alias('avg_positive'),
        F.avg('negative').alias('avg_negative'),
        F.avg('anger').alias('avg_anger'),
        F.avg('anticipation').alias('avg_anticipation'),
        F.avg('disgust').alias('avg_disgust'),
        F.avg('fear').alias('avg_fear'),
        F.avg('joy').alias('avg_joy'),
        F.avg('sadness').alias('avg_sadness'),
        F.avg('surprise').alias('avg_surprise'),
        F.avg('trust').alias('avg_trust'),
    )
)

# COMMAND ----------
"""
Silver -> Gold
Using calculated averages, create temp view for staging then upsert into gold
"""

spark.sql("""
    CREATE TABLE IF NOT EXISTS hot100.serving.gold (
        date             DATE,
        n_songs          INT,
        avg_n_words      DOUBLE,
        avg_positive     DOUBLE,
        avg_negative     DOUBLE,
        avg_anger        DOUBLE,
        avg_anticipation DOUBLE,
        avg_disgust      DOUBLE,
        avg_fear         DOUBLE,
        avg_joy          DOUBLE,
        avg_sadness      DOUBLE,
        avg_surprise     DOUBLE,
        avg_trust        DOUBLE
    )
""")

weekly_affect.createOrReplaceTempView('weekly_affect_staging')

spark.sql("""
    MERGE INTO hot100.serving.gold AS target
    USING weekly_affect_staging AS source
    ON target.date = source.date
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")