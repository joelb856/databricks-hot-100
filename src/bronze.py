# Databricks notebook source
from pyspark.sql import functions as F
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/hot100/raw/landing/_schema")
    .load("/Volumes/hot100/raw/landing/")
    .withColumns({
        "_ingest_time": F.current_timestamp(),
        "_source_file": F.col("_metadata.file_path")
        })
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/hot100/raw/landing/_checkpoint")
    .trigger(availableNow=True)
    .toTable("hot100.raw.bronze"))