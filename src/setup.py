# Databricks notebook source
# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS hot100;
# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS hot100.raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS hot100.processed;
# MAGIC CREATE SCHEMA IF NOT EXISTS hot100.serving
# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS hot100.raw.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS hot100.processed.checkpoints;
# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS hot100.raw.bronze;
# MAGIC CREATE TABLE IF NOT EXISTS hot100.processed.silver;
# MAGIC CREATE TABLE IF NOT EXISTS hot100.serving.gold;