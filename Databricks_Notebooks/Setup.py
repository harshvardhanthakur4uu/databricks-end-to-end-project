# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE VOLUME flights_catalog.raw.rawvolume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/flights_catalog/raw/rawvolume/rawdata/bookings")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/flights_catalog/raw/rawvolume/rawdata/flights")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/flights_catalog/raw/rawvolume/rawdata/customers")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/flights_catalog/raw/rawvolume/rawdata/airports")

# COMMAND ----------

