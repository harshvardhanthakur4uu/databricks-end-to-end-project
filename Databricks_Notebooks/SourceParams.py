# Databricks notebook source
source_array = [
    {"source" : "bookings"},
    {"source" : "flights"},
    {"source" : "airports"},
    {"source" : "customers"}
]

# COMMAND ----------

dbutils.jobs.taskValues.set(key="output_key", value=source_array)

# COMMAND ----------

