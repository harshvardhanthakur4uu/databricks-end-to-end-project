# Databricks notebook source
# MAGIC %md
# MAGIC ## **INCREMENTAL DATA INGESTION**

# COMMAND ----------

dbutils.widgets.text("source","")

# COMMAND ----------

source_val = dbutils.widgets.get("source")
source_val

# COMMAND ----------


df = spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format", "csv")\
        .option("cloudFiles.schemaLocation", f"/Volumes/flights_catalog/bronze/bronzevolume/{source_val}/checkpoint")\
        .option("cloudFiles.schemaEvolutionMode", "rescue")\
        .load(f"/Volumes/flights_catalog/raw/rawvolume/rawdata/{source_val}/")

# COMMAND ----------

df.writeStream.format("delta")\
        .outputMode("append")\
        .trigger(once=True)\
        .option("checkpointLocation", f"/Volumes/flights_catalog/bronze/bronzevolume/{source_val}/checkpoint")\
        .option("path", f"/Volumes/flights_catalog/bronze/bronzevolume/{source_val}/data")\
        .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/flights_catalog/bronze/bronzevolume/flights/data`

# COMMAND ----------

