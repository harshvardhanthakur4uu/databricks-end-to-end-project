# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
        .load("/Volumes/flights_catalog/bronze/bronzevolume/bookings/data/")
display(df)

# COMMAND ----------

df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From flights_catalog.silver.silver_bookings

# COMMAND ----------

df2 = spark.read.format("delta")\
        .load("/Volumes/flights_catalog/bronze/bronzevolume/flights/data/")

df2 = df2.withColumn("flight_date", to_date(col("flight_date")))\
        .drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())
display(df2)

# COMMAND ----------

df3 = spark.read.format("delta")\
        .load("/Volumes/flights_catalog/bronze/bronzevolume/customers/data/")
df3 = df3.drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())
display(df3)

# COMMAND ----------

df4 = spark.read.format("delta")\
        .load("/Volumes/flights_catalog/bronze/bronzevolume/airports/data/")
df4 = df4.drop("_rescued_data")\
            .withColumn("modifiedDate", current_timestamp())

display(df4)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * From flights_catalog.silver.silver_business;

# COMMAND ----------

