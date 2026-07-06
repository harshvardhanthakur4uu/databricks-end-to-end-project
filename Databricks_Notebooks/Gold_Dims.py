# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flights_catalog.silver.silver_passengers

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Parameters**

# COMMAND ----------

# # Key columns
# dbutils.widgets.text("keycols","")

# # CDC columns
# dbutils.widgets.text("cdccol","")

# # BackDate Refresh
# dbutils.widgets.text("backdate_refresh","")

# # Scoure Object
# dbutils.widgets.text("source_obj","")

# # Scoure Schema
# dbutils.widgets.text("source_sch","")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Fetching Parameters & Creating Variables**

# COMMAND ----------

# # Key cols list
# key_cols = dbutils.widgets.get("keycols")
# key_cols_list = eval(key_cols)

# # CDC col
# cdc_col = dbutils.widgets.get("cdccol")

# # BackDate Refresh
# backdate_refresh = dbutils.widgets.get("backdate_refresh")

# # Source Object
# source_obj = dbutils.widgets.get("source_obj")

# # Source Schema
# source_sch = dbutils.widgets.get("source_sch")


# COMMAND ----------

# # Dim Flights

# key_cols = "['flight_id']"
# key_cols_list = eval(key_cols)

# cdc_col = "modifiedDate"

# backdate_refresh = ""

# catalog = "flights_catalog"

# source_obj = "silver_flights"

# source_sch = "silver"

# target_sch = "gold"

# target_obj = "dim_flights"

# # Surrogate key
# surrogate_key = "DimFlightsKey"

# COMMAND ----------

# # Dim Airports

# key_cols = "['airport_id']"
# key_cols_list = eval(key_cols)

# cdc_col = "modifiedDate"

# backdate_refresh = ""

# catalog = "flights_catalog"

# source_obj = "silver_airports"

# source_sch = "silver"

# target_sch = "gold"

# target_obj = "dim_airports"

# # Surrogate key
# surrogate_key = "DimAirportsKey"

# COMMAND ----------

# Dim Passengers

key_cols = "['passenger_id']"
key_cols_list = eval(key_cols)

cdc_col = "modifiedDate"

backdate_refresh = ""

catalog = "flights_catalog"

source_obj = "silver_passengers"

source_sch = "silver"

target_sch = "gold"

target_obj = "dim_passengers"

# Surrogate key
surrogate_key = "DimPassengersKey"

# COMMAND ----------

# MAGIC %md
# MAGIC ### **INCREMENTAL DATA INGESTION**

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Last Load Date**

# COMMAND ----------

# No BackDate Refresh
if len(backdate_refresh) == 0:
    # Check if table exists
    if spark.catalog.tableExists(f"{catalog}.{target_sch}.{target_obj}"):
        last_load = spark.sql(f"SELECT MAX({cdc_col}) FROM {catalog}.{target_sch}.{target_obj}").collect()[0][0]
    else:
        last_load = "1900-01-01 00:00:00"
# Yes BackDate Refresh
else:
    last_load = backdate_refresh

last_load

# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {catalog}.{source_sch}.{source_obj} WHERE {cdc_col} > '{last_load}'")
display(df_src)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Old vs New Records**

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_sch}.{target_obj}"):
    # key col string for incremental
    key_cols_str_inc = ", ".join(key_cols_list)

    df_trg = spark.sql(f"""SELECT {key_cols_str_inc},{surrogate_key}, create_date, update_date FROM {catalog}.{target_sch}.{target_obj}""")     
     
else:
    # key col string for initial
    key_cols_str_init = [f"'' as {i}" for i in key_cols_list]
    key_cols_str_init = ", ".join(key_cols_str_init)
    
    df_trg = spark.sql(f"SELECT {key_cols_str_init}, CAST('0' AS INT) as {surrogate_key}, CAST('1900-01-01 00:00:00' AS timestamp) AS create_date, CAST('1900-01-01 00:00:00' AS timestamp) AS update_date WHERE 1=0")


# COMMAND ----------

display(df_trg)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Join Condition**

# COMMAND ----------

join_cond = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])
join_cond

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
                SELECT src.*, trg.{surrogate_key}, trg.create_date, trg.update_date
                FROM src 
                LEFT JOIN trg
                ON {join_cond}
                    """)

display(df_join)

# COMMAND ----------

# Old records
df_old = df_join.filter(col(f"{surrogate_key}").isNotNull())

# New records
df_new = df_join.filter(col(f"{surrogate_key}").isNull())

# COMMAND ----------

display(df_old)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Preparing df_old**

# COMMAND ----------

df_old_enr = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Preparing df_new**

# COMMAND ----------

display(df_new)

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_sch}.{target_obj}"):
    max_surrogate_key = spark.sql(f"""
                            SELECT max({surrogate_key}) FROM {catalog}.{target_sch}.{target_obj}
                        """).collect()[0][0]
    df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                        .withColumn("create_date", current_timestamp())\
                        .withColumn("update_date", current_timestamp())


else:
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                        .withColumn("create_date", current_timestamp())\
                        .withColumn("update_date", current_timestamp())
    

# COMMAND ----------

display(df_new_enr)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Unioning old and new records**

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)
display(df_union)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Upsert**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_sch}.{target_obj}"):
    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_sch}.{target_obj}")
    dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
            .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col} ")\
            .whenNotMatchedInsertAll()\
            .execute()

else:
    df_union.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_sch}.{target_obj}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flights_catalog.gold.dim_flights

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flights_catalog.gold.dim_airports

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flights_catalog.gold.dim_passengers

# COMMAND ----------

