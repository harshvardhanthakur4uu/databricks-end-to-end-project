# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Parameters**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flights_catalog.silver.silver_bookings

# COMMAND ----------

# Catalog Name
catalog = "flights_catalog"

# Source Schema
source_sch = "silver"

# Source Object 
source_obj = "silver_bookings"

# CDC Column
cdc_col = "modifiedDate"

# Backdated Refresh
backdate_refresh = ""

# Source Fact Table
fact_table = f"{catalog}.{source_sch}.{source_obj}"

# Target Schema 
target_sch = "gold"

# Target Object 
target_obj = "fact_bookings"

# Fact Key Cols List 
fact_key_cols = ["DimPassengersKey","DimFlightsKey","DimAirportsKey","booking_date"]

# COMMAND ----------

dimensions = [
    {
        "table": f"{catalog}.{target_sch}.dim_passengers",
        "alias": "DimPassengers",
        "join_keys": [("passenger_id", "passenger_id")]  # (fact_col, dim_col)
    },
    {
        "table": f"{catalog}.{target_sch}.dim_flights",
        "alias": "DimFlights",
        "join_keys": [("flight_id", "flight_id")]  # (fact_col, dim_col)
    },
    {
        "table": f"{catalog}.{target_sch}.dim_airports",
        "alias": "DimAirports",
        "join_keys": [("airport_id", "airport_id")]  # (fact_col, dim_col)
    },
]

# Columns you want to keep from Fact table (besides the surrogate keys)
fact_cols = ["amount","booking_date","modifiedDate"]

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Last Load Date**

# COMMAND ----------

# No Back Dated Refresh
if len(backdate_refresh) == 0:
  
  # If Table Exists In The Destination
  if spark.catalog.tableExists(f"{catalog}.{target_sch}.{target_obj}"):

    last_load = spark.sql(f"SELECT max({cdc_col}) FROM {catalog}.{target_sch}.{target_obj}").collect()[0][0]
    
  else:
    last_load = "1900-01-01 00:00:00"

# Yes Back Dated Refresh
else:
  last_load = backdate_refresh

# Test The Last Load 
last_load

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dynamic Fact Query [Bring Keys]**

# COMMAND ----------

def gen_fact_query_inc(fact_table, dimensions, fact_cols, cdc_col, processing_date):
    fact_alias = "f"

    # Base cols to select
    select_cols = [f"{fact_alias}.{col}" for col in fact_cols]

    # Build joins dynamically
    join_clauses = []
    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]
        table_name = table_full.split(".")[-1]
        surrogate_key = f"{alias}.{alias}Key"
        select_cols.append(surrogate_key)

        # Build ON clause
        on_cond = [
            f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim["join_keys"]
        ]
        join_clause = f"LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_cond)
        join_clauses.append(join_clause)

    # Final SELECT and JOIN clauses
    select_clause = ",\n   ".join(select_cols)
    joins = "\n".join(join_clauses)

    # WHERE clause for incremental filtering
    where_clause = f"{fact_alias}.{cdc_col} >= DATE('{last_load}')"

    # Final query
    query = f"""
    SELECT {select_clause}
    FROM {fact_table} {fact_alias}
    {joins}
    WHERE {where_clause}
    """.strip()
    return query



# COMMAND ----------

query = gen_fact_query_inc(fact_table, dimensions, fact_cols, cdc_col, last_load)
print(query)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DF_FACT**

# COMMAND ----------

df_fact = spark.sql(query)
display(df_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Upsert**

# COMMAND ----------

# Fact key cols merge conditions
fact_key_cols_str = " And ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# COMMAND ----------

from delta.tables import DeltaTable

if spark.catalog.tableExists(f"{catalog}.{target_sch}.{target_obj}"):

    dlt_obj = DeltaTable.forName(spark, f"{catalog}.{target_sch}.{target_obj}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
                        .whenMatchedUpdateAll(condition = f"src.{cdc_col} >= trg.{cdc_col}")\
                        .whenNotMatchedInsertAll()\
                        .execute()

else:
    df_fact.write.format("delta")\
            .mode("append")\
            .saveAsTable(f"{catalog}.{target_sch}.{target_obj}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM flights_catalog.gold.fact_bookings

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     f.amount,
# MAGIC     f.booking_date,
# MAGIC     d.airport_id,
# MAGIC     d.airport_name,
# MAGIC     d.city,
# MAGIC     d.country
# MAGIC FROM
# MAGIC     flights_catalog.gold.fact_bookings f
# MAGIC LEFT JOIN
# MAGIC     flights_catalog.gold.dim_airports d
# MAGIC ON f.DimAirportsKey = d.DimAirportsKey;

# COMMAND ----------

