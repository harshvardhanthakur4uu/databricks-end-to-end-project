import dlt
from pyspark.sql.functions import * 
from pyspark.sql.types import *

@dlt.table(
    name = "stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta")\
            .load("/Volumes/flights_catalog/bronze/bronzevolume/bookings/data/")
    return df


@dlt.view(
    name = "trans_bookings"
)
def trans_bookings():
    df = dlt.readStream("stage_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
        .withColumn("modifiedDate", current_timestamp())\
        .withColumn("booking_date", to_date(col("booking_date")))\
        .drop("_rescued_data")
    return df

rules = {
    "rule1" : "booking_id is not null",
    "rule2" : "passenger_id is not null"
}

@dlt.table(
    name = "silver_bookings",
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = dlt.readStream("trans_bookings")\
        .option("ignoreChanges", "true")
    return df

#-------------------------------------------------------------------------------
#flights

@dlt.view(
    name = "trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights_catalog/bronze/bronzevolume/flights/data/")
    df = df.withColumn("flight_date", to_date(col("flight_date")))\
        .drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())

    return df 

dlt.create_streaming_table(
    name = "silver_flights"
)
dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys = ["flight_id"],
    sequence_by= col("modifiedDate"),
    stored_as_scd_type = 1
)

#-------------------------------------------------------------------------------
# passengers

@dlt.view(
    name = "trans_passengers"
)
def trans_passengers():
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights_catalog/bronze/bronzevolume/customers/data/")
    df = df.drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())

    return df

dlt.create_streaming_table(
    name = "silver_passengers"
)
dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys = ["passenger_id"],
    sequence_by= col("modifiedDate"),
    stored_as_scd_type = 1
)

#-------------------------------------------------------------------------------
# airports

@dlt.view(
    name = "trans_airports"
)
def trans_passengers():
    df = spark.readStream.format("delta")\
        .load("/Volumes/flights_catalog/bronze/bronzevolume/airports/data/")
    df = df.drop("_rescued_data")\
        .withColumn("modifiedDate", current_timestamp())

    return df

dlt.create_streaming_table(
    name = "silver_airports"
)
dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by= col("modifiedDate"),
    stored_as_scd_type = 1
)

#-----------------------------------------------------------------
# Silver Business View

@dlt.table(
    name = "silver_business"
)
def silver_business():
    df = dlt.readStream("silver_bookings")\
        .join(dlt.readStream("silver_flights"), on = "flight_id", how = "inner")\
        .join(dlt.readStream("silver_passengers"), on = "passenger_id", how = "inner")\
        .join(dlt.readStream("silver_airports"), on = "airport_id", how = "inner")\
        .drop("modifiedDate")

    return df