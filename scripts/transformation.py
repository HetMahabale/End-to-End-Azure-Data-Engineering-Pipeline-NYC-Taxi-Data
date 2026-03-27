# scripts/transformations.py

from pyspark.sql.functions import *

def transform_trip_data(df):
    return df.withColumn("trip_date", to_date("lpep_pickup_datetime")) \
             .withColumn("trip_year", year("lpep_pickup_datetime")) \
             .withColumn("trip_month", month("lpep_pickup_datetime"))

def clean_trip_data(df):
    return df.dropna()

def select_required_columns(df):
    return df.select(
        "VendorID",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
        "total_amount"
    )

def transform_zone(df):
    return df.withColumn("zone1", split(col("Zone"), "/")[0]) \
             .withColumn("zone2", split(col("Zone"), "/")[1])