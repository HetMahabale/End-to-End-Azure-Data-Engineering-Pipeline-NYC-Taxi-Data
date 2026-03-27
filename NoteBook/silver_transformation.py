# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

# Authentication handled securely using Azure Key Vault / Databricks Secret Scope
# COMMAND ----------

storage_account = "nyctaxistoragehet"
# ---------------------------------------------
# Authentication
# ---------------------------------------------
# Credentials are securely managed using Azure Key Vault / Databricks Secret Scope
# COMMAND ----------

dbutils.fs.ls('abfss://bronze@nyctaxistoragehet.dfs.core.windows.net/')

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC Importing Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC reading csv data

# COMMAND ----------

# MAGIC %md 
# MAGIC Trip Type Data

# COMMAND ----------

# Authentication handled securely using Azure Key Vault / Databricks Secret Scope

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load('abfss://bronze@nyctaxistoragehet.dfs.core.windows.net/trip_type')

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC Trip Zone

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load('abfss://bronze@nyctaxistoragehet.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC Trip Data 2023

# COMMAND ----------

myschema =(
    '''
        VendorID BIGINT,
        lpep_pickup_datetime TIMESTAMP,
        lpep_dropoff_datetime TIMESTAMP,
        store_and_fwd_flag STRING,
        RatecodeID BIGINT,
        PULocationID BIGINT,
        DOLocationID BIGINT,
        passenger_count BIGINT,
        trip_distance DOUBLE,
        fare_amount DOUBLE,
        extra DOUBLE,
        mta_tax DOUBLE,
        tip_amount DOUBLE,
        tolls_amount DOUBLE,
        ehail_fee DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount DOUBLE,
        payment_type BIGINT,
        trip_type BIGINT,
        congestion_surcharge DOUBLE
    '''
)

# COMMAND ----------

df_trip = spark.read.format('parquet')\
                .schema(myschema)\
                .option('header',True)\
                .option('recursiveFileLookup',True)\
                .load('abfss://bronze@nyctaxistoragehet.dfs.core.windows.net/trip2023data/')

# COMMAND ----------

 df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Tranfomation

# COMMAND ----------

# MAGIC %md
# MAGIC Taxi Trip Type

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type = df_trip_type.withColumnRenamed('description','trip_description')
df_trip_type.display()

# COMMAND ----------

df_trip_type.write.format('parquet')\
            .mode('append')\
            .option("path",'abfss://silver@nyctaxistoragehet.dfs.core.windows.net/trip_type')\
            .save()    

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Zone

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('zone1', split(col('Zone'),'/')[0])\
                            .withColumn('zone2', split(col('Zone'),'/')[1])

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
            .mode('append')\
            .option("path",'abfss://silver@nyctaxistoragehet.dfs.core.windows.net/trip_zone')\
            .save()    

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Data

# COMMAND ----------

 df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date',to_date('lpep_pickup_datetime'))\
                    .withColumn('trip_year',year('lpep_pickup_datetime'))\
                    .withColumn('trip_month',month('lpep_pickup_datetime'))

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip_data = df_trip.select('VendorID','PULocationID','DOLocationID','fare_amount','total_amount')
df_trip_data.display()

# COMMAND ----------

df_trip_data.write.format('parquet')\
            .mode('append')\
            .option("path",'abfss://silver@nyctaxistoragehet.dfs.core.windows.net/trip2023data')\
            .save()    

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis

# COMMAND ----------

display(df_trip)

# COMMAND ----------

 