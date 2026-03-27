# Databricks notebook source
# MAGIC %md 
# MAGIC # Data Access

# COMMAND ----------
# Authentication handled securely using Azure Key Vault / Databricks Secret Scope
# COMMAND ----------

# MAGIC %md
# MAGIC # Database Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC use database gold

# COMMAND ----------

storage_account = "nyctaxistoragehet"

# ---------------------------------------------
# Authentication
# ---------------------------------------------
# Credentials are securely managed using Azure Key Vault / Databricks Secret Scope
# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading and Writing and Creating Delta Tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stroage Variables

# COMMAND ----------

silver = 'abfss://silver@nyctaxistoragehet.dfs.core.windows.net'

gold = 'abfss://gold@nyctaxistoragehet.dfs.core.windows.net'

# COMMAND ----------

# MAGIC %md
# MAGIC Data Zone

# COMMAND ----------

df_zone = spark.read.format('parquet')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load(f'{silver}/trip_zone') 

# COMMAND ----------

df_zone.printSchema()

# COMMAND ----------

display(df_zone)

# COMMAND ----------

df_zone = df_zone.dropDuplicates()
df_zone.display()



# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format("delta") \
    .mode("append") \
    .save(f'{gold}/trip_zone')

# COMMAND ----------

df_zone.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.trip_zone")

# COMMAND ----------

# MAGIC %sql
# MAGIC use gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trip_zone;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM delta.`abfss://gold@nyctaxistoragehet.dfs.core.windows.net/trip_zone`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone
# MAGIC where Borough = 'EWR'

# COMMAND ----------

df_type = spark.read.format('parquet')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load(f'{silver}/trip_type') 

# COMMAND ----------

df_type = df_type.dropDuplicates()
df_type.display()


# COMMAND ----------

df_type.write.format("delta") \
    .mode("append") \
    .save(f'{gold}/trip_type')

# COMMAND ----------



# COMMAND ----------

df_type.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("gold.trip_type")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT trip_type, trip_description
# MAGIC FROM gold.trip_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_type;

# COMMAND ----------

# MAGIC %md
# MAGIC Trips Data

# COMMAND ----------

df_trip_data = spark.read.format('parquet')\
                .option('inferSchema',True)\
                .option('header',True)\
                .load(f'{silver}/trip2023data') 

# COMMAND ----------

df_trip_data.display()

# COMMAND ----------

df_trip_data.write.format("delta") \
    .mode("append") \
    .save(f'{gold}/trip_data')

# COMMAND ----------

df_trip_data.write.format("delta")\
    .mode("overwrite")\
    .saveAsTable("gold.trip_data")

# COMMAND ----------

    

# COMMAND ----------

# MAGIC %md
# MAGIC # Learing Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone;

# COMMAND ----------

# MAGIC %sql
# MAGIC update gold.trip_zone
# MAGIC set Borough = 'EMR' where LocationID = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone where LocationID = 100;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE gold.trip_zone;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from gold.trip_zone
# MAGIC where LocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.trip_zone;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone
# MAGIC where LocationID = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC restore gold.trip_zone to version as of 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone;

# COMMAND ----------

# MAGIC %md 
# MAGIC # Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Type

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_type;

# COMMAND ----------

# MAGIC %md 
# MAGIC Trip Zone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone;

# COMMAND ----------

# MAGIC %md
# MAGIC Trip Data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_data;

# COMMAND ----------

