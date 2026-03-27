# scripts/config.py

# Azure Data Lake paths
BRONZE_PATH = "abfss://bronze@nyctaxistoragehet.dfs.core.windows.net"
SILVER_PATH = "abfss://silver@nyctaxistoragehet.dfs.core.windows.net"
GOLD_PATH = "abfss://gold@nyctaxistoragehet.dfs.core.windows.net"

# Subfolders
TRIP_DATA = "trip2023data"
TRIP_ZONE = "trip_zone"
TRIP_TYPE = "trip_type"