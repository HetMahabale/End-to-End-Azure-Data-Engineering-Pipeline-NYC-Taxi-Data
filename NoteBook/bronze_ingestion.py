# Databricks notebook source

# ---------------------------------------------
# BRONZE LAYER - DATA INGESTION
# ---------------------------------------------
# In this project, data ingestion is handled using Azure Data Factory (ADF).
# The pipeline dynamically fetches NYC Taxi data (monthly) from an HTTP source
# and stores it into Azure Data Lake Storage (Bronze layer).

# Source:
# HTTP endpoint (NYC Taxi Parquet files)

# Destination:
# Azure Data Lake Storage Gen2 (Bronze container)

# Key Features:
# - Dynamic ingestion using ForEach loop in ADF
# - Parameterized dataset (month-wise data ingestion)
# - Data stored in raw format (no transformation)

# Note:
# This notebook represents the Bronze layer concept.
# Actual ingestion logic is implemented in ADF pipeline.

print("Bronze layer ingestion handled by Azure Data Factory")