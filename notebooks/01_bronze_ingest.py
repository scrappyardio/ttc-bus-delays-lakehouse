# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 — Bronze ingest
# MAGIC Auto Loader reads the yearly CSV files into `bronze.bus_delays_raw`.
# MAGIC Everything stays STRING. Column names that changed across years are all kept
# MAGIC as separate columns — silver is responsible for merging them.
# MAGIC
# MAGIC The stream stops when it meets a column that is not yet in the schema.
# MAGIC That is expected: rerun the cell, two or three times is normal.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Way to Reset Bronze

# COMMAND ----------

# Reset bronze state. Uncomment and run manually when reloading from scratch.
# spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
# dbutils.fs.rm(CHECKPOINT, True)
# dbutils.fs.rm(SCHEMA_LOC, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defining Locations

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG    = "ttc_bus_delays"
LANDING    = f"/Volumes/{CATALOG}/bronze/landing/files"
SCHEMA_LOC = f"/Volumes/{CATALOG}/bronze/landing/_schema"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/landing/_checkpoint"
TARGET     = f"{CATALOG}.bronze.bus_delays_raw"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader with AddNewColumns Mode 

# COMMAND ----------

import re

def clean_name(c):
    return re.sub(r"[ ,;{}()\n\t=]+", "_", c).rstrip("_")

df_raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOC)
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("header", "true")
    .load(LANDING)
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_name")))

df_raw = df_raw.toDF(*[clean_name(c) for c in df_raw.columns])

query = (df_raw.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking the Schema

# COMMAND ----------

spark.table(TARGET).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column census
# MAGIC How many rows are filled for each column name variant.

# COMMAND ----------

df = spark.table(TARGET)
print(f"rows: {df.count():,}")
print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Counting Which Columns Were Used By Which Years

# COMMAND ----------

display(df.select([F.count(F.col(f"`{c}`")).alias(c) for c in df.columns]))

# COMMAND ----------

display(
    df.groupBy("_source_file")
      .count()
      .orderBy("_source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check _rescued_data to find any issues after adding new columns

# COMMAND ----------

display(
    df.filter(F.col("_rescued_data").isNotNull())
      .select("_source_file", "_rescued_data")
      .limit(20)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ttc_bus_delays.bronze.bus_delays_raw
# MAGIC WHERE Report_Date IS NOT NULL

# COMMAND ----------

display(spark.read.option("header", "true")
        .csv(f"{LANDING}/ttc-bus-delay-data-2021.csv")
)