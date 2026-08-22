# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingest
# MAGIC Auto Loader reads the yearly CSV files into `bronze.bus_delays_raw`.
# MAGIC Everything stays STRING. Column names that changed across years are all kept
# MAGIC as separate columns — silver is responsible for merging them.
# MAGIC
# MAGIC The stream stops when it meets a column that is not yet in the schema.
# MAGIC That is expected: rerun the cell, two or three times is normal.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG    = "ttc_bus_delays"
LANDING    = f"/Volumes/{CATALOG}/bronze/landing/files"
SCHEMA_LOC = f"/Volumes/{CATALOG}/bronze/landing/_schema"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/landing/_checkpoint"
TARGET     = f"{CATALOG}.bronze.bus_delays_raw"

# COMMAND ----------

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

query = (df_raw.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET))

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Column census
# MAGIC How many rows are filled for each column name variant.
# MAGIC Screenshot this for the README — it is the proof of schema drift.

# COMMAND ----------

df = spark.table(TARGET)
print(f"rows: {df.count():,}")
print(df.columns)

# COMMAND ----------

display(df.select([F.count(F.col(f"`{c}`")).alias(c) for c in df.columns]))

# COMMAND ----------

display(
    df.groupBy("_source_file")
      .count()
      .orderBy("_source_file")
)
