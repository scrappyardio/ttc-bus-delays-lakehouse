# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 — Silver build
# MAGIC Reads `bronze.bus_delays_raw` and produces:
# MAGIC - `silver.bus_delays` — the single source of truth
# MAGIC - `silver.bus_delays_quarantine` — rows that failed validation, with the reason
# MAGIC - `silver.incident_map` — analyst-maintained mapping of incident codes to categories
# MAGIC
# MAGIC This is the only notebook that writes silver tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ttc_bus_delays

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "ttc_bus_delays"
spark.sql(f"USE CATALOG {CATALOG}")

bronze = spark.table("bronze.bus_delays_raw")
present = set(bronze.columns)
display(sorted(present))

# COMMAND ----------


def unify(*names):
    """Coalesce column-name variants, skipping ones that do not exist in bronze.

    Without the existence check, F.col("Line") raises AnalysisException whenever
    a year that uses that name has not been loaded yet.
    """
    cols = [F.col(f"`{n}`") for n in names if n in present]
    return F.coalesce(*cols) if cols else F.lit(None).cast("string")


# COMMAND ----------

raw_date  = unify("Report_Date", "Date")
raw_time  = unify("Time")
raw_route = unify("Route", "Line")

# Explicit format cascade. A plain cast("date") silently nulls every non-ISO year.
# substring(1,10) handles "2014-01-01 00:00:00" and "2025-01-01T00:00:00",
# which a bare yyyy-MM-dd pattern rejects because of the trailing text.
event_date = F.coalesce(
    F.try_to_date(F.substring(raw_date, 1, 10), F.lit("yyyy-MM-dd")),
    F.try_to_date(raw_date, F.lit("d-MMM-yy")),
    F.try_to_date(raw_date, F.lit("M/d/yyyy")),
)

# Spark expects AM/PM, the source has "a.m." with dots and lowercase.
time_clean = F.upper(F.regexp_replace(raw_time, r"\.", ""))
parsed_time = F.coalesce(
    F.try_to_timestamp(time_clean, F.lit("hh:mm:ss a")),
    F.try_to_timestamp(time_clean, F.lit("HH:mm:ss")),
    F.try_to_timestamp(time_clean, F.lit("HH:mm")),
)
event_time = F.date_format(parsed_time, "HH:mm:ss")

route_num = F.regexp_extract(raw_route, r"(\d+)", 1)
direction = F.upper(F.trim(unify("Direction", "Bound")))
vehicle   = F.trim(unify("Vehicle"))

# COMMAND ----------

std = bronze.select(
    event_date.alias("event_date"),
    event_time.alias("event_time"),
    F.try_to_timestamp(
        F.concat_ws(" ", F.date_format(event_date, "yyyy-MM-dd"), event_time)
    ).alias("event_ts"),
    F.hour(parsed_time).alias("hour_of_day"),
    F.date_format(event_date, "EEEE").alias("day_of_week"),
    F.when(route_num == "", None).otherwise(route_num).alias("route_id"),
    F.upper(F.trim(unify("Location", "Station"))).alias("location"),
    F.when(direction.rlike("^(N|NB|N/B|NORTH)$"), "North")
     .when(direction.rlike("^(S|SB|S/B|SOUTH)$"), "South")
     .when(direction.rlike("^(E|EB|E/B|EAST)$"),  "East")
     .when(direction.rlike("^(W|WB|W/B|WEST)$"),  "West")
     .when(direction.rlike("^(BW|B/W)$"), "Both Ways")
     .otherwise(None).alias("direction"),
    F.upper(F.trim(unify("Incident", "Code"))).alias("incident_type"),
    unify("Min_Delay", "Delay").cast("int").alias("delay_min"),
    unify("Min_Gap", "Gap").cast("int").alias("gap_min"),
    F.when(vehicle.isin("0", "null", ""), None).otherwise(vehicle).alias("vehicle_id"),
    F.col("_source_file"),
)

# display(std.select("*").groupBy("incident_type").count())
display(std)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Incident map
# MAGIC The rules below produce a draft. New codes are appended with `is_reviewed = false`.
# MAGIC Existing rows are never overwritten, so manual corrections survive the next run.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding the Incidents Codes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM read_files(
# MAGIC     '/Volumes/ttc_bus_delays/bronze/landing/Code Descriptions.csv',
# MAGIC     format => 'csv',
# MAGIC     header => true
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### "â" Symbols in Select Statement Above
# MAGIC ### Are Due to Wrong Coding 
# MAGIC Read Fix Below

# COMMAND ----------

codes = (spark.read
    .option("header", "true")
    .option("encoding", "ISO-8859-1")
    .csv("/Volumes/ttc_bus_delays/bronze/landing/Code Descriptions.csv")
    .select(
        F.upper(F.trim(F.col("CODE"))).alias("incident_type"),
        F.upper(F.trim(F.regexp_replace(F.col("DESCRIPTION"), "[^\\x20-\\x7E]+", "-"))).alias("code_description"),
    ))

codes.write.mode("overwrite").saveAsTable("silver.code_descriptions")
display(codes)

# COMMAND ----------

std.select("incident_type").distinct().createOrReplaceTempView("v_incidents")

spark.sql("""
INSERT INTO silver.incident_map
SELECT
  i.incident_type,
  CASE
    WHEN i.incident_type RLIKE '^MF|MECHANICAL'                             THEN 'Equipment'
    WHEN i.incident_type RLIKE '^(SF|EF)|EMERGENCY|SECURITY|COLLISION|FIRE' THEN 'External'
    WHEN i.incident_type RLIKE '^(TFC|EO|MR)|DIVERSION|GENERAL DELAY|OFF ROUTE|LATE' THEN 'Operational'
    WHEN i.incident_type RLIKE 'OPERATOR|GARAGE|CREW|STAFF'                 THEN 'Personnel'
    WHEN i.incident_type RLIKE 'CLEANING|UNSANITARY|VISION|INVESTIGATION'   THEN 'Safety'
    ELSE 'Other'
  END,
  false
FROM v_incidents i
WHERE i.incident_type IS NOT NULL
  AND i.incident_type NOT IN (SELECT incident_type FROM silver.incident_map)
""")

# COMMAND ----------

display(
    std.groupBy("incident_type")
       .agg(F.count("*").alias("rows"), F.sum("delay_min").alias("lost_minutes"))
       .orderBy(F.desc("rows"))
       .limit(40)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM ttc_bus_delays.silver.code_descriptions
# MAGIC WHERE incident_type LIKE 'EF%'

# COMMAND ----------

std.select("incident_type").distinct().createOrReplaceTempView("v_incidents")

spark.sql("""
INSERT INTO silver.incident_map
SELECT
  i.incident_type,
  CASE
    WHEN i.incident_type LIKE 'EF%' THEN 'Equipment'
    WHEN i.incident_type LIKE 'SF%' THEN 'External'
    WHEN i.incident_type IN ('MFCN') THEN 'Safety'
    WHEN i.incident_type LIKE 'MF%' THEN 'Equipment'
    WHEN i.incident_type LIKE 'TF%' THEN 'Operational'
    ELSE 'Other'
  END,
  false
FROM v_incidents i
WHERE i.incident_type IS NOT NULL
  AND i.incident_type NOT IN (SELECT incident_type FROM silver.incident_map)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT incident_category, count(*) AS n
# MAGIC FROM silver.incident_map
# MAGIC GROUP BY incident_category
# MAGIC ORDER BY n DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT incident_type FROM silver.incident_map ORDER BY incident_type

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE silver.incident_map
# MAGIC SET incident_category = 'Equipment', is_reviewed = true
# MAGIC WHERE incident_type LIKE '%MECHANICAL%';
# MAGIC
# MAGIC UPDATE silver.incident_map
# MAGIC SET incident_category = 'Operational', is_reviewed = true
# MAGIC WHERE incident_type LIKE 'LATE%'
# MAGIC    OR incident_type LIKE '%DIVERSION%'
# MAGIC    OR incident_type LIKE '%OFF ROUTE%'
# MAGIC    OR incident_type LIKE '%GENERAL DELAY%'
# MAGIC    OR incident_type LIKE '%OPERATIONS%'
# MAGIC    OR incident_type LIKE '%HELD BY%'
# MAGIC    OR incident_type LIKE '%MANAGEMENT%';
# MAGIC
# MAGIC UPDATE silver.incident_map
# MAGIC SET incident_category = 'External', is_reviewed = true
# MAGIC WHERE incident_type LIKE '%COLLISION%'
# MAGIC    OR incident_type LIKE '%EMERGENCY%'
# MAGIC    OR incident_type LIKE '%SECURITY%'
# MAGIC    OR incident_type LIKE '%ROAD BLOCKED%';
# MAGIC
# MAGIC UPDATE silver.incident_map
# MAGIC SET incident_category = 'Safety', is_reviewed = true
# MAGIC WHERE incident_type LIKE '%CLEANING%'
# MAGIC    OR incident_type LIKE '%INVESTIGATION%'
# MAGIC    OR incident_type LIKE '%VISION%';
# MAGIC
# MAGIC UPDATE silver.incident_map
# MAGIC SET incident_category = 'Operational', is_reviewed = true
# MAGIC WHERE incident_type = 'MFDV';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT incident_category, count(*) AS n, collect_set(incident_type) AS examples
# MAGIC FROM silver.incident_map
# MAGIC GROUP BY incident_category
# MAGIC ORDER BY n DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver.incident_map

# COMMAND ----------

enriched = (std
    .join(spark.table("silver.incident_map").select("incident_type", "incident_category"),
          on="incident_type", how="left")
    .withColumn("incident_category", F.coalesce("incident_category", F.lit("Other"))))

# COMMAND ----------

# delay_min range comes from the meaning of the data: a bus delay longer than
# a full day is a data entry error, not a real delay.
rules = (
    F.col("event_date").isNotNull()
    & F.col("event_ts").isNotNull()
    & F.col("route_id").isNotNull()
    & F.col("delay_min").isNotNull()
    & F.col("delay_min").between(0, 1440)
)

# vehicle_id belongs in the key: two buses on the same route in the same minute
# are two real incidents, not a duplicate.
business_key = ["event_ts", "route_id", "location", "incident_type", "vehicle_id"]

silver_cols = [
    "event_date", "event_time", "event_ts", "hour_of_day", "day_of_week",
    "route_id", "location", "direction", "incident_type", "incident_category",
    "delay_min", "gap_min", "vehicle_id", "_source_file",
]

clean = enriched.filter(rules).dropDuplicates(business_key).select(*silver_cols)

quarantine = (enriched.filter(~rules)
    .withColumn("failed_rule",
        F.when(F.col("event_date").isNull(), "unparsed_date")
         .when(F.col("event_ts").isNull(), "unparsed_time")
         .when(F.col("route_id").isNull(), "missing_route")
         .when(F.col("delay_min").isNull(), "non_numeric_delay")
         .otherwise("delay_out_of_range"))
    .select(*silver_cols, "failed_rule"))

# COMMAND ----------

clean.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.bus_delays")
quarantine.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.bus_delays_quarantine")

# COMMAND ----------

n_ok = spark.table("silver.bus_delays").count()
n_bad = spark.table("silver.bus_delays_quarantine").count()
print(f"passed {n_ok:,} | quarantined {n_bad:,} | pass rate {100 * n_ok / (n_ok + n_bad):.2f}%")