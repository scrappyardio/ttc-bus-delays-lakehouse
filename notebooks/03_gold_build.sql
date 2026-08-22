-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 — Gold build
-- MAGIC Row-level fact table plus four aggregates.
-- MAGIC Everything is rebuilt with CREATE OR REPLACE, so rerunning is safe by construction.

-- COMMAND ----------

USE CATALOG ttc_bus_delays;

-- COMMAND ----------

-- Clustering goes on the row-level table, where it matters.
-- The aggregates are a few hundred rows and need nothing.
CREATE OR REPLACE TABLE gold.fct_bus_delays
CLUSTER BY (event_date, route_id) AS
SELECT
  event_date,
  event_time,
  event_ts,
  hour_of_day,
  day_of_week,
  year(event_date)  AS year,
  month(event_date) AS month,
  route_id,
  location,
  direction,
  incident_type,
  incident_category,
  delay_min,
  gap_min,
  vehicle_id,
  _source_file
FROM silver.bus_delays;

-- COMMAND ----------

-- Threshold removes long-tail noise: a route with one 300-minute delay
-- would otherwise top the ranking.
-- Median next to the mean because delay distributions have a long tail.
CREATE OR REPLACE TABLE gold.agg_route_performance AS
SELECT
  route_id,
  count(*)                             AS total_incidents,
  sum(delay_min)                       AS total_delay_min,
  round(avg(delay_min), 1)             AS avg_delay_min,
  round(percentile(delay_min, 0.5), 1) AS median_delay_min,
  max(delay_min)                       AS max_delay_min,
  mode(incident_category)              AS primary_incident_category
FROM gold.fct_bus_delays
GROUP BY route_id
HAVING count(*) >= 50;

-- COMMAND ----------

-- No threshold here on purpose: it would drop entire categories.
CREATE OR REPLACE TABLE gold.agg_incident_trends AS
SELECT
  year,
  month,
  incident_category,
  count(*)                 AS incident_count,
  sum(delay_min)           AS total_delay_min,
  round(avg(delay_min), 1) AS avg_delay_min
FROM gold.fct_bus_delays
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_incident_by_hour AS
SELECT
  incident_category,
  hour_of_day,
  day_of_week,
  count(*)                 AS incident_count,
  sum(delay_min)           AS total_delay_min,
  round(avg(delay_min), 1) AS avg_delay_min
FROM gold.fct_bus_delays
GROUP BY ALL;

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.agg_location_hotspots AS
SELECT
  location,
  count(*)                 AS incident_count,
  sum(delay_min)           AS total_delay_min,
  round(avg(delay_min), 1) AS avg_delay_min,
  mode(incident_category)  AS common_issue
FROM gold.fct_bus_delays
GROUP BY location
HAVING count(*) >= 50;

-- COMMAND ----------

DESCRIBE DETAIL gold.fct_bus_delays;
