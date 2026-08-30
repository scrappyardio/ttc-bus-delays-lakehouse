-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 — Gold build
-- MAGIC Row-level fact table plus four aggregates.
-- MAGIC Everything is rebuilt with CREATE OR REPLACE, so rerunning is safe by construction.

-- COMMAND ----------

USE CATALOG ttc_bus_delays;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Gold Table from The Silver Table(CTAS) and Enable Liquid Clustering

-- COMMAND ----------

CREATE OR REPLACE TABLE ttc_bus_delays.gold.fct_bus_delays
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
FROM silver.bus_delays
WHERE year(event_date) BETWEEN 2018 AND 2025

-- COMMAND ----------

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

CREATE OR REPLACE TABLE ttc_bus_delays.gold.agg_incident_by_hour AS
SELECT

  year,
  incident_category,
  hour_of_day,
  day_of_week,
  count(*)                 AS incident_count,
  sum(delay_min)           AS total_delay_min,
  round(avg(delay_min), 1) AS avg_delay_min
FROM ttc_bus_delays.gold.fct_bus_delays
GROUP BY ALL;

-- COMMAND ----------

SELECT DISTINCT incident_category FROM gold.agg_incident_by_hour ORDER BY 1;

-- COMMAND ----------

CREATE OR REPLACE TABLE ttc_bus_delays.gold.agg_location_hotspots AS
SELECT
  location,
  count(*)                 AS incident_count,
  sum(delay_min)           AS total_delay_min,
  round(avg(delay_min), 1) AS avg_delay_min,
  mode(incident_category)  AS common_issue
FROM ttc_bus_delays.gold.fct_bus_delays
GROUP BY location
HAVING count(*) >= 50;

-- COMMAND ----------

SELECT year, count(*) AS rows FROM gold.fct_bus_delays GROUP BY 1 ORDER BY 1;

-- COMMAND ----------

SELECT COUNT(*)
FROM ttc_bus_delays.gold.fct_bus_delays