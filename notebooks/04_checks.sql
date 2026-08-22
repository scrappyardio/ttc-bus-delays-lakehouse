-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 — Checks and monitoring
-- MAGIC Validation queries plus one row appended to `monitoring.pipeline_logs`.
-- MAGIC Read the outputs — several of these catch failures that are otherwise invisible.

-- COMMAND ----------

USE CATALOG ttc_bus_delays;

-- COMMAND ----------

-- 1. Pass rate. Put this number in the README.
SELECT
  (SELECT count(*) FROM silver.bus_delays)            AS clean_rows,
  (SELECT count(*) FROM silver.bus_delays_quarantine) AS bad_rows,
  round(100.0 * (SELECT count(*) FROM silver.bus_delays)
    / ((SELECT count(*) FROM silver.bus_delays)
     + (SELECT count(*) FROM silver.bus_delays_quarantine)), 2) AS pass_rate_pct;

-- COMMAND ----------

-- 2. Why rows were rejected.
SELECT failed_rule, count(*) AS rows
FROM silver.bus_delays_quarantine
GROUP BY failed_rule
ORDER BY rows DESC;

-- COMMAND ----------

-- 3. Rows per year. A missing or tiny year means a date format the parser does not handle.
SELECT year(event_date) AS year, count(*) AS rows
FROM silver.bus_delays
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- 4. Rejections per source file. A whole file failing points at one specific format.
SELECT _source_file, failed_rule, count(*) AS rows
FROM silver.bus_delays_quarantine
GROUP BY 1, 2
ORDER BY rows DESC;

-- COMMAND ----------

-- 5. Incident codes not yet reviewed by hand.
SELECT incident_category, count(*) AS codes, collect_set(incident_type) AS examples
FROM silver.incident_map
WHERE is_reviewed = false
GROUP BY incident_category
ORDER BY codes DESC;

-- COMMAND ----------

-- 6. Share of rows landing in Other. A large share means the mapping rules need work.
SELECT
  incident_category,
  count(*) AS rows,
  round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM silver.bus_delays
GROUP BY incident_category
ORDER BY rows DESC;

-- COMMAND ----------

-- 7. The headline number: incident count vs lost minutes by category.
SELECT
  incident_category,
  count(*)                                                          AS incidents,
  sum(delay_min)                                                    AS lost_minutes,
  round(100.0 * count(*)      / sum(count(*))      OVER (), 1)      AS pct_incidents,
  round(100.0 * sum(delay_min)/ sum(sum(delay_min))OVER (), 1)      AS pct_lost_minutes
FROM gold.fct_bus_delays
GROUP BY incident_category
ORDER BY lost_minutes DESC;

-- COMMAND ----------

INSERT INTO monitoring.pipeline_logs
SELECT
  'ttc_bus_delays_pipeline',
  current_timestamp(),
  (SELECT count(*) FROM silver.bus_delays),
  (SELECT count(*) FROM silver.bus_delays_quarantine),
  round(100.0 * (SELECT count(*) FROM silver.bus_delays)
    / ((SELECT count(*) FROM silver.bus_delays)
     + (SELECT count(*) FROM silver.bus_delays_quarantine)), 2);

-- COMMAND ----------

SELECT * FROM monitoring.pipeline_logs ORDER BY run_ts DESC LIMIT 5;
