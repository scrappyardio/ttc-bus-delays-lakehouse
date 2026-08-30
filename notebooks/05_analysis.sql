-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 05 — Analysis
-- MAGIC Final analytics for the project

-- COMMAND ----------

USE CATALOG ttc_bus_delays;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Finding 1 — the minutes are in the tail
-- MAGIC Two periods reported separately: the source changed its incident coding in 2025 (see Finding 3).
-- MAGIC The point to read off this table is the median, not the total: Equipment, External and Safety
-- MAGIC all sit at 10 minutes in both periods, while Operational runs a mean far above its median.

-- COMMAND ----------

SELECT
  CASE WHEN year <= 2024 THEN '2018-2024' ELSE '2025' END AS period,
  incident_category,
  count(*)                    AS incidents,
  sum(delay_min)              AS lost_minutes,
  round(avg(delay_min), 1)    AS avg_min,
  percentile(delay_min, 0.5)  AS median_min,
  max(delay_min)              AS worst_min,
  round(100.0 * count(*)
        / sum(count(*)) OVER (PARTITION BY CASE WHEN year <= 2024 THEN '2018-2024' ELSE '2025' END), 1) AS pct_incidents,
  round(100.0 * sum(delay_min)
        / sum(sum(delay_min)) OVER (PARTITION BY CASE WHEN year <= 2024 THEN '2018-2024' ELSE '2025' END), 1) AS pct_minutes
FROM gold.fct_bus_delays
GROUP BY CASE WHEN year <= 2024 THEN '2018-2024' ELSE '2025' END, incident_category
ORDER BY period, lost_minutes DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Finding 2 — a thirty-minute ceiling
-- MAGIC Routes ranked by the gap between mean and median. A large gap means a long tail:
-- MAGIC most incidents are ordinary, a few run for hours.
-- MAGIC Note that the highest-gap routes are not the busiest ones.

-- COMMAND ----------

SELECT
  route_id,
  count(*)                                                     AS incidents,
  round(avg(delay_min), 1)                                     AS avg_min,
  percentile(delay_min, 0.5)                                   AS median_min,
  round(avg(delay_min) - percentile(delay_min, 0.5), 1)        AS gap
FROM gold.fct_bus_delays
WHERE year <= 2024
GROUP BY route_id
HAVING count(*) >= 50
ORDER BY gap DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Same route, broken down by category. This is where the ceiling shows:
-- MAGIC Equipment and Safety stop around 30 minutes, Operational does not stop.

-- COMMAND ----------

SELECT
  incident_category,
  count(*)                    AS incidents,
  round(avg(delay_min), 1)    AS avg_min,
  percentile(delay_min, 0.5)  AS median_min,
  max(delay_min)              AS worst_min
FROM gold.fct_bus_delays
WHERE route_id = '77' AND year <= 2024
GROUP BY incident_category
ORDER BY avg_min DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Finding 3 — the source changed methodology twice
-- MAGIC Four independent signals. First: volume. 2021 has 2,808 rows, 2022 has 57,779.

-- COMMAND ----------

SELECT year, count(*) AS rows FROM gold.fct_bus_delays GROUP BY 1 ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Second: share of lost minutes by category, per year. 2021 breaks the pattern —
-- MAGIC Operational collapses while External and Safety spike.

-- COMMAND ----------

SELECT
  year,
  incident_category,
  round(100.0 * sum(delay_min)
        / sum(sum(delay_min)) OVER (PARTITION BY year), 1) AS pct_minutes
FROM gold.fct_bus_delays
GROUP BY year, incident_category
ORDER BY year, pct_minutes DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Third: composition of External codes. Before 2021 there is only EMERGENCY SERVICES.
-- MAGIC DIVERSION makes the transition visible — hundreds of cases a year, then exactly one
-- MAGIC in 2021, then thousands.

-- COMMAND ----------

SELECT year, incident_type, count(*) AS n
FROM gold.fct_bus_delays
WHERE incident_category = 'External'
GROUP BY year, incident_type
ORDER BY year, n DESC;

-- COMMAND ----------

SELECT year, count(*) AS n
FROM gold.fct_bus_delays
WHERE incident_type LIKE '%DIVERSION%'
GROUP BY year ORDER BY year;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Fourth, and the strongest: the *shape* of the cause field, not its meaning.
-- MAGIC Text labels and short codes have zero overlap — no year mixes the two.
-- MAGIC Read against silver, which still holds 2026, so the new coding is visible in both years.

-- COMMAND ----------

SELECT
  year(event_date) AS yr,
  sum(CASE WHEN incident_type RLIKE '^[A-Z]{2,5}$' THEN 1 ELSE 0 END) AS short_codes,
  sum(CASE WHEN incident_type RLIKE ' '            THEN 1 ELSE 0 END) AS text_labels,
  count(*)                                                            AS total_rows
FROM silver.bus_delays
GROUP BY 1 ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Layer reconciliation
-- MAGIC Every row between bronze and gold is accounted for: deduplication and the excluded 2026.

-- COMMAND ----------

SELECT
  (SELECT count(*) FROM bronze.bus_delays_raw)             AS bronze_rows,
  (SELECT count(*) FROM silver.bus_delays)                 AS silver_passed,
  (SELECT count(*) FROM silver.bus_delays_quarantine)      AS silver_quarantined,
  (SELECT count(*) FROM gold.fct_bus_delays)               AS gold_rows,
  (SELECT count(*) FROM bronze.bus_delays_raw)
    - (SELECT count(*) FROM silver.bus_delays)
    - (SELECT count(*) FROM silver.bus_delays_quarantine)  AS collapsed_by_dedup,
  (SELECT count(*) FROM silver.bus_delays WHERE year(event_date) = 2026) AS excluded_2026;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Gold must equal silver over the same period. This returns zero.

-- COMMAND ----------

SELECT (SELECT count(*) FROM silver.bus_delays WHERE year(event_date) BETWEEN 2018 AND 2025)
     - (SELECT count(*) FROM gold.fct_bus_delays) AS diff;