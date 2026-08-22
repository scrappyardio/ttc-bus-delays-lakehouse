-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 — Setup
-- MAGIC Creates the catalog, schemas, landing volume and the monitoring table.
-- MAGIC Run once by hand. Not part of the scheduled job.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ttc_bus_delays;

-- COMMAND ----------

USE CATALOG ttc_bus_delays;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze     COMMENT 'Raw ingested data, source fidelity preserved';
CREATE SCHEMA IF NOT EXISTS silver     COMMENT 'Standardized, cleaned, quality-checked';
CREATE SCHEMA IF NOT EXISTS gold       COMMENT 'Business aggregates for BI';
CREATE SCHEMA IF NOT EXISTS monitoring COMMENT 'Pipeline execution logs';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS bronze.landing
  COMMENT 'Landing zone: source CSV files in files/, Auto Loader state in _schema/ and _checkpoint/';

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS monitoring.pipeline_logs (
  pipeline_name   STRING,
  run_ts          TIMESTAMP,
  rows_silver     BIGINT,
  rows_quarantine BIGINT,
  pass_rate_pct   DOUBLE
) COMMENT 'One row per pipeline run';

-- COMMAND ----------

LIST '/Volumes/ttc_bus_delays/bronze/landing/files/';
