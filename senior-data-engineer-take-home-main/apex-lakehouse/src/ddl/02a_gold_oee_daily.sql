-- =============================================================================
-- Apex Manufacturing Lakehouse — Gold: OEE daily mart
-- Run via a Jobs sql_task.file with a named `catalog` parameter. Split into its
-- own file (previously in 02_gold_marts.sql alongside equipment_reliability):
-- Databricks Jobs' sql_task.file special-cases any file containing
-- CREATE MATERIALIZED VIEW / STREAMING TABLE and requires EXACTLY ONE such
-- statement per file (it routes through an implicit pipeline-backed execution
-- path, not plain statement execution) — found by actually running this with
-- two MVs in one file.
--
-- USE CATALOG, not IDENTIFIER() in the CREATE target: also found by actually
-- running this — the same "exactly one CREATE [...] statement expected, but 0
-- found" error fired even with a single MV per file, because the file's
-- statement-type detector doesn't recognize IDENTIFIER(...) as a valid CREATE
-- MATERIALIZED VIEW target; it wants a literal name. Setting the catalog via
-- USE CATALOG once, then using plain schema.table names for both the CREATE
-- target and every FROM/JOIN reference, sidesteps that detector entirely while
-- still keeping catalog fully parameterized per environment.
--
-- MUST run AFTER the Lakeflow pipeline (src/pipelines/apex_pipeline.py) has
-- completed at least one run — reads silver.production_runs and
-- silver.maintenance_work_orders, which the pipeline creates.
-- =============================================================================

USE CATALOG IDENTIFIER(:catalog);

-- OR REPLACE, not IF NOT EXISTS: for a materialized VIEW the SQL in this file is the
-- deployable artifact — a redeploy should always make the live definition match Git,
-- not silently keep whatever query first created the MV. See
-- 02b_gold_equipment_reliability.sql's header for how this was found.
CREATE OR REPLACE MATERIALIZED VIEW gold.oee_daily
COMMENT 'One row per line per day. OEE = availability * performance * quality. Target > 85%.'
AS
WITH runtime AS (
  SELECT line_id,
         DATE(start_ts_local)                                   AS production_date,
         SUM(TIMESTAMPDIFF(MINUTE, start_ts, end_ts))           AS run_minutes,
         SUM(total_units)                                       AS total_units,
         SUM(good_units)                                        AS good_units
  FROM silver.production_runs
  WHERE run_status = 'COMPLETED'
  GROUP BY 1, 2
),
downtime AS (
  SELECT e.line_id,
         DATE(w.created_ts_local)         AS production_date,
         SUM(w.downtime_minutes)          AS unplanned_downtime_min
  FROM silver.maintenance_work_orders w
  JOIN ref.equipment_xref e ON w.equipment_key = e.equipment_key
  WHERE w.planning_category = 'UNPLANNED'
  GROUP BY 1, 2
),
capacity AS (  -- nameplate BPM per line from plant context
  SELECT * FROM VALUES (1, 600), (2, 800), (3, 500) AS t(line_id, nameplate_bpm)
)
SELECT r.line_id,
       r.production_date,
       ROUND(r.run_minutes / (r.run_minutes + COALESCE(d.unplanned_downtime_min, 0)), 4)  AS availability,
       ROUND(r.total_units / NULLIF(r.run_minutes * c.nameplate_bpm, 0), 4)               AS performance,
       ROUND(r.good_units / NULLIF(r.total_units, 0), 4)                                  AS quality,
       ROUND( (r.run_minutes / (r.run_minutes + COALESCE(d.unplanned_downtime_min, 0)))
            * (r.total_units / NULLIF(r.run_minutes * c.nameplate_bpm, 0))
            * (r.good_units / NULLIF(r.total_units, 0)), 4)                               AS oee
FROM runtime r
LEFT JOIN downtime d USING (line_id, production_date)
JOIN capacity c USING (line_id);
