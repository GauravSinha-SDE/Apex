-- =============================================================================
-- Apex Manufacturing Lakehouse — Gold: Changeover Time mart
-- Run via a Jobs sql_task.file with a named `catalog` parameter. See
-- 02a_gold_oee_daily.sql's header for the USE CATALOG / one-MV-per-file pattern
-- this follows, and 02b_gold_equipment_reliability.sql's header for why
-- CREATE OR REPLACE, not IF NOT EXISTS.
--
-- Grain: one row per changeover EVENT (not per day) — the gap between the end of
-- one completed run and the start of the next completed run on the same line,
-- only where product_sku differs between them. Target < 30 min (plant KPI).
--
-- No source file/column in the sample data names a "changeover" directly —
-- this is inferred from production_runs' own sequencing (consecutive runs per
-- line, ordered by start_ts). ASSUMPTION worth flagging: this treats ANY gap
-- between two different-SKU runs as changeover time, with no way from
-- production_runs alone to separate genuine SKU-to-SKU changeover from other
-- idle time that happens to coincide with a SKU switch (e.g. a shift-end gap
-- or an unrelated unplanned stop from maintenance_work_orders). A more precise
-- version would subtract overlapping downtime windows from
-- silver.maintenance_work_orders before calling the remainder "changeover" —
-- flagged as a follow-up, not built here.
--
-- DQ-14 (found by actually building and querying this mart, not caught in the
-- original profiling pass in docs/02_data_quality.md): Line 3's production_runs
-- has genuinely OVERLAPPING run windows — e.g. RUN-036 runs 2026-03-13 14:00-17:55
-- while RUN-048 starts at 15:00, same line, before RUN-036 ends. Physically
-- impossible for a single line. 4 of 20 raw changeover candidates (all on Line 3,
-- none on Lines 1/2) computed a NEGATIVE gap as a result. These are excluded
-- below (changeover_minutes >= 0) rather than silently producing a nonsensical
-- negative "changeover time" — quarantined by omission, not passed through.
-- Source-system root cause not determined; worth a Postgres production-scheduling
-- system check before trusting Line 3 changeover figures specifically.
--
-- MUST run AFTER the Lakeflow pipeline has completed at least one run — reads
-- silver.production_runs, which the pipeline creates.
-- =============================================================================

USE CATALOG IDENTIFIER(:catalog);

CREATE OR REPLACE MATERIALIZED VIEW gold.changeover_time
COMMENT 'Gap between consecutive completed runs on the same line where product_sku changes. Target < 30 min. One row per changeover event, not per day. Excludes overlapping-run artifacts (DQ-14, Line 3 only) — see file header.'
AS
WITH ordered_runs AS (
  SELECT run_id, line_id, product_sku, start_ts, end_ts,
         LAG(product_sku) OVER (PARTITION BY line_id ORDER BY start_ts) AS prev_product_sku,
         LAG(end_ts)      OVER (PARTITION BY line_id ORDER BY start_ts) AS prev_end_ts
  FROM silver.production_runs
  WHERE run_status = 'COMPLETED'
)
SELECT line_id,
       prev_product_sku                                              AS from_product_sku,
       product_sku                                                   AS to_product_sku,
       prev_end_ts                                                   AS changeover_start_ts,
       start_ts                                                      AS changeover_end_ts,
       ROUND(TIMESTAMPDIFF(MINUTE, prev_end_ts, start_ts), 1)        AS changeover_minutes
FROM ordered_runs
WHERE prev_product_sku IS NOT NULL
  AND product_sku <> prev_product_sku
  AND start_ts >= prev_end_ts;  -- DQ-14: drop overlapping-run artifacts (negative gap)
