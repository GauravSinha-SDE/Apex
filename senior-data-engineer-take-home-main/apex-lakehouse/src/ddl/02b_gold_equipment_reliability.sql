-- =============================================================================
-- Apex Manufacturing Lakehouse — Gold: MTBF / MTTR mart
-- Run via a Jobs sql_task.file with a named `catalog` parameter. See
-- 02a_gold_oee_daily.sql's header for why this is a separate file from the OEE
-- mart, and why USE CATALOG + plain schema.table names, not IDENTIFIER() in the
-- CREATE target.
--
-- Grain: one row per equipment PER CALENDAR MONTH (plant-local), not one row per
-- equipment all-time. Found by actually testing this mart through Genie: the
-- original all-time-cumulative version silently answered "unplanned stops this
-- month" with the all-time total (same 3 equipment, same counts, regardless of
-- which month was asked about) — a wrong answer presented with full confidence,
-- worse than a decline. Time-windowing by month is the fix; downstream consumers
-- (including Genie) can always SUM(unplanned_stop_count) across months back to an
-- all-time figure when that's genuinely what's asked, but they can no longer
-- accidentally get all-time data when they asked for one month.
--
-- Caveat worth carrying into Genie instructions: mtbf_hours computed within a
-- single calendar month, from a small number of stops, is statistically noisy
-- (in the limit, one stop in a month makes it NULL/undefined via the
-- COUNT(*) - 1 denominator). Fine for stop-count questions; treat single-month
-- MTBF as directional, not precise — a longer trailing window would be more
-- defensible, and is exactly the kind of follow-up mart worth building next.
--
-- MUST run AFTER the Lakeflow pipeline (src/pipelines/apex_pipeline.py) has
-- completed at least one run — reads silver.maintenance_work_orders, which the
-- pipeline creates.
-- =============================================================================

USE CATALOG IDENTIFIER(:catalog);

-- OR REPLACE, not IF NOT EXISTS: for a materialized VIEW, the SQL in this file is the
-- deployable artifact — a redeploy should always make the live definition match Git.
-- IF NOT EXISTS would silently keep the OLD query definition on every redeploy once the
-- MV exists once, which is exactly how this file's stop_month grain fix would have gone
-- unnoticed (found by actually hitting that while making this change).
CREATE OR REPLACE MATERIALIZED VIEW gold.equipment_reliability
COMMENT 'MTBF (hours between unplanned stops, target > 72h) and MTTR (minutes, target < 45) per equipment, per calendar month (plant-local). Not all-time — SUM(unplanned_stop_count) across months for an all-time total.'
AS
SELECT w.equipment_key,
       e.equipment_name,
       e.line_id,
       TRUNC(w.created_ts_local, 'MM')                                 AS stop_month,
       COUNT(*)                                                        AS unplanned_stop_count,
       ROUND(TIMESTAMPDIFF(HOUR, MIN(w.created_ts), MAX(w.created_ts))
             / NULLIF(COUNT(*) - 1, 0), 1)                             AS mtbf_hours,
       ROUND(AVG(w.downtime_minutes), 1)                               AS mttr_minutes
FROM silver.maintenance_work_orders w
JOIN ref.equipment_xref e USING (equipment_key)
WHERE w.planning_category = 'UNPLANNED'
GROUP BY 1, 2, 3, 4;
