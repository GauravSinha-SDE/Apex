-- =============================================================================
-- Apex Manufacturing Lakehouse — Gold: First-Pass Yield mart
-- Run via a Jobs sql_task.file with a named `catalog` parameter. See
-- 02a_gold_oee_daily.sql's header for the USE CATALOG / one-MV-per-file pattern
-- this follows, and 02b_gold_equipment_reliability.sql's header for why
-- CREATE OR REPLACE, not IF NOT EXISTS.
--
-- Grain: one row per line per plant-local check day. Target > 98% (plant KPI).
--
-- ASSUMPTION worth flagging explicitly: "first check" is not a labeled concept
-- in the source LIMS export — quality_checks has no attempt/retry sequence
-- number. This defines "first check" as the earliest check_ts within each
-- distinct (batch_id, check_type, parameter) combination — i.e. the first time
-- THAT SPECIFIC measurement was taken on THAT batch, not the first check of any
-- kind on the batch (a batch legitimately gets multiple different check_types
-- as part of normal process, e.g. incoming + in_process; that's not a retry).
-- A later check on the same (batch, check_type, parameter) is then treated as a
-- recheck/retest and excluded from the yield calculation, same as the KPI
-- definition implies ("first check," not "eventual check"). Verify this
-- matches how LabWare LIMS actually models retests before trusting this mart.
--
-- MUST run AFTER the Lakeflow pipeline has completed at least one run — reads
-- silver.quality_checks, which the pipeline creates.
-- =============================================================================

USE CATALOG IDENTIFIER(:catalog);

CREATE OR REPLACE MATERIALIZED VIEW gold.first_pass_yield
COMMENT 'First-pass yield: % of (batch, check_type, parameter) combinations that passed on their earliest-recorded check. Target > 98%. See file header for the "first check" assumption — no retry/attempt number exists in the source data.'
AS
WITH first_checks AS (
  SELECT line_id, check_ts_local, check_result,
         ROW_NUMBER() OVER (PARTITION BY batch_id, check_type, parameter ORDER BY check_ts) AS check_seq
  FROM silver.quality_checks
)
SELECT line_id,
       DATE(check_ts_local)                                                          AS check_date,
       COUNT(*)                                                                      AS first_checks_total,
       SUM(CASE WHEN check_result = 'PASS' THEN 1 ELSE 0 END)                        AS first_checks_passed,
       ROUND(SUM(CASE WHEN check_result = 'PASS' THEN 1 ELSE 0 END)
             / NULLIF(COUNT(*), 0), 4)                                               AS first_pass_yield
FROM first_checks
WHERE check_seq = 1
GROUP BY 1, 2;
