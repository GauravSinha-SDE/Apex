-- =============================================================================
-- Apex Manufacturing Lakehouse — Gold: Scrap Rate daily mart
-- Run via a Jobs sql_task.file with a named `catalog` parameter. See
-- 02a_gold_oee_daily.sql's header for the USE CATALOG / one-MV-per-file pattern
-- this follows, and 02b_gold_equipment_reliability.sql's header for why
-- CREATE OR REPLACE, not IF NOT EXISTS.
--
-- Grain: one row per line per plant-local production day. Target < 2% (plant KPI).
-- Simpler than OEE's quality component (good_units/total_units) only in that this
-- is scrap's own explicit ratio, not derived through the OEE formula — the two
-- are complementary, not duplicative: OEE.quality answers "how good was
-- production," scrap_rate answers "how much did we throw away," and they're
-- asked as genuinely separate questions by plant managers (see plant KPI table).
--
-- MUST run AFTER the Lakeflow pipeline has completed at least one run — reads
-- silver.production_runs, which the pipeline creates.
-- =============================================================================

USE CATALOG IDENTIFIER(:catalog);

CREATE OR REPLACE MATERIALIZED VIEW gold.scrap_rate_daily
COMMENT 'Daily scrap rate per line: rejected units / total units produced (plant-local day). Target < 2%.'
AS
SELECT line_id,
       DATE(start_ts_local)                                        AS production_date,
       SUM(scrap_units)                                            AS scrap_units,
       SUM(total_units)                                            AS total_units,
       ROUND(SUM(scrap_units) / NULLIF(SUM(total_units), 0), 4)    AS scrap_rate
FROM silver.production_runs
WHERE run_status = 'COMPLETED'
GROUP BY 1, 2;
