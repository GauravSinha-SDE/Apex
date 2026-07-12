-- =============================================================================
-- Apex Manufacturing Lakehouse — Gold: Critical Work Orders mart
-- Run via a Jobs sql_task.file with a named `catalog` parameter. See
-- 02a_gold_oee_daily.sql's header for the USE CATALOG / one-MV-per-file pattern
-- this follows, and 02b_gold_equipment_reliability.sql's header for why
-- CREATE OR REPLACE, not IF NOT EXISTS.
--
-- Grain: one row per technician per plant-local resolved-week. "Resolved" means
-- work_order_status = 'COMPLETED'; "critical" means priority = 'Critical' — an
-- explicit value in the source data (maintenance_logs.priority also has
-- High/Medium/Low), not inferred. Bucketed by completion week (completed_ts_local),
-- not creation week — "resolved last week" is about when the work finished, not
-- when it was opened. Closes example question 5 from the brief ("which
-- technician resolved the most critical work orders last week").
--
-- MUST run AFTER the Lakeflow pipeline has completed at least one run — reads
-- silver.maintenance_work_orders, which the pipeline creates.
-- =============================================================================

USE CATALOG IDENTIFIER(:catalog);

CREATE OR REPLACE MATERIALIZED VIEW gold.critical_work_orders
COMMENT 'Critical-priority work orders resolved, per technician per plant-local week. "Resolved" = work_order_status COMPLETED, "critical" = priority = Critical.'
AS
SELECT technician,
       CAST(DATE_TRUNC('WEEK', completed_ts_local) AS DATE)  AS resolved_week,
       COUNT(*)                                              AS critical_work_orders_resolved,
       ROUND(AVG(downtime_minutes), 1)                       AS avg_downtime_minutes
FROM silver.maintenance_work_orders
WHERE priority = 'Critical'
  AND work_order_status = 'COMPLETED'
GROUP BY 1, 2;
