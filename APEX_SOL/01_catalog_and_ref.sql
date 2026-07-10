-- =============================================================================
-- Apex Manufacturing Lakehouse — Catalog, Schemas, Reference Layer
-- Target: Unity Catalog. One catalog per environment (apex_dev / apex_stg / apex_prod),
-- injected by Asset Bundle variable ${catalog}. Schemas are stable across envs.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS ${catalog};

CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze COMMENT 'Raw, byte-faithful landing zone. Append-only.';
CREATE SCHEMA IF NOT EXISTS ${catalog}.silver COMMENT 'Cleaned, conformed, deduplicated. Analyst-readable.';
CREATE SCHEMA IF NOT EXISTS ${catalog}.gold   COMMENT 'Business KPIs and dimensional model. Genie-facing.';
CREATE SCHEMA IF NOT EXISTS ${catalog}.ref    COMMENT 'Crosswalks and seeds, versioned in Git.';

-- -----------------------------------------------------------------------------
-- REF: conformed identity crosswalks (seeded from src/mappings/*.csv via DAB job)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ${catalog}.ref.equipment_xref (
  equipment_key        STRING NOT NULL COMMENT 'Surrogate conformed key, e.g. EQP-101',
  source_equipment_id  STRING NOT NULL COMMENT 'SCADA/registry ID: EQ-1xx | EQUIP_2xx | 3xx',
  sap_asset_number     STRING          COMMENT 'SAP PM asset number A-xxx (assumption: suffix match — verify vs EQUI)',
  equipment_name       STRING,
  equipment_type       STRING,
  line_id              INT,
  criticality          STRING          COMMENT 'A/B/C, normalized upper-case',
  is_active            BOOLEAN,
  CONSTRAINT pk_equipment_xref PRIMARY KEY (equipment_key)
) COMMENT 'Single source of truth for equipment identity. Every silver table joins through this.';

CREATE TABLE IF NOT EXISTS ${catalog}.ref.line_xref (
  source_line_value STRING NOT NULL COMMENT 'Any observed encoding: 1, L1, Line 1, Line-1 ...',
  line_id           INT    NOT NULL,
  CONSTRAINT pk_line_xref PRIMARY KEY (source_line_value)
);

CREATE TABLE IF NOT EXISTS ${catalog}.ref.alarm_severity_xref (
  source_severity STRING NOT NULL,
  severity_norm   STRING NOT NULL COMMENT 'CRITICAL/HIGH/MEDIUM/LOW',
  severity_rank   INT    NOT NULL COMMENT '1 = most severe. ASSUMPTION: FactoryTalk 1=CRITICAL — verify with controls engineering',
  CONSTRAINT pk_sev_xref PRIMARY KEY (source_severity)
);

CREATE TABLE IF NOT EXISTS ${catalog}.ref.wo_type_xref (
  source_type       STRING NOT NULL,
  type_norm         STRING NOT NULL COMMENT 'PREVENTIVE/CORRECTIVE/BREAKDOWN',
  planning_category STRING NOT NULL COMMENT 'PLANNED | UNPLANNED — drives MTBF/MTTR',
  CONSTRAINT pk_wo_type_xref PRIMARY KEY (source_type)
);

CREATE TABLE IF NOT EXISTS ${catalog}.ref.product_sku (
  product_sku  STRING NOT NULL,
  product_name STRING,
  category     STRING COMMENT 'CSD | STW | JCE',
  unit_size_ml INT    COMMENT 'Parsed from product name; used to validate volume scale',
  CONSTRAINT pk_product_sku PRIMARY KEY (product_sku)
);

-- =============================================================================
-- GOLD: dimensional model + KPI marts (created by pipeline as materialized
-- views / streaming tables; DDL here shows target shape and physical design)
-- =============================================================================

-- Highest-volume table: sensor telemetry.
-- Physical design rationale in docs/01_architecture.md §Physical Table Design:
--   * Liquid Clustering, NOT hive partitioning + ZORDER
--   * cluster columns match dominant predicates: equipment, tag, time
--   * deletion vectors + predictive optimization on
CREATE TABLE IF NOT EXISTS ${catalog}.silver.sensor_readings (
  reading_ts             TIMESTAMP NOT NULL COMMENT 'UTC. Parsed from 3 source formats incl. epoch-ms',
  reading_ts_local       TIMESTAMP GENERATED ALWAYS AS (from_utc_timestamp(reading_ts, 'America/Chicago')) COMMENT 'Plant-local (Austin, TX)',
  equipment_key          STRING NOT NULL,
  sensor_tag             STRING NOT NULL,
  value                  DOUBLE COMMENT 'NULL when source was sentinel (-999.99) or empty',
  opc_quality_code       INT COMMENT '192=Good, 64=Uncertain, 0=Bad',
  reading_quality_status STRING COMMENT 'GOOD | SUSPECT | BAD — never named just "status"',
  is_good_quality        BOOLEAN,
  line_id                INT,
  _ingest_ts             TIMESTAMP,
  _source_file           STRING
)
CLUSTER BY (equipment_key, sensor_tag, reading_ts)
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.tuneFileSizesForRewrites' = 'true',
  'delta.dataSkippingStatsColumns' = 'reading_ts,equipment_key,sensor_tag,value,opc_quality_code',
  'delta.deletedFileRetentionDuration' = 'interval 7 days',
  'delta.logRetentionDuration' = 'interval 30 days'
);

-- OEE daily mart — the flagship Genie-facing table
CREATE MATERIALIZED VIEW IF NOT EXISTS ${catalog}.gold.oee_daily
COMMENT 'One row per line per day. OEE = availability * performance * quality. Target > 85%.'
AS
WITH runtime AS (
  SELECT line_id,
         DATE(start_ts_local)                                   AS production_date,
         SUM(TIMESTAMPDIFF(MINUTE, start_ts, end_ts))           AS run_minutes,
         SUM(total_units)                                       AS total_units,
         SUM(good_units)                                        AS good_units
  FROM ${catalog}.silver.production_runs
  WHERE run_status = 'COMPLETED'
  GROUP BY 1, 2
),
downtime AS (
  SELECT e.line_id,
         DATE(w.created_ts_local)         AS production_date,
         SUM(w.downtime_minutes)          AS unplanned_downtime_min
  FROM ${catalog}.silver.maintenance_work_orders w
  JOIN ${catalog}.ref.equipment_xref e ON w.equipment_key = e.equipment_key
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

-- MTBF / MTTR mart
CREATE MATERIALIZED VIEW IF NOT EXISTS ${catalog}.gold.equipment_reliability
COMMENT 'MTBF (hours between unplanned stops, target > 72h) and MTTR (minutes, target < 45) per equipment.'
AS
SELECT w.equipment_key,
       e.equipment_name,
       e.line_id,
       COUNT(*)                                                        AS unplanned_stop_count,
       ROUND(TIMESTAMPDIFF(HOUR, MIN(w.created_ts), MAX(w.created_ts))
             / NULLIF(COUNT(*) - 1, 0), 1)                             AS mtbf_hours,
       ROUND(AVG(w.downtime_minutes), 1)                               AS mttr_minutes
FROM ${catalog}.silver.maintenance_work_orders w
JOIN ${catalog}.ref.equipment_xref e USING (equipment_key)
WHERE w.planning_category = 'UNPLANNED'
GROUP BY 1, 2, 3;
