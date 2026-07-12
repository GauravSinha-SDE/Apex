-- =============================================================================
-- Apex Manufacturing Lakehouse — Catalog, Schemas, Reference Layer
-- Target: Unity Catalog. One catalog per environment (apex_dev / apex_stg / apex_prod).
-- Run via a Jobs sql_task.file with a named `catalog` parameter, bound through
-- IDENTIFIER(:catalog || '.schema[.table]') wherever a catalog-qualified object is
-- referenced or created — NOT ${catalog} substitution, which is Databricks
-- SQL/notebook widget syntax that a Jobs sql_task.file never resolves (confirmed by
-- actually running this: the literal string "${catalog}" reached the warehouse and
-- failed to parse). See src/ddl/02a_gold_oee_daily.sql's header for the fuller story,
-- including why a notebook-based workaround was tried and abandoned.
-- =============================================================================

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.bronze') COMMENT 'Raw, byte-faithful landing zone. Append-only.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.silver') COMMENT 'Cleaned, conformed, deduplicated. Analyst-readable.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.gold')   COMMENT 'Business KPIs and dimensional model. Genie-facing.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.ref')    COMMENT 'Crosswalks and seeds, versioned in Git.';

-- -----------------------------------------------------------------------------
-- REF: conformed identity crosswalks (seeded from src/mappings/*.csv via DAB job)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.ref.equipment_xref') (
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

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.ref.line_xref') (
  source_line_value STRING NOT NULL COMMENT 'Any observed encoding: 1, L1, Line 1, Line-1 ...',
  line_id           INT    NOT NULL,
  CONSTRAINT pk_line_xref PRIMARY KEY (source_line_value)
);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.ref.alarm_severity_xref') (
  source_severity STRING NOT NULL,
  severity_norm   STRING NOT NULL COMMENT 'CRITICAL/HIGH/MEDIUM/LOW',
  severity_rank   INT    NOT NULL COMMENT '1 = most severe. ASSUMPTION: FactoryTalk 1=CRITICAL — verify with controls engineering',
  CONSTRAINT pk_sev_xref PRIMARY KEY (source_severity)
);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.ref.wo_type_xref') (
  source_type       STRING NOT NULL,
  type_norm         STRING NOT NULL COMMENT 'PREVENTIVE/CORRECTIVE/BREAKDOWN',
  planning_category STRING NOT NULL COMMENT 'PLANNED | UNPLANNED — drives MTBF/MTTR',
  CONSTRAINT pk_wo_type_xref PRIMARY KEY (source_type)
);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:catalog || '.ref.product_sku') (
  product_sku  STRING NOT NULL,
  product_name STRING,
  category     STRING COMMENT 'CSD | STW | JCE',
  unit_size_ml INT    COMMENT 'Parsed from product name; used to validate volume scale',
  CONSTRAINT pk_product_sku PRIMARY KEY (product_sku)
);

-- Gold KPI marts (oee_daily, equipment_reliability) are NOT defined here — they read
-- from silver.production_runs / silver.maintenance_work_orders, which the Lakeflow
-- pipeline creates, not this script. Running them here, before the pipeline has ever
-- executed, fails with TABLE_OR_VIEW_NOT_FOUND. See src/ddl/02a_gold_oee_daily.sql and
-- src/ddl/02b_gold_equipment_reliability.sql, which must run AFTER the pipeline's first
-- successful run — the apex_bootstrap job in databricks.yml sequences this correctly
-- (schemas/ref -> seeds -> pipeline -> OEE mart -> reliability mart).
