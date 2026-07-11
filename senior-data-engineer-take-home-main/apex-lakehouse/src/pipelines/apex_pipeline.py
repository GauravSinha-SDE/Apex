"""
Apex Manufacturing — Lakeflow Declarative Pipeline (bronze → silver → gold)

Design notes
------------
* Bronze: Auto Loader, all-STRING schema + rescued data column. Byte-faithful, append-only.
* Silver: every data-quality fix from docs/02_data_quality.md is implemented here,
  with @dlt expectations so violations are OBSERVABLE (event log) not silent.
* Quarantine pattern: expect_or_drop on the main table + an inverse-filtered
  quarantine table carrying a reason code — nothing is ever lost.
* Gold KPI marts are defined as SQL materialized views (src/ddl) and referenced
  by the same pipeline; kept in SQL because analysts own that layer.
* Multi-schema publish: table `name=` is schema-qualified ("bronze.x" / "silver.x")
  rather than underscore-prefixed. One Lakeflow Declarative Pipeline can publish to
  several schemas in a catalog this way; the pipeline's own `schema` bundle setting
  is just the fallback default for any (none, here) unqualified table name. This is
  what makes bronze.* / silver.* line up exactly with the ${catalog}.silver.* names
  the gold materialized views in 01_catalog_and_ref.sql already query.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

RAW = spark.conf.get("apex.raw_path")          # e.g. /Volumes/apex_prod/bronze/landing
CATALOG = spark.conf.get("apex.catalog")

# ---------------------------------------------------------------------------
# Shared parsing helpers
# ---------------------------------------------------------------------------

def parse_multi_ts(col):
    """Handle the three observed timestamp encodings (DQ-04/05):
    ISO-8601 (with/without Z), 'yyyy-MM-dd HH:mm:ss', US 'MM/dd/yyyy HH:mm',
    and 13-digit epoch-milliseconds. Unparseable -> NULL -> quarantined."""
    c = F.col(col)
    return (
        F.when(c.rlike(r"^\d{13}$"), F.timestamp_millis(c.cast("long")))
         .when(c.rlike(r"^\d{4}-\d{2}-\d{2}T.*Z$"), F.to_timestamp(c))
         .when(c.rlike(r"^\d{4}-\d{2}-\d{2}[T ]"),                       # naked local time
               F.to_utc_timestamp(F.to_timestamp(F.regexp_replace(c, "T", " ")),
                                  "America/Chicago"))
         .when(c.rlike(r"^\d{2}/\d{2}/\d{4}"),
               F.to_utc_timestamp(F.to_timestamp(c, "MM/dd/yyyy HH:mm"),
                                  "America/Chicago"))
    )

def with_line_id(df, src_col):
    line_xref = spark.table(f"{CATALOG}.ref.line_xref")
    return (df.join(F.broadcast(line_xref),
                    df[src_col] == line_xref.source_line_value, "left")
              .drop("source_line_value"))

def with_equipment_key(df, src_col, via="source_equipment_id"):
    """Resolve any equipment alias (SCADA id or SAP asset number) to the conformed key."""
    xref = spark.table(f"{CATALOG}.ref.equipment_xref") \
                .select("equipment_key", "source_equipment_id", "sap_asset_number")
    return df.join(F.broadcast(xref), df[src_col] == xref[via], "left")

# ---------------------------------------------------------------------------
# BRONZE — one pattern per source file family
# ---------------------------------------------------------------------------

def bronze_table(name, path_glob, fmt="csv"):
    @dlt.table(name=f"bronze.{name}",   # schema-qualified: publishes into ${CATALOG}.bronze, not a single default schema
               comment=f"Raw {name} landing. All STRING, append-only.",
               table_properties={"quality": "bronze"})
    def _t():
        return (spark.readStream.format("cloudFiles")
                .option("cloudFiles.format", fmt)
                .option("cloudFiles.inferColumnTypes", "false")   # everything STRING
                .option("cloudFiles.schemaEvolutionMode", "rescue")
                .load(f"{RAW}/{path_glob}")
                .withColumn("_ingest_ts", F.current_timestamp())
                .withColumn("_source_file", F.col("_metadata.file_path")))
    return _t

bronze_table("sensor_readings", "sensor_readings/*")
bronze_table("production_runs", "production_runs/*")
bronze_table("maintenance_logs", "maintenance_logs/*")
bronze_table("quality_checks", "quality_checks/*")
bronze_table("alarms_events", "alarms_events/*")
bronze_table("equipment_registry", "equipment_registry/*", fmt="json")

# ---------------------------------------------------------------------------
# SILVER — sensor readings (highest volume, most fixes)
# ---------------------------------------------------------------------------

@dlt.table(name="silver.sensor_readings",   # matches gold DDL's ${catalog}.silver.sensor_readings target shape
           comment="Cleaned telemetry. UTC timestamps, conformed equipment, sentinel-scrubbed.",
           cluster_by=["equipment_key", "sensor_tag", "reading_ts"],
           table_properties={"quality": "silver",
                             "delta.enableDeletionVectors": "true"})
@dlt.expect_or_drop("valid_timestamp", "reading_ts IS NOT NULL")
@dlt.expect_or_drop("known_equipment", "equipment_key IS NOT NULL")
@dlt.expect("value_present", "value IS NOT NULL")            # warn-only: tracked, kept
def silver_sensor_readings():
    df = dlt.read_stream("bronze.sensor_readings")
    df = df.withColumn("reading_ts", parse_multi_ts("timestamp"))
    # DQ-06: sentinel scrub — -999.99 family and impossible negatives on physical tags
    df = df.withColumn(
        "value",
        F.when(F.col("value").cast(DoubleType()) <= -999.0, F.lit(None))
         .when(F.col("sensor_tag").rlike("(?i)vib") &
               (F.col("value").cast(DoubleType()) < 0), F.lit(None))
         .otherwise(F.col("value").cast(DoubleType())))
    df = with_equipment_key(df, "equipment_id")
    df = with_line_id(df, "line_id")
    # DQ-08: deterministic dedupe on natural key (watermark bounds state at scale)
    df = (df.withWatermark("reading_ts", "6 hours")   # sized for PI store-and-forward (Part 3 Q5)
            .dropDuplicates(["reading_ts", "equipment_key", "sensor_tag"]))
    return df.select(
        "reading_ts", "equipment_key", "sensor_tag", "value",
        F.col("quality").cast("int").alias("opc_quality_code"),
        F.col("status").alias("reading_quality_status"),      # DQ-13: renamed, never 'status'
        (F.col("quality") == "192").alias("is_good_quality"),
        "line_id", "_ingest_ts", "_source_file")

@dlt.table(name="silver.quarantine_sensor_readings",
           comment="Telemetry rows failing hard expectations, with reason codes. Nothing is lost.")
def quarantine_sensor_readings():
    df = dlt.read_stream("bronze.sensor_readings") \
            .withColumn("reading_ts", parse_multi_ts("timestamp"))
    df = with_equipment_key(df, "equipment_id")
    return (df.filter("reading_ts IS NULL OR equipment_key IS NULL")
              .withColumn("quarantine_reason",
                          F.when(F.col("reading_ts").isNull(), "UNPARSEABLE_TIMESTAMP")
                           .otherwise("UNKNOWN_EQUIPMENT_ID")))

# ---------------------------------------------------------------------------
# SILVER — production runs (unit + scale normalization, DQ-11)
# ---------------------------------------------------------------------------

@dlt.table(name="silver.production_runs",
           comment="Runs with volumes normalized to litres (unit AND 1000x scale fix).",
           table_properties={"quality": "silver"})
@dlt.expect_or_fail("valid_line", "line_id IS NOT NULL")
@dlt.expect("unit_math_holds", "good_units + scrap_units = total_units OR run_status <> 'COMPLETED'")
def silver_production_runs():
    df = dlt.read_stream("bronze.production_runs")
    unit_factor = (F.when(F.col("volume_unit").isin("gal", "gallons"), F.lit(3.785))
                    .otherwise(F.lit(1.0)))
    df = (df
          .withColumn("start_ts", parse_multi_ts("start_time"))
          .withColumn("end_ts", parse_multi_ts("end_time"))
          .withColumn("actual_volume_l",
                      F.col("actual_volume").cast("double") * unit_factor * 1000)  # DQ-11 scale
          .withColumn("target_volume_l",
                      F.col("target_volume").cast("double") * unit_factor * 1000)
          .withColumnRenamed("status", "run_status"))                              # DQ-13
    df = with_line_id(df, "line_id")
    return df.drop("volume_unit", "actual_volume", "target_volume",
                   "start_time", "end_time")

# ---------------------------------------------------------------------------
# SILVER — maintenance work orders (SAP asset crosswalk, type normalization)
# ---------------------------------------------------------------------------

@dlt.table(name="silver.maintenance_work_orders",
           comment="Work orders joined to conformed equipment via SAP asset crosswalk.",
           table_properties={"quality": "silver"})
@dlt.expect_or_drop("known_asset", "equipment_key IS NOT NULL")
def silver_maintenance_work_orders():
    df = dlt.read_stream("bronze.maintenance_logs")
    df = with_equipment_key(df, "asset_number", via="sap_asset_number")   # DQ-01
    wo_types = spark.table(f"{CATALOG}.ref.wo_type_xref")
    df = df.join(F.broadcast(wo_types), df["type"] == wo_types.source_type, "left")  # DQ-10
    return (df
            .withColumn("created_ts", parse_multi_ts("created_date"))
            .withColumn("completed_ts", parse_multi_ts("completed_date"))
            .withColumn("downtime_minutes", F.col("downtime_minutes").cast("int"))
            .withColumnRenamed("status", "work_order_status")            # DQ-13
            .select("work_order_id", "equipment_key", "description",
                    "work_order_status", "priority",
                    F.col("type_norm").alias("maintenance_type"),
                    "planning_category", "created_ts", "completed_ts",
                    "downtime_minutes", "technician", "parts_used"))

# ---------------------------------------------------------------------------
# SILVER — alarms (severity normalization) and quality checks (line normalization)
# ---------------------------------------------------------------------------

@dlt.table(name="silver.alarms", table_properties={"quality": "silver"},
           comment="Alarms with unified severity scale and renamed lifecycle column.")
@dlt.expect_or_drop("known_severity", "severity_norm IS NOT NULL")
def silver_alarms():
    df = dlt.read_stream("bronze.alarms_events")
    sev = spark.table(f"{CATALOG}.ref.alarm_severity_xref")
    df = df.join(F.broadcast(sev), df.severity == sev.source_severity, "left")   # DQ-09
    df = with_equipment_key(df, "equipment_id")
    df = with_line_id(df, "line_id")
    return (df.withColumn("alarm_ts", parse_multi_ts("timestamp"))
              .withColumn("acknowledged_ts", parse_multi_ts("acknowledged_at"))
              .withColumnRenamed("status", "alarm_state")                        # DQ-13
              .select("alarm_id", "alarm_ts", "equipment_key", "alarm_tag",
                      "description", "severity_norm", "severity_rank",
                      "alarm_state", "acknowledged_by", "acknowledged_ts",
                      F.col("duration_seconds").cast("int").alias("duration_seconds"),
                      "line_id"))

@dlt.table(name="silver.quality_checks", table_properties={"quality": "silver"},
           comment="LIMS results with normalized line ids and spec-conformance flag.")
@dlt.expect_or_fail("valid_line", "line_id IS NOT NULL")
def silver_quality_checks():
    df = dlt.read_stream("bronze.quality_checks")
    df = with_line_id(df, "line_id")            # DQ-02: handles 'Line 1', 'Line-2', '3'
    return (df.withColumn("check_ts", parse_multi_ts("check_timestamp"))
              .withColumn("value", F.col("value").cast("double"))
              .withColumn("lower_spec", F.col("lower_spec").cast("double"))
              .withColumn("upper_spec", F.col("upper_spec").cast("double"))
              .withColumn("in_spec", F.col("value").between(F.col("lower_spec"),
                                                            F.col("upper_spec")))
              .withColumnRenamed("result", "check_result")
              .drop("check_timestamp"))

# ---------------------------------------------------------------------------
# SILVER — equipment dimension, SCD Type 2 (DQ-03, EQ-110 → EQ-111 story)
# ---------------------------------------------------------------------------

dlt.create_streaming_table("silver.dim_equipment",
    comment="SCD2 equipment dimension. History preserved across replacements/decommissions.")

dlt.create_auto_cdc_flow(
    target="silver.dim_equipment",
    source="bronze.equipment_registry",
    keys=["equipment_id"],
    sequence_by="_ingest_ts",
    stored_as_scd_type=2,
)
