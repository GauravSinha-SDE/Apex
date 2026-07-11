# Databricks notebook source
# MAGIC %md
# MAGIC # Apex Manufacturing — Data Exploration & Quality Profiling
# MAGIC
# MAGIC **Purpose:** systematically profile the six source extracts *before* designing the lakehouse.
# MAGIC Every data-quality issue documented in `docs/02_data_quality.md` is **discovered and evidenced here**,
# MAGIC not asserted. Re-run this notebook to reproduce every finding.
# MAGIC
# MAGIC **Method:** load everything as `STRING` (bronze-style, byte-faithful), then interrogate.
# MAGIC Casting on read would silently destroy the very defects we need to find — the epoch-millisecond
# MAGIC timestamps are the clearest example: a naive `CAST` nulls 42 rows without complaint.
# MAGIC
# MAGIC **Outcome:** a defect inventory that drives (a) the Silver expectations, (b) the `ref.*` crosswalk
# MAGIC seeds, and (c) the Genie semantic instructions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Setup
# MAGIC Point `BASE` at wherever the sample extracts landed — a UC Volume in a real deployment.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

BASE = "/Volumes/apex_dev/bronze/landing"   # <- adjust to your Volume path
# For a quick local test you can also use a DBFS path, e.g. "dbfs:/FileStore/apex"

def load_csv(name):
    """Bronze-style read: everything STRING. No inference — inference hides defects."""
    return (spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(f"{BASE}/{name}.csv"))

sensor = load_csv("sensor_readings")
runs   = load_csv("production_runs")
maint  = load_csv("maintenance_logs")
qc     = load_csv("quality_checks")
alarms = load_csv("alarms_events")
reg    = spark.read.option("multiLine", True).json(f"{BASE}/equipment_registry.json")

for n, df in [("sensor_readings", sensor), ("production_runs", runs),
              ("maintenance_logs", maint), ("quality_checks", qc),
              ("alarms_events", alarms), ("equipment_registry", reg)]:
    print(f"{n:22s} {df.count():>6,} rows | {len(df.columns)} cols")

# Expected on the sample extract:
#   sensor_readings           515 rows | 7 cols
#   production_runs            60 rows | 15 cols
#   maintenance_logs           80 rows | 11 cols
#   quality_checks            100 rows | 12 cols
#   alarms_events             150 rows | 11 cols
#   equipment_registry         30 rows | 12 cols

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1 · Identity: can these files even be joined?
# MAGIC
# MAGIC This is the first question to ask of any multi-source dataset, and here the answer is **no**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ-01 · Equipment is identified four different ways
# MAGIC Three conventions inside the OT files (one per line, apparently by install vintage),
# MAGIC plus a completely separate SAP asset number in the maintenance extract.

# COMMAND ----------

display(
    sensor.select("equipment_id").distinct()
    .withColumn("id_scheme",
        F.when(F.col("equipment_id").rlike("^EQ-"),      "EQ-1xx      (Line 1)")
         .when(F.col("equipment_id").rlike("^EQUIP_"),   "EQUIP_2xx   (Line 2)")
         .when(F.col("equipment_id").rlike("^[0-9]+$"),  "bare 3xx    (Line 3)")
         .otherwise("UNKNOWN"))
    .groupBy("id_scheme")
    .agg(F.sort_array(F.collect_set("equipment_id")).alias("examples"))
    .orderBy("id_scheme")
)

# COMMAND ----------

# The maintenance extract uses SAP asset numbers instead — a fourth namespace.
print("SAP asset numbers in maintenance_logs:")
display(maint.select("asset_number").distinct().orderBy("asset_number"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The join that fails
# MAGIC If we naively join work orders to the equipment registry on the obvious columns,
# MAGIC we get **zero rows**. Silently. No error, no warning — just an empty result set and a
# MAGIC dashboard that reports no maintenance ever happened.

# COMMAND ----------

naive = maint.join(reg, maint.asset_number == reg.equipment_id, "inner")
print(f"Naive join  maintenance.asset_number == registry.equipment_id  ->  {naive.count()} rows")
print("=> Work orders cannot be joined to equipment, sensors, or alarms at all.")
print("=> MTBF, MTTR and OEE availability are ALL unanswerable until this is fixed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### The crosswalk that fixes it
# MAGIC Both namespaces share a numeric suffix (`EQ-101` / `EQUIP_201` / `301` ↔ `A-101` / `A-201` / `A-301`).
# MAGIC That relationship is an **inference, not a guarantee** — in production I would confirm it against the
# MAGIC SAP `EQUI` equipment master rather than trust a pattern. It is recorded as an explicit assumption.

# COMMAND ----------

xref = (reg
    .withColumn("num", F.regexp_extract("equipment_id", r"(\d{3})$", 1))
    .withColumn("equipment_key", F.concat(F.lit("EQP-"), F.col("num")))
    .withColumn("sap_asset_number", F.concat(F.lit("A-"), F.col("num")))
    .select("equipment_key", F.col("equipment_id").alias("source_equipment_id"),
            "sap_asset_number", "name", "line"))

fixed = maint.join(xref, maint.asset_number == xref.sap_asset_number, "inner")
print(f"With crosswalk -> {fixed.count()} of {maint.count()} work orders joined")
print(f"Assets resolved: {fixed.select('asset_number').distinct().count()} "
      f"of {maint.select('asset_number').distinct().count()}")
display(xref.orderBy("equipment_key"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ-02 · `line_id` is encoded differently in every single file
# MAGIC Including **inconsistently within one file** — `quality_checks` alone contains three formats.

# COMMAND ----------

for name, df, col in [("sensor_readings",    sensor, "line_id"),
                      ("production_runs",    runs,   "line_id"),
                      ("alarms_events",      alarms, "line_id"),
                      ("quality_checks",     qc,     "line_id"),
                      ("equipment_registry", reg,    "line")]:
    vals = sorted([r[0] for r in df.select(col).distinct().collect() if r[0]])
    print(f"{name:20s} -> {vals}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Impact:** any "OEE by line" or "scrap by line" aggregation fragments into phantom lines.
# MAGIC **Fix:** a `ref.line_xref` seed mapping every observed variant → `line_id INT`, applied in Silver.
# MAGIC Unmapped values fail a DLT expectation rather than passing through silently.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2 · Time: the sensor timestamps are three different things

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ-04 · Three timestamp encodings in one column — including raw epoch-millis

# COMMAND ----------

ts_profile = sensor.withColumn("ts_format",
    F.when(F.col("timestamp").rlike(r"^\d{13}$"),              "EPOCH_MILLIS  (13-digit)")
     .when(F.col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2}T.*Z$"), "ISO-8601 UTC  (…Z)")
     .when(F.col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2} "),     "NAKED LOCAL   (no TZ)")
     .otherwise("UNPARSEABLE"))

display(ts_profile.groupBy("ts_format").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why this is dangerous: the silent data loss
# MAGIC A standard `CAST(timestamp AS TIMESTAMP)` — what almost every pipeline does by default —
# MAGIC **nulls 42 rows (8% of telemetry) without raising a single error.**

# COMMAND ----------

lost = sensor.filter(F.col("timestamp").cast("timestamp").isNull())
print(f"Rows silently NULLed by a naive CAST: {lost.count()} of {sensor.count()} "
      f"({100*lost.count()/sensor.count():.1f}%)")
display(lost.select("timestamp", "equipment_id", "sensor_tag", "value").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The multi-format parser (this goes into Silver)
# MAGIC Try each known encoding in turn; quarantine anything that matches none. Never drop silently.
# MAGIC
# MAGIC **Timezone assumption:** plant is Austin, TX. `Z`-suffixed values are UTC; naked values are
# MAGIC treated as plant-local (`America/Chicago`) and converted to UTC. This *must* be confirmed with the
# MAGIC historian team — a 5–6 hour error here would corrupt every shift-level KPI.

# COMMAND ----------

parsed = sensor.withColumn("reading_ts",
    F.when(F.col("timestamp").rlike(r"^\d{13}$"),
           F.timestamp_millis(F.col("timestamp").cast("long")))
     .when(F.col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2}T.*Z$"),
           F.to_timestamp("timestamp"))
     .when(F.col("timestamp").rlike(r"^\d{4}-\d{2}-\d{2} "),
           F.to_utc_timestamp(F.to_timestamp("timestamp"), "America/Chicago")))

print(f"Rows still unparsed after multi-format handling: "
      f"{parsed.filter(F.col('reading_ts').isNull()).count()}  (target: 0)")
display(parsed.select("timestamp", "reading_ts").limit(8))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3 · Values: sentinels, quality codes, and duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ-06 · Historian error sentinels masquerading as readings
# MAGIC `-999.99` is a classic PLC/historian "no data" code. Left in place, a single one shifts a
# MAGIC daily temperature average by several degrees — enough to fire a false process alert.

# COMMAND ----------

v = sensor.withColumn("v", F.col("value").cast(DoubleType()))
print("Sentinel  (-999.99)        :", v.filter(F.col("v") <= -999).count())
print("Empty / NULL value         :", v.filter(F.col("v").isNull()).count())
print("Negative vibration (impossible):",
      v.filter(F.col("sensor_tag").rlike("(?i)vib") & (F.col("v") < 0)).count())
display(v.filter((F.col("v") <= -999) |
                 (F.col("sensor_tag").rlike("(?i)vib") & (F.col("v") < 0)))
         .select("timestamp", "equipment_id", "sensor_tag", "value", "quality"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ-07 · OPC quality codes must gate every analytic
# MAGIC `192 = Good · 64 = Uncertain · 0 = Bad`. **11.3% of readings are not trustworthy.**
# MAGIC Gold aggregates filter to `quality = 192`; Silver keeps everything with an `is_good_quality` flag
# MAGIC so the bad data remains auditable.

# COMMAND ----------

display(sensor.groupBy("quality", "status").count().orderBy(F.desc("count")))

total = sensor.count()
bad   = sensor.filter(F.col("quality") != "192").count()
print(f"Untrustworthy: {bad}/{total} = {100*bad/total:.1f}% — MUST be excluded from KPI aggregates")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ-08 · Duplicate readings from store-and-forward replay
# MAGIC Expected behaviour for a historian recovering after a network outage — but the pipeline must be idempotent.

# COMMAND ----------

dups = (sensor.groupBy("timestamp", "equipment_id", "sensor_tag")
        .count().filter("count > 1"))
print(f"Duplicate natural keys: {dups.count()}")
display(dups)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4 · Categoricals: two coding systems, one column

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ-09 · Alarm severity is *both* numeric and text
# MAGIC The mapping direction is **not inferable from the data**. Is `1` the most severe or the least?
# MAGIC I assume the FactoryTalk convention (`1 = CRITICAL … 4 = LOW`), and flag it loudly:
# MAGIC **inverting this inverts every "critical alarms" answer the plant managers will ever get.**

# COMMAND ----------

display(
    alarms.withColumn("coding_system",
        F.when(F.col("severity").rlike("^[0-9]+$"), "NUMERIC")
         .otherwise("TEXT"))
    .groupBy("coding_system", "severity").count()
    .orderBy("coding_system", "severity")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ-10 · Maintenance type synonyms corrupt MTBF
# MAGIC `Preventive` and `PM` are the same thing. MTBF counts **unplanned** stops only — if `PM` rows are
# MAGIC miscounted as unplanned, the KPI is wrong. Resolved via `ref.wo_type_xref` → `planning_category`.

# COMMAND ----------

display(maint.groupBy("type").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5 · The trap: volume units *and* scale
# MAGIC
# MAGIC ### DQ-11 · Fixing the unit label is not enough
# MAGIC `volume_unit` is inconsistent (`liters` / `L` / `gal` / `gallons`) — that much is obvious.
# MAGIC The subtle part: **the values are also on the wrong scale.**

# COMMAND ----------

display(runs.groupBy("volume_unit").count())

# COMMAND ----------

# MAGIC %md
# MAGIC **The cross-check that reveals it.** CSD-001 is a 355 ml SKU. If a run produced 38,312 units,
# MAGIC it must have used ≈ 13,600 litres. The file reports `13.2 liters`.
# MAGIC The values are in **thousands of litres (kL)**, mislabeled as litres.
# MAGIC
# MAGIC A pipeline that "fixes the units" by converting gallons→litres and stops there
# MAGIC still carries a **1000× error**. This is why unit normalization must be validated against
# MAGIC an independent quantity (`units × sku_size_ml`), not taken on faith from the label.

# COMMAND ----------

display(
    runs.filter(F.col("total_units").isNotNull() & (F.col("total_units") != ""))
    .withColumn("units",            F.col("total_units").cast("int"))
    .withColumn("reported_volume",  F.col("actual_volume").cast("double"))
    .withColumn("implied_litres",   F.round(F.col("units") * 0.355, 1))   # 355 ml SKU
    .withColumn("ratio",            F.round(F.col("implied_litres") /
                                            F.col("reported_volume"), 0))
    .select("run_id", "product_sku", "units", "reported_volume", "volume_unit",
            "implied_litres", "ratio")
    .limit(8)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6 · Semantics: the word "status" means five different things

# COMMAND ----------

# MAGIC %md
# MAGIC ### DQ-13 · The single biggest threat to a natural-language interface
# MAGIC A plant manager asking *"what's the status of Line 2?"* could mean any of five columns.
# MAGIC No amount of prompt engineering fixes an ambiguous schema — **the schema itself must be disambiguated.**

# COMMAND ----------

for name, df in [("sensor_readings",    sensor),
                 ("production_runs",    runs),
                 ("maintenance_logs",   maint),
                 ("alarms_events",      alarms),
                 ("equipment_registry", reg)]:
    if "status" in df.columns:
        vals = sorted([r[0] for r in df.select("status").distinct().collect() if r[0]])
        print(f"{name:20s} status -> {vals}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Resolution (architectural, not cosmetic):** no Gold column is ever named `status`.
# MAGIC Each is renamed to be self-describing, so both a human and an LLM can disambiguate from the schema alone:
# MAGIC
# MAGIC | Source | Renamed to |
# MAGIC |---|---|
# MAGIC | `sensor_readings.status` | `reading_quality_status` |
# MAGIC | `production_runs.status` | `run_status` |
# MAGIC | `maintenance_logs.status` | `work_order_status` |
# MAGIC | `alarms_events.status` | `alarm_state` |
# MAGIC | `equipment_registry.status` | `equipment_operational_status` |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7 · Equipment registry: the master data is the messiest file

# COMMAND ----------

# install_date has FOUR formats — one of them ambiguous (day-first vs month-first)
display(
    reg.withColumn("date_format",
        F.when(F.col("install_date").rlike(r"^\d{4}-\d{2}-\d{2}$"),        "ISO  yyyy-MM-dd")
         .when(F.col("install_date").rlike(r"^\d{2}-[A-Za-z]{3}-\d{4}$"),  "dd-MMM-yyyy")
         .when(F.col("install_date").rlike(r"^\d{4}/\d{2}/\d{2}$"),        "yyyy/MM/dd")
         .when(F.col("install_date").rlike(r"^\d{2}/\d{2}/\d{4}$"),        "dd/MM/yyyy  <- AMBIGUOUS")
         .otherwise("other"))
    .groupBy("date_format").count().orderBy(F.desc("count"))
)

# COMMAND ----------

print("criticality (note the stray lowercase):",
      sorted([r[0] for r in reg.select("criticality").distinct().collect()]))
print("\nequipment 'type' (casing chaos — same type, different strings):")
for t in sorted([r[0] for r in reg.select("type").distinct().collect()]):
    print("   ", t)

# COMMAND ----------

# MAGIC %md
# MAGIC ### The SCD Type 2 case, hiding in plain sight
# MAGIC `EQ-110` is **DECOMMISSIONED** and was replaced by `EQ-111` in Dec 2025.
# MAGIC Historical sensor readings and work orders still reference the old asset.
# MAGIC A Type-1 overwrite would orphan that history — hence an SCD2 equipment dimension.

# COMMAND ----------

display(reg.filter(F.col("equipment_id").isin("EQ-110", "EQ-111"))
           .select("equipment_id", "name", "status", "install_date",
                   "last_maintenance", "specs"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8 · What *passed* — encoded as expectations anyway
# MAGIC
# MAGIC Good profiling reports the clean results too. These hold today, but "clean today" is not
# MAGIC "clean forever" — each becomes a DLT expectation so a future regression is caught at ingest.

# COMMAND ----------

bad_math = (runs.filter(F.col("total_units").isNotNull() & (F.col("total_units") != ""))
            .withColumn("t", F.col("total_units").cast("int"))
            .withColumn("g", F.col("good_units").cast("int"))
            .withColumn("s", F.col("scrap_units").cast("int"))
            .filter(F.col("g") + F.col("s") != F.col("t")))
print(f"Runs where good + scrap != total ......... {bad_math.count()}  (expect 0)")

qc_bad = (qc.withColumn("v",  F.col("value").cast("double"))
            .withColumn("lo", F.col("lower_spec").cast("double"))
            .withColumn("hi", F.col("upper_spec").cast("double"))
            .withColumn("in_spec", F.col("v").between(F.col("lo"), F.col("hi")))
            .filter((F.upper(F.col("result")) == "PASS") != F.col("in_spec")))
print(f"QC rows where result contradicts specs ... {qc_bad.count()}  (expect 0)")

orphans = maint.join(xref, maint.asset_number == xref.sap_asset_number, "left_anti")
print(f"Work orders with unresolvable assets ..... {orphans.count()}  (expect 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9 · Defect inventory → architecture
# MAGIC
# MAGIC | ID | Defect | Severity | Where it is fixed |
# MAGIC |---|---|---|---|
# MAGIC | DQ-01 | Four equipment ID namespaces (incl. SAP `A-xxx`) | 🔴 blocks all joins | `ref.equipment_xref` conformed dimension |
# MAGIC | DQ-02 | `line_id` encoded 4 ways, inconsistent within a file | 🔴 fragments every per-line KPI | `ref.line_xref` + `expect_or_fail` |
# MAGIC | DQ-03 | Decommissioned asset still present (EQ-110→111) | 🟠 | SCD2 equipment dimension |
# MAGIC | DQ-04 | 3 timestamp formats incl. epoch-ms (8% silently lost) | 🔴 | multi-format parser + quarantine |
# MAGIC | DQ-05 | Mixed ISO / US date formats; ambiguous day-first | 🟠 | same parser, range-check disambiguation |
# MAGIC | DQ-06 | `-999.99` sentinels, impossible negatives | 🟠 corrupts averages | Silver expectation → quarantine |
# MAGIC | DQ-07 | OPC quality: 11.3% untrustworthy | 🟠 | `is_good_quality`; Gold filters to 192 |
# MAGIC | DQ-08 | Duplicate keys from store-and-forward | 🟡 | watermarked dedupe (idempotent) |
# MAGIC | DQ-09 | Alarm severity: numeric *and* text in one column | 🔴 | `ref.alarm_severity_xref` (assumption flagged) |
# MAGIC | DQ-10 | `Preventive` vs `PM` synonyms | 🟠 corrupts MTBF | `ref.wo_type_xref` → `planning_category` |
# MAGIC | DQ-11 | Volume units inconsistent **and** 1000× scale error | 🟠 | normalize + validate vs `units × sku_size` |
# MAGIC | DQ-12 | `specs` polymorphic (JSON / text / null) | 🟡 | `try_parse_json` → VARIANT, keep raw |
# MAGIC | DQ-13 | `status` means 5 different things | 🔴 breaks NL queries | rename every one in Silver/Gold |
# MAGIC
# MAGIC ### Three principles this exploration produced
# MAGIC
# MAGIC 1. **Load raw, interrogate after.** Schema inference on read would have hidden the epoch-ms
# MAGIC    timestamps, the sentinels, and the mixed severity coding. Bronze stays all-`STRING` for a reason.
# MAGIC 2. **Quarantine, never drop.** Every rejected row lands in a `quarantine_*` table with a reason code.
# MAGIC    In a regulated food-and-beverage plant, silently discarding data is not acceptable.
# MAGIC 3. **Identity is the architecture.** The crosswalk tables are not cleanup chores — they are the
# MAGIC    integration between OT and IT that this plant has never had. Everything else depends on them.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next: `src/pipelines/apex_pipeline.py`
# MAGIC Each finding above maps to a specific DLT expectation or transformation in the Lakeflow
# MAGIC Declarative Pipeline. The pipeline is where these fixes become **enforced and monitored**,
# MAGIC rather than merely documented.
