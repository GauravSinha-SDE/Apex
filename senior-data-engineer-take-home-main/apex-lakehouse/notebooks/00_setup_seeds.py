# Databricks notebook source
# MAGIC %md
# MAGIC # Apex Manufacturing — Reference Seed Loader
# MAGIC
# MAGIC **Purpose:** load the Git-versioned crosswalk seeds in `src/mappings/*.csv` into the
# MAGIC `${catalog}.ref.*` tables created by `src/ddl/01_catalog_and_ref.sql`. These are the
# MAGIC identity-resolution tables every Silver join depends on (`equipment_xref`, `line_xref`,
# MAGIC `alarm_severity_xref`, `wo_type_xref`, and any future seed dropped in the same folder,
# MAGIC e.g. `product_sku`).
# MAGIC
# MAGIC **Run order:** after the DDL script (schemas + `ref` table shapes must already exist),
# MAGIC before the Lakeflow pipeline (Silver joins fail closed — `expect_or_drop` — on an
# MAGIC unresolved key, so an empty `ref` table doesn't corrupt data, it just drops everything).
# MAGIC
# MAGIC **Convention:** filename stem == table name. `equipment_xref.csv` → `ref.equipment_xref`.
# MAGIC This is deliberate: adding a new seed is "drop a CSV whose columns match an existing
# MAGIC `ref` table DDL", no code change here.
# MAGIC
# MAGIC **Load semantics:** full overwrite per table (`ref/*` is Git-owned — the CSV is the
# MAGIC source of truth, not the table). Small dimension tables, so a full replace is cheap and
# MAGIC avoids silently accumulating stale rows a merge key might miss.

# COMMAND ----------

dbutils.widgets.text("catalog", "apex_dev")
dbutils.widgets.text("mappings_path", "")

CATALOG = dbutils.widgets.get("catalog")
MAPPINGS_PATH = dbutils.widgets.get("mappings_path")

assert MAPPINGS_PATH, (
    "mappings_path widget is empty. When run via the apex_bootstrap job this is set to "
    "${workspace.file_path}/src/mappings by databricks.yml; for an ad-hoc run, pass the "
    "Workspace Files path to src/mappings explicitly."
)

print(f"catalog        = {CATALOG}")
print(f"mappings_path  = {MAPPINGS_PATH}")

# COMMAND ----------

from pathlib import Path
from pyspark.sql import functions as F

seed_files = sorted(Path(MAPPINGS_PATH).glob("*.csv"))
assert seed_files, f"No CSV files found under {MAPPINGS_PATH}"
print(f"Found {len(seed_files)} seed file(s): {[f.name for f in seed_files]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load each seed
# MAGIC
# MAGIC `inferSchema=True` is safe here (unlike the source-system extracts in
# MAGIC `01_data_exploration.py`) — these CSVs are authored and reviewed by us, not exported
# MAGIC from a dirty upstream system. Each frame is then cast column-by-column to the *actual*
# MAGIC `ref` table schema before writing, so the seed's types always match the DDL exactly
# MAGIC (e.g. `is_active` lands as the `BOOLEAN` the table expects, not whatever Spark guessed).

# COMMAND ----------

results = []

for path in seed_files:
    table_name = path.stem
    target = f"{CATALOG}.ref.{table_name}"

    if not spark.catalog.tableExists(target):
        print(f"SKIP  {path.name}: {target} does not exist — run 01_catalog_and_ref.sql first")
        results.append((table_name, "SKIPPED_NO_TABLE", 0))
        continue

    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)
          .csv(str(path)))

    target_schema = spark.table(target).schema
    target_cols = {f.name for f in target_schema.fields}
    source_cols = set(df.columns)

    missing_in_source = target_cols - source_cols
    extra_in_source = source_cols - target_cols
    assert not missing_in_source, (
        f"{path.name}: CSV is missing column(s) {missing_in_source} required by {target}"
    )
    if extra_in_source:
        print(f"WARN  {path.name}: dropping column(s) not in {target}: {extra_in_source}")

    # Cast to the target schema's types and column order — makes insertInto positional-safe.
    df = df.select([F.col(f.name).cast(f.dataType) for f in target_schema.fields])

    before = spark.table(target).count()
    df.write.mode("overwrite").insertInto(target)
    after = spark.table(target).count()

    print(f"LOAD  {path.name} -> {target}: {before} -> {after} rows")
    results.append((table_name, "LOADED", after))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify
# MAGIC Fails the notebook (and therefore the job task) if any seed didn't load — a silent
# MAGIC empty `ref` table is exactly the failure mode this notebook exists to prevent.

# COMMAND ----------

failed = [r for r in results if r[1] != "LOADED"]
for table_name, status, count in results:
    print(f"{table_name:24s} {status:18s} {count:>5} rows")

assert not failed, f"Seed load incomplete: {failed}"
print("\nAll reference seeds loaded.")
