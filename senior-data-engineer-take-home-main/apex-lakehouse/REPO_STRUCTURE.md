# Repository Structure

```
.
├── notebooks/
│   ├── 01_data_exploration.py      # Spark profiling — discovers & evidences 13 of 14 DQ issues
│   └── 00_setup_seeds.py           # loads src/mappings/*.csv into ref.* tables
├── src/
│   ├── ddl/
│   │   ├── 01_catalog_and_ref.sql              # UC catalog/schemas, ref crosswalks (pre-pipeline)
│   │   ├── 02a_gold_oee_daily.sql              # gold MV: OEE daily
│   │   ├── 02b_gold_equipment_reliability.sql  # gold MV: MTBF/MTTR, per calendar month
│   │   ├── 02c_gold_scrap_rate_daily.sql       # gold MV: scrap rate
│   │   ├── 02d_gold_changeover_time.sql        # gold MV: changeover time (excludes DQ-14 rows)
│   │   └── 02e_gold_first_pass_yield.sql       # gold MV: first-pass yield
│   │       # All five read pipeline-created silver tables, so each must run AFTER the
│   │       # pipeline (not with 01), and after each other in sequence, not parallel —
│   │       # see 02a's and 02b's file headers for why.
│   ├── pipelines/
│   │   └── apex_pipeline.py        # Lakeflow Declarative Pipeline (bronze → silver)
│   └── mappings/                   # Crosswalk seed data (Git-versioned, loaded to ref.*)
│       ├── equipment_xref.csv      #   4 equipment ID schemes → one surrogate key
│       ├── line_xref.csv           #   all line_id encodings → line_id INT
│       ├── alarm_severity_xref.csv #   numeric 1-4 + text severities → unified scale
│       └── wo_type_xref.csv        #   Preventive/PM synonyms → planning_category
├── docs/
│   ├── 01_architecture.md          # Connector strategy, streaming/batch, governance (Part 2 B/C/E)
│   ├── 02_data_quality.md          # Data quality assessment (Part 1a.3) — 14 findings
│   ├── 03_nl_analytics.md          # Genie / NL analytics approach (Part 1b) — incl. live Genie test
│   ├── 04_short_answers.md         # Part 3 (Q2, Q4, Q5, Q6)
│   ├── source_system_mapping.xlsx  # Which sample file comes from which source system
│   └── diagrams/
│       ├── current_state_connectivity_pro.drawio   # Plant as-is: how OT & IT connect today
│       ├── ingestion_architecture.drawio           # Part 2A: end-to-end ingestion
│       ├── data_model_logical_physical.drawio      # Logical ER + physical Delta model
│       └── dimensional_model.drawio                # Star schema
├── databricks.yml                  # Asset Bundle: dev/stg/prod, pipeline + bootstrap job (Part 2D)
├── README-SOLUTION.md              # Assumptions, scope decisions, what I'd do with more time
└── README.md                       # Assignment brief (provided, one level up from this dir)
```

## Setup notes
1. Upload the six sample extracts to a UC Volume, e.g. `/Volumes/apex_dev/bronze/landing/`
2. Set `BASE` in `notebooks/01_data_exploration.py` to that path
3. `databricks bundle deploy -t dev` (fill in `sql_warehouse_id` in `databricks.yml`'s dev
   target first — left blank on purpose; `workspace.host` resolves from `--profile`/
   `DATABRICKS_HOST`, not the file — see file comments)
4. `databricks bundle run apex_bootstrap -t dev` — runs everything in true dependency order:
   `01_catalog_and_ref.sql` (catalog, schemas, ref table shapes) → `00_setup_seeds.py`
   (loads `src/mappings/*.csv` into `ref.*`) → one `apex_pipeline` run (creates bronze/silver
   tables) → all five gold MVs in sequence (`02a` → `02b` → `02c` → `02d` → `02e`).
5. Subsequent pipeline runs (e.g. after new data lands), independent of the one-time
   bootstrap: `databricks bundle run apex_pipeline -t dev`

## Live-tested, not just written

This entire `databricks.yml` + DDL + pipeline chain was actually deployed and run end-to-end
against a real Databricks workspace (not just validated for syntax) — twice over, in two
rounds: once to get the platform itself deploying and running cleanly, once to actually ask
Genie the brief's 5 example questions against the deployed gold layer. Both rounds surfaced
real bugs a code read wouldn't have caught.

### Round 1 — deploying the platform (6 bugs)

1. **`apex_pipeline.py` schema-qualification mismatch** — pipeline published `bronze_x`/`silver_x`
   into one implicit schema; gold MVs already queried dot-qualified `catalog.silver.x`. Fixed by
   schema-qualifying every `@dlt.table` name.
2. **`${catalog}` doesn't substitute in a Jobs `sql_task.file`** — that's notebook-widget syntax.
   Fixed by using named SQL parameters (`:catalog`) bound via the task's `parameters` field, with
   `USE CATALOG IDENTIFIER(:catalog)` at the top of each file and plain `schema.table` names below
   it (a literal `IDENTIFIER(...)` in the `CREATE MATERIALIZED VIEW` target itself isn't recognized
   by the Jobs SQL-file-type detector — see point 5).
3. **`equipment_registry.json` needs `multiLine: true`** — it's a pretty-printed JSON array, not
   newline-delimited JSON; Auto Loader's schema inference failed without it.
4. **`with_line_id()` produced two same-named `line_id` columns** (the raw source one and the
   conformed `ref.line_xref` one) after every join, causing `AMBIGUOUS_REFERENCE` wherever the
   result was later selected by name. Fixed by dropping the raw column post-join.
5. **`sql_task.file` requires exactly one `CREATE MATERIALIZED VIEW`/`STREAMING TABLE` per file**
   (it routes through an implicit pipeline-backed execution path) — split into one file per MV
   (`02a`–`02e`). Running two in parallel hit a workspace-tier quota (max 1 concurrent
   DBSQL-type pipeline), so all five are sequenced, not parallel.
6. **Gold MVs assumed `start_ts_local`/`created_ts_local`/`check_ts_local` columns** that were
   never actually added to the relevant silver tables in the pipeline — only
   `silver.sensor_readings` originally had the plant-local generated-column pattern. Added it to
   `silver.production_runs`, `silver.maintenance_work_orders`, and `silver.quality_checks` for
   consistency, then ran full-refresh pipeline updates to backfill existing rows each time (a
   checkpointed Auto Loader table doesn't retroactively populate a new column on an incremental
   run).
7. **`CREATE MATERIALIZED VIEW IF NOT EXISTS` silently ignores query changes** on an MV that
   already exists — switched every gold MV to `CREATE OR REPLACE`, since for a materialized view
   the SQL file *is* the deployable artifact and a redeploy should always match Git.

### Round 2 — asking Genie the brief's 5 example questions (1 bug, 1 new DQ finding)

4 of 5 questions were **correctly declined** by Genie (no data in the requested range, or a
genuinely missing mart) — Gold-only scoping doing exactly what it's for. 1 of 5 was
**confidently wrong**: `gold.equipment_reliability` had no time dimension, so "unplanned stops
this month" silently returned the all-time cumulative total. Fixing that (adding a `stop_month`
grain) required building `gold.changeover_time` as a comparison point, which surfaced **DQ-14**:
Line 3's `production_runs` has physically-impossible overlapping time windows — invisible from
row-by-row profiling, only visible once a derived cross-run metric was actually computed. Both
fixed; full account in `docs/03_nl_analytics.md` §2a and `docs/02_data_quality.md` DQ-14.

Full account of both rounds, including dead ends tried and abandoned (a notebook-based DDL
runner that hung on serverless network egress), in `databricks.yml`'s and the `src/ddl/*.sql`
files' own comments, and summarized in `README-SOLUTION.md`.

## Built
- `databricks.yml` — Asset Bundle: dev/stg/prod targets, `apex_pipeline` (Lakeflow Declarative
  Pipeline, serverless), `apex_bootstrap` job (schemas/ref DDL → seeds → one pipeline run → 5
  gold MVs, sequenced; classic job cluster for staging/prod's seed-load task, serverless default
  for dev) → Part 2D
- `notebooks/00_setup_seeds.py` — generic loader: any `src/mappings/<name>.csv` → `ref.<name>`
- `src/pipelines/apex_pipeline.py` — schema-qualified table names; `silver.sensor_readings`'s
  Liquid Clustering + full `TBLPROPERTIES` live in the `@dlt.table` decorator (the only place
  they're actually enforced); `equipment_registry` bronze source reads with `multiLine: true`;
  `with_line_id()` no longer produces an ambiguous duplicate column; plant-local `_local` columns
  added consistently across `production_runs`, `maintenance_work_orders`, `quality_checks`.
- `src/ddl/02a`–`02e` — five gold KPI marts, `USE CATALOG`-parameterized, `CREATE OR REPLACE`,
  sequenced after the pipeline and after each other. Covers 6 of the plant's 7 KPIs — CIP Cycle
  Time has no source signal anywhere in the sample extracts (checked directly), so no mart for it.
