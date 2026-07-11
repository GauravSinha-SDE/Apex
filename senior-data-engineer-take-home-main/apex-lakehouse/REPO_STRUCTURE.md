# Repository Structure

```
.
├── notebooks/
│   └── 01_data_exploration.py      # Spark profiling — discovers & evidences all 13 DQ issues
├── src/
│   ├── ddl/
│   │   └── 01_catalog_and_ref.sql  # UC catalog/schemas, ref crosswalks, gold KPI MVs
│   ├── pipelines/
│   │   └── apex_pipeline.py        # Lakeflow Declarative Pipeline (bronze → silver)
│   └── mappings/                   # Crosswalk seed data (Git-versioned, loaded to ref.*)
│       ├── equipment_xref.csv      #   4 equipment ID schemes → one surrogate key
│       ├── line_xref.csv           #   all line_id encodings → line_id INT
│       ├── alarm_severity_xref.csv #   numeric 1-4 + text severities → unified scale
│       └── wo_type_xref.csv        #   Preventive/PM synonyms → planning_category
├── docs/
│   ├── 02_data_quality.md          # Data quality assessment (Part 1a.3)
│   ├── source_system_mapping.xlsx  # Which sample file comes from which source system
│   └── diagrams/
│       ├── current_state_connectivity_pro.drawio   # Plant as-is: how OT & IT connect today
│       ├── ingestion_architecture.drawio           # Part 2A: end-to-end ingestion
│       ├── data_model_logical_physical.drawio      # Logical ER + physical Delta model
│       └── dimensional_model.drawio                # Star schema
└── README.md                       # Assignment brief (provided)
```

## Still to build
- `docs/03_nl_analytics.md` — Genie / NL analytics approach (Part 1b)
- `docs/01_architecture.md` — connector strategy, streaming vs batch, governance (Part 2 B/C/E)
- `docs/04_short_answers.md` — Part 3 (answering Q2, Q4, Q5, Q6)
- `README-SOLUTION.md` — assumptions, scope decisions, what I'd do with more time

## Setup notes
1. Upload the six sample extracts to a UC Volume, e.g. `/Volumes/apex_dev/bronze/landing/`
2. Set `BASE` in `notebooks/01_data_exploration.py` to that path
3. `databricks bundle deploy -t dev` (fill in `sql_warehouse_id` / workspace `host` in
   `databricks.yml` first — left blank on purpose, see file comments)
4. `databricks bundle run apex_bootstrap -t dev` — runs `01_catalog_and_ref.sql` (catalog,
   schemas, ref table shapes, gold MVs), then `00_setup_seeds.py` (loads `src/mappings/*.csv`
   into `ref.*`). Silver joins in the pipeline depend on this having run first.
5. `databricks bundle run apex_pipeline -t dev`

## Built
- `databricks.yml` — Asset Bundle: dev/stg/prod targets, `apex_pipeline` (Lakeflow
  Declarative Pipeline, serverless), `apex_bootstrap` job (DDL + seed load, classic job
  cluster) → Part 2D
- `notebooks/00_setup_seeds.py` — generic loader: any `src/mappings/<name>.csv` → `ref.<name>`
- `src/pipelines/apex_pipeline.py` — table names are now schema-qualified (`bronze.x` /
  `silver.x`) instead of underscore-prefixed, so pipeline output lines up with the
  `${catalog}.silver.*` references already in the gold MVs (`01_catalog_and_ref.sql`)
