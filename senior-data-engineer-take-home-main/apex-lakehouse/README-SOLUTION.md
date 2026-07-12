# Solution Notes — Assumptions, Scope, What's Next

## How to read this repo

Start with `docs/02_data_quality.md` (findings, evidenced in `notebooks/01_data_exploration.py`),
then `src/ddl/01_catalog_and_ref.sql` and `src/pipelines/apex_pipeline.py` for the
architecture those findings drove. `docs/01_architecture.md`, `docs/03_nl_analytics.md`, and
`docs/04_short_answers.md` cover Part 2 and Part 3 of the brief. `databricks.yml` +
`notebooks/00_setup_seeds.py` are the deployment mechanics (Part 2D). `REPO_STRUCTURE.md`
has the file-by-file map and setup/run order.

## This was actually deployed and run, not just written

`databricks.yml` and the full DDL/pipeline chain were deployed to a real Databricks workspace
and run end-to-end — catalog/schema/ref DDL, seed load, a full Lakeflow pipeline run against
the six sample extracts, and **five gold materialized views** (`oee_daily`,
`equipment_reliability`, `scrap_rate_daily`, `changeover_time`, `first_pass_yield`) — with real
row counts and real KPI numbers coming out the other end. That surfaced real bugs a code read
wouldn't have caught, in two rounds:

**Round 1 — deploying the platform:** a schema-qualification mismatch between the pipeline and
the gold DDL; a `${catalog}`-substitution assumption that's actually notebook-widget-only
syntax; a missing `multiLine` option on a pretty-printed JSON source; an ambiguous duplicate
`line_id` column after a ref-table join; a Jobs `sql_task.file` constraint on
materialized-view-creating files (exactly one per file, plus a workspace-tier quota on how many
can run concurrently); and two gold-MV columns that were referenced but never actually produced
by the pipeline. Full account, including a notebook-based DDL-runner approach that was tried
and abandoned after hanging on serverless network egress, in `REPO_STRUCTURE.md`'s
"Live-tested, not just written" section and in `databricks.yml` / `src/ddl/02a_gold_oee_daily.sql`.

**Round 2 — actually asking Genie the brief's 5 example questions against the deployed gold
layer:** 4 of 5 correctly declined (no data in range, or genuinely missing marts — Gold-only
scoping doing exactly what it's for). 1 of 5 was **confidently wrong**: `equipment_reliability`
had no time dimension, so "unplanned stops this month" silently returned the all-time total.
Fixed by adding a `stop_month` grain — which also surfaced **DQ-14**, a new data quality
finding (Line 3 has physically-impossible overlapping production-run time windows) invisible
until a derived cross-run metric (`changeover_time`) was actually built and queried. Full
account in `docs/03_nl_analytics.md` §2a and `docs/02_data_quality.md` DQ-14.

## Explicit assumptions (repeated here because getting any one wrong is a correctness bug, not a style choice)

1. **SAP asset `A-xxx` ↔ equipment registry ID by matching numeric suffix** (`A-101` ↔
   `EQ-101`). Verified consistent for all 15 assets present in the sample `maintenance_logs`,
   but that's a small, possibly non-representative sample. In production I would confirm
   against the SAP PM equipment master (`EQUI` table) directly rather than inferring from
   suffix pattern-matching — a coincidental suffix collision would misattribute a work order
   to the wrong physical equipment. (`docs/02_data_quality.md` DQ-01)

2. **Alarm severity numeric coding is FactoryTalk convention: `1 = CRITICAL` … `4 = LOW`**
   (lower number = more severe). The sample data gives no way to derive this from values
   alone — it's read off FactoryTalk's documented convention, not verified against Apex's
   actual SCADA config. **Getting this backwards inverts every "critical alarm" answer** the
   VP's use cases care about. Flagged for controls-engineering sign-off before this goes near
   a real Genie space. (`docs/02_data_quality.md` DQ-09)

3. **Naked (no-`Z`) timestamps are plant-local, `America/Chicago`** (Austin, TX); `Z`-suffixed
   timestamps are UTC. Both timestamp families appear in the same `sensor_readings` file, so
   this is a per-value inference, not a per-file one. A 5-6 hour timezone error here would
   corrupt every shift-level KPI (shift boundaries, OEE-by-shift) silently rather than
   loudly — this is the assumption I'd move fastest to confirm with the historian team in a
   real engagement. (`docs/02_data_quality.md` DQ-04)

4. **Volume figures in `production_runs` are in thousands of litres (kL), mislabeled as
   litres**, derived by cross-checking `actual_volume` against `total_units × SKU size`. This
   is a *scale* inference from two other fields, not a spec I was handed — worth a source-system
   confirmation, since the correction is a 1000× multiplier and getting it wrong in either
   direction is not visually obvious in a dashboard. (`docs/02_data_quality.md` DQ-11)

5. **Lakeflow Connect covers SAP (Open Hub/CDS) and PostgreSQL as ingestion sources** for this
   architecture. I'm confident about PostgreSQL (a flagship Lakeflow Connect source); I am
   **not** independently verifying SAP support against Apex's specific hosting/module
   combination as of this writeup, and I said so explicitly in `docs/01_architecture.md`
   rather than presenting it as confirmed. A partner connector (Fivetran et al.) or a
   hand-rolled batch extractor are the fallback if it isn't covered.

6. **Lakebase** is named in `docs/04_short_answers.md` Q6 as a directionally-relevant newer
   capability for the write-back problem, with an explicit caveat that I have not verified its
   current maturity/GA scope. Named because it's the right kind of thing to spike on, not
   because I'm certain it's production-ready for this exact pattern today.

## Scope decisions

- **Genie is scoped to `gold` only**, never `silver`/`bronze` — a governance and semantic-safety
  choice, not a limitation I ran out of time to fix. Reasoning in `docs/03_nl_analytics.md` §1
  and `docs/01_architecture.md` §E.
- **Gold marts built: `oee_daily`, `equipment_reliability`, `scrap_rate_daily`,
  `changeover_time`, `first_pass_yield`** — 6 of the plant's 7 KPIs (CIP Cycle Time has no
  source signal in the sample data at all). Two of the five example questions in the brief
  (critical work orders, quality-parameter correlation) still don't have a purpose-built
  mart, and one (fill-weight trend by SKU) is blocked on a genuine source-data-model gap, not
  a missing mart — all three are the top items in "what's next" below, not something I
  considered out-of-scope.
- **Fixed one real inconsistency in already-drafted work rather than leaving it**:
  `src/pipelines/apex_pipeline.py` originally published tables as `bronze_x`/`silver_x` in one
  implicit schema; the gold materialized views already queried `${catalog}.silver.x`
  (dot-qualified). Renamed the pipeline's table targets to be schema-qualified
  (`bronze.x`/`silver.x`, a real Lakeflow multi-schema-publish mechanism) so the two
  already-committed pieces actually agree, instead of documenting a bug I could have fixed in
  three lines.
- **`databricks.yml` ships with real placeholders, not fabricated values**: `sql_warehouse_id`
  is intentionally blank with a comment explaining how to fill it, rather than inventing a
  plausible-looking fake ID that would silently fail on `bundle deploy`. `workspace.host` is
  deliberately absent from every target (resolved via `--profile`/`DATABRICKS_HOST` instead —
  see databricks.yml's Targets comment for why: host resolves before variable substitution
  runs, so it can't be a bundle variable either, confirmed by actually hitting that failure
  mode). Same reasoning for the prod `run_as` service principal — omitted rather than pointed
  at an empty variable.
- **Split `src/ddl/01_catalog_and_ref.sql` into three files after actually deploying and
  running the bundle against a real workspace.** The gold MVs read `silver.production_runs`
  and `silver.maintenance_work_orders` — tables the *pipeline* creates, not this DDL script.
  Running them as part of one pre-pipeline DDL script (my original design) fails with
  `TABLE_OR_VIEW_NOT_FOUND` the first time it's ever run, because the pipeline hasn't created
  those tables yet. Gold MVs now live in `src/ddl/02a_gold_oee_daily.sql` and
  `src/ddl/02b_gold_equipment_reliability.sql` — two files, not one, because Databricks Jobs'
  `sql_task.file` requires exactly one `CREATE MATERIALIZED VIEW` per file (also found live).
  `apex_bootstrap` in `databricks.yml` sequences all of this in true dependency order
  (schemas/ref → seeds → one pipeline run → OEE mart → reliability mart). This is exactly the
  kind of ordering bug that's invisible from reading the SQL in isolation and only surfaces by
  actually running it — which is why I did.
- **`silver.sensor_readings`'s Liquid Clustering + `TBLPROPERTIES`** were originally declared
  twice: once (inert) as a `CREATE TABLE` in the DDL, and once in the pipeline's `@dlt.table`
  decorator (missing several properties the DDL had). Since DLT owns and creates this table,
  the DDL's copy never actually executes against it — it was documentation that looked like
  code. Consolidated everything into the `@dlt.table` decorator, the one place these
  properties are real.
- **Answered 4 of 6 Part 3 questions** (2, 4, 5, 6) — chosen because each ties directly to a
  decision made elsewhere in the repo (physical design, semantic layer, streaming design,
  governance), so the answer demonstrates reasoning already exercised rather than restating
  generic Databricks knowledge cold. Q1 (Pipelines vs. Workflows) and Q3 (serverless
  trade-offs) are the two I'd answer next if asked live — both come up naturally in the
  15-20 minute follow-up discussion.

## What I'd do with more time

Roughly in priority order:

1. **`gold.critical_work_orders`** (technician/priority from `silver.maintenance_work_orders`,
   Silver-layer fix already done) and **a quality-parameter-vs-scrap-rate correlation mart** —
   the two remaining pieces of the brief's 5 example questions. A **batch-to-run mapping**
   (`quality_checks.batch_id` → `production_runs.product_sku`) is the third gap but is a
   source-data-model problem, not a missing mart — see `docs/03_nl_analytics.md` §5 for why I
   wouldn't want to infer it. `gold.alarm_summary` also still missing.
2. **`ref.product_sku` seed CSV** — the DDL already defines the table (used to validate the
   volume-scale fix, DQ-11) but no seed file exists yet; `00_setup_seeds.py` will pick it up
   automatically once added (that's why it's written generically rather than with a hardcoded
   file list).
3. **`ref.line_manager_assignment` seed** — needed to make the row-level-security function in
   `docs/01_architecture.md` §E actually runnable, currently sketched but not backed by data.
4. **Verify the RLS row filter composes correctly with Genie-generated multi-table joins** —
   flagged as an open question in `docs/01_architecture.md`, not something I'd assume works
   from the SQL syntax alone.
5. **Actual CI/CD pipeline YAML** (GitHub Actions or Azure DevOps) implementing the
   validate → deploy-staging → gated-deploy-prod flow described in `docs/01_architecture.md`
   §D — the *design* is written, the workflow file itself isn't.
6. **MQTT-to-Kafka bridge** for Line 3 — called out in `docs/01_architecture.md` as real
   infrastructure to build, not a Databricks config toggle; not started.
7. **Silver-layer completeness checks** for late-arriving PI data (expected-tag-count-per-bucket
   vs. actual) and an ingest-lag monitor — both named as gaps in `docs/04_short_answers.md` Q5.
8. **A Genie regression benchmark** — ~20 pinned NL questions with known-correct answers, run
   on a schedule, to catch semantic drift before a plant manager does. Described but not built
   in `docs/03_nl_analytics.md` §2.
9. **Validate every domain assumption in the list above with an actual Apex stakeholder**
   (controls engineering for alarm severity direction, historian team for timezone, SAP team
   for the asset-number mapping) — the single highest-leverage thing I'd do with real access
   to the plant, versus continuing to build on inferred-from-sample-data assumptions.
