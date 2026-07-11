# Solution Notes — Assumptions, Scope, What's Next

## How to read this repo

Start with `docs/02_data_quality.md` (findings, evidenced in `notebooks/01_data_exploration.py`),
then `src/ddl/01_catalog_and_ref.sql` and `src/pipelines/apex_pipeline.py` for the
architecture those findings drove. `docs/01_architecture.md`, `docs/03_nl_analytics.md`, and
`docs/04_short_answers.md` cover Part 2 and Part 3 of the brief. `databricks.yml` +
`notebooks/00_setup_seeds.py` are the deployment mechanics (Part 2D). `REPO_STRUCTURE.md`
has the file-by-file map and setup/run order.

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
- **Gold marts built: `oee_daily`, `equipment_reliability`.** Three of the five example
  questions in the brief (critical work orders, quality-parameter correlation, changeover time)
  don't have a purpose-built mart yet — building those, plus `scrap_by_line` and
  `alarm_summary`, is the top item in "what's next" below, not something I considered
  out-of-scope.
- **Fixed one real inconsistency in already-drafted work rather than leaving it**:
  `src/pipelines/apex_pipeline.py` originally published tables as `bronze_x`/`silver_x` in one
  implicit schema; the gold materialized views in `01_catalog_and_ref.sql` already query
  `${catalog}.silver.x` (dot-qualified). Renamed the pipeline's table targets to be
  schema-qualified (`bronze.x`/`silver.x`, a real Lakeflow multi-schema-publish mechanism) so
  the two already-committed pieces actually agree, instead of documenting a bug I could have
  fixed in three lines.
- **`databricks.yml` ships with real placeholders, not fabricated values**: `sql_warehouse_id`
  and workspace `host` per target are intentionally blank with a comment explaining how to
  fill them, rather than inventing plausible-looking fake IDs that would silently fail on
  `bundle deploy`. Same for the prod `run_as` service principal — omitted rather than pointed
  at an empty variable.
- **Answered 4 of 6 Part 3 questions** (2, 4, 5, 6) — chosen because each ties directly to a
  decision made elsewhere in the repo (physical design, semantic layer, streaming design,
  governance), so the answer demonstrates reasoning already exercised rather than restating
  generic Databricks knowledge cold. Q1 (Pipelines vs. Workflows) and Q3 (serverless
  trade-offs) are the two I'd answer next if asked live — both come up naturally in the
  15-20 minute follow-up discussion.

## What I'd do with more time

Roughly in priority order:

1. **Build the missing Gold marts** (`scrap_by_line`, `alarm_summary`,
   `quality_first_pass_yield`, `changeover_log`) so Genie can actually answer all five example
   questions from the brief, not just the two OEE/reliability ones.
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
