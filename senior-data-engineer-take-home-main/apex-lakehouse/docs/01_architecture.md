# Architecture — Connector Strategy, Streaming/Batch, Governance (Part 2 B/C/E)

Diagram: `docs/diagrams/ingestion_architecture.drawio` (Part 2A). This doc is the written
counterpart — per-source connector choice, streaming/batch justification, and governance —
plus the reasoning the diagram can't carry.

## B/C. Connector strategy and streaming vs. batch, per source

| Source | Extraction | Landing format | Databricks ingestion | Mode | Why |
|---|---|---|---|---|---|
| **Line 3 vibration (MQTT)** | Eclipse Mosquitto broker → a lightweight MQTT-to-Kafka bridge (e.g. a small subscriber process publishing to a Kafka/Kafka-compatible topic — Databricks has no native MQTT connector) | Kafka topic, JSON payload matching the sensor schema (`sensor_id`, `timestamp`, `x/y/z_accel`, `temperature`) | Structured Streaming reading Kafka directly (`readStream.format("kafka")`) in a **continuous, non-serverless** Workflows job, not the Lakeflow pipeline described above | **Streaming** | 1,000 msg/s with a sub-minute anomaly-detection SLA rules out anything batch or micro-batch on a >1 min trigger. This is also *why it's a separate job from the bronze/silver Lakeflow pipeline* in `databricks.yml`: bundling it in would force SAP/LIMS batch sources onto the same always-on compute as a 1 kHz feed, and DLT's serverless billing model isn't the right fit for a single always-on stateful stream doing anomaly scoring — a purpose-built Structured Streaming job on classic (or a dedicated small serverless streaming compute, if/when GA for this shape) is more predictable to cost and tune. MQTT itself is explicitly **not** a Databricks-native source; the bridge is a real piece of infrastructure to build, not a config toggle — called out because it's easy to undersell in a diagram. |
| **PI historian (2,000 tags, 1s scan)** | PI Web API (REST) or PI-to-Kafka connector if the plant licenses OSIsoft's own Kafka integration; absent that, a scheduled extractor hitting PI Web API's batch/stream endpoints, landing files to a staging area Auto Loader watches | Parquet or CSV per extraction window in a UC Volume, e.g. `/Volumes/apex_{env}/bronze/landing/pi_historian/` | **Auto Loader** (`cloudFiles`) in the Lakeflow pipeline, incremental file discovery | **Micro-batch**, ~1-2 min trigger interval | 5-minute landing SLA has slack a true streaming source doesn't need to spend on; Auto Loader's incremental file listing is cheap and idempotent, and this avoids operating a second always-on Kafka-consuming job for a source that isn't natively event-streamed to begin with (PI is poll/export-based, not a message bus). Databricks has no native PI connector — this extraction step happens outside Databricks (PI Web API client) and Databricks resumes ownership once files land. **Note:** this source is the one with the documented 2-6h late-arrival problem — see `docs/04_short_answers.md` Q5 for the watermark/backfill design. |
| **SAP maintenance work orders / production schedules (PP)** | **Lakeflow Connect** for SAP (managed CDC connector, if the plant's SAP edition and hosting are on Lakeflow Connect's supported list — I'm flagging this as unconfirmed, see caveat below), extracting from BW/4HANA Open Hub or CDS views per the plant context doc | Managed Delta table, written directly by the connector | Lakeflow Connect ingestion pipeline (declarative, managed) | **Batch**, matching SAP's own update cadence (30 min - 4h per the brief) | SAP data changes in batches on the source side (Open Hub extraction runs, not a live feed) — matching that cadence avoids re-querying SAP more often than it actually produces new data. **Caveat, stated plainly since the brief says "don't invent features": I have not personally verified current Lakeflow Connect SAP source support against Apex's specific SAP hosting (Azure) and module set (PM/PP) as of this writing.** If Lakeflow Connect doesn't cover this combination, the fallback is Fivetran/an equivalent partner connector (available directly in Databricks Partner Connect) landing into the same bronze Volume/table, or a scheduled SAP OData/BAPI extract via a Workflows job — same batch cadence, different extraction tool. |
| **PostgreSQL production DB** (`production_runs`, `shift_schedule`, `operator_assignments`) | **Lakeflow Connect** for PostgreSQL (CDC via logical replication) — this one I'm more confident is supported, as Postgres is one of Lakeflow Connect's flagship managed sources | Managed Delta table, CDC applied automatically | Lakeflow Connect | **Near-real-time / micro-batch** (CDC polling, not sub-second) | Production run status changes matter operationally sooner than SAP maintenance data does (a plant manager asking "what's running right now" wants current state), but there's no sub-minute SLA stated for this source — CDC on a short poll interval (minutes) balances freshness against not hammering the transactional DB that shift-management UIs are also hitting live. A raw JDBC batch pull would work too but throws away the natural CDC granularity (and the REST API mentioned in the plant doc is a viable alternative extraction path if CDC access isn't grantable). |
| **LabWare LIMS quality results** | CSV export to shared network drive (already the plant's own export mechanism) → a lightweight agent (or cloud storage sync — e.g. Azure File Sync / a scheduled script) pushes the export to a UC Volume | CSV in `/Volumes/apex_{env}/bronze/landing/quality_checks/` | Auto Loader | **Batch**, matching the 30-minute export cadence | The source itself only produces new data every 30 minutes — there's nothing to stream. This is the simplest connector in the whole architecture and deliberately so: don't build streaming infrastructure for a source that's fundamentally a periodic file drop. |

**General principle applied throughout:** match ingestion mode to the source's actual data
production cadence, not to a uniform "everything streams" default. Three of five sources here
are batch or micro-batch; only the genuinely high-frequency, low-latency-SLA source (Line 3
MQTT) gets true streaming. Over-streaming a 30-minute-cadence LIMS export buys nothing and
adds an always-on job to monitor.

## D. Deployment & CI/CD

Covered concretely in `databricks.yml` (Part 2D deliverable) — summary of the reasoning:

- **Databricks Asset Bundles** define everything as code: catalog/schema DDL execution, the
  Lakeflow pipeline, the seed-loader job, cluster specs. One `databricks.yml`, three targets
  (`dev`/`staging`/`prod`), each pointing at its own catalog (`apex_dev`/`apex_stg`/`apex_prod`)
  and (in a real deployment) its own workspace.
- **`mode: development`** on the `dev` target gets DAB's built-in behavior of prefixing
  resource names with the deploying engineer's identity and pausing schedules — multiple
  engineers can `bundle deploy -t dev` without colliding on resource names.
- **CI/CD shape** (not yet wired up as actual pipeline YAML in this repo — see
  README-SOLUTION.md): a GitHub Actions/Azure DevOps workflow running `databricks bundle
  validate` on every PR, `databricks bundle deploy -t staging` + a smoke-test job run on merge
  to main, and a manually-gated `deploy -t prod` (or auto-deploy on a release tag) run as the
  prod service principal, not a human identity — `databricks.yml`'s `prod` target has a
  `run_as` block ready but intentionally left unfilled since no service principal exists for
  this take-home.
- **Why Bundles over manually clicking through three workspaces:** the same crosswalk-seed
  bug or DLT expectation gets fixed once, in Git, and rolls forward through dev → staging →
  prod identically. The alternative (hand-configuring three DLT pipelines in the UI) is exactly
  the kind of silent environment drift that turns into "it worked in dev" incidents.

## E. Governance

### Catalog/schema naming
`apex_{env}.{layer}` — catalog is the environment boundary, schema is the medallion layer
(`bronze`/`silver`/`gold`/`ref`), stable across environments. This means a permission grant
written once (`GRANT ... ON SCHEMA apex_prod.gold ...`) has an identical shape in every
environment — only the catalog name changes, which is exactly what UC's catalog-scoped grants
are designed around.

### Three personas, three grant patterns

| Persona | Access | UC mechanism |
|---|---|---|
| **Data engineers** | Full read/write on all schemas in `apex_dev`; read/write on `bronze`/`silver` and DDL rights in `apex_stg`/`apex_prod` via a `data-engineers` group, not individual grants | `GRANT ALL PRIVILEGES ON CATALOG apex_dev TO \`data-engineers\`;` per catalog, scoped down in staging/prod to exclude direct `gold` writes (gold is pipeline/DDL-managed, not hand-edited) |
| **Data analysts** | Read-only on `silver` and `gold` in `apex_stg`/`apex_prod`. No `bronze` access (raw layer has no conformance applied yet — an analyst querying bronze directly is how "the report doesn't match the dashboard" incidents start) and no `ref` write access (crosswalks are Git/DAB-owned) | `GRANT SELECT ON SCHEMA apex_prod.silver TO \`data-analysts\`; GRANT SELECT ON SCHEMA apex_prod.gold TO \`data-analysts\`;` |
| **Plant managers** | No direct SQL/table access at all. All access is through Genie, scoped to `gold` only, filtered by **row-level security on `line_id`** | See below |

### Row-level security by line

UC **row filters** on the Gold tables/views, keyed off group membership:

```sql
CREATE FUNCTION apex_prod.gold.line_access_filter(line_id INT)
RETURNS BOOLEAN
RETURN
  is_account_group_member('plant-managers-all')          -- corporate/multi-line role sees everything
  OR EXISTS (
    SELECT 1 FROM apex_prod.ref.line_manager_assignment    -- ref table: which manager sees which line(s)
    WHERE manager_group = current_user()
      AND line_manager_assignment.line_id = line_access_filter.line_id
  );

ALTER TABLE apex_prod.gold.oee_daily
  SET ROW FILTER apex_prod.gold.line_access_filter ON (line_id);
```

(`ref.line_manager_assignment` is a small seed I'd add alongside the existing crosswalks —
same load mechanism as `00_setup_seeds.py`, not yet built.) The row filter applies to *every*
query against the table, including ones Genie generates — a plant manager cannot get Line 1
data out of Genie by phrasing a clever question, because the filter is enforced at the SQL
engine, not in the NL layer. This is the concrete reason Genie access is scoped to Gold and
nowhere else: row filters are only defined on the Gold tables. If Genie could read Silver, the
line scoping would need to be re-implemented (and re-verified) there too.

Plant managers get **no direct SQL warehouse or notebook access** — enforced by simply not
granting `USE CATALOG`/workspace access outside the Genie space, not by relying on the NL
interface as the sole control.

### What I'd verify with more time
Whether `ref.line_manager_assignment`-style row filters compose cleanly with Genie's own
generated SQL in every case (joins across multiple Gold tables in one Genie answer need the
filter to apply post-join, not just on the first table touched) — I'd want to see this tested
against a real multi-table Genie question before trusting it in front of a plant manager.
