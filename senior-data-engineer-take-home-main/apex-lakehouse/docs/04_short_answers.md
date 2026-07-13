# Part 3: Short Answers

Answering Q2, Q4, Q5, Q6 — chosen because they map directly to decisions already made
elsewhere in this repo (physical design, semantic layer, streaming design, governance) 

## Q2. Storage Optimization: migrating `sensor_readings` off `date`-partition + `ZORDER`

At ~170M rows/day, daily partitioning already means small files are guaranteed on any day
with uneven ingestion (Line 3's 1,000 msg/s vs. the PI historian's batch drops don't arrive
evenly), and `ZORDER` on `(equipment_id, sensor_tag)` has to fully rewrite the *entire*
partition's data on every `OPTIMIZE` run — expensive and needs manual scheduling to stay
effective as data keeps landing.

**Migration plan:**
1. Create the new table with `CLUSTER BY (equipment_key, sensor_tag, reading_ts)` (already
   the target shape declared on `silver.sensor_readings` in
   `src/pipelines/apex_pipeline.py`'s `@dlt.table` decorator) — Liquid Clustering, no
   `PARTITIONED BY`. Cluster columns match the dominant predicate shape (queries filter by
   equipment/tag and a time range), same rationale ZORDER was originally chosen for, but
   Liquid Clustering incrementally reclusters new data instead of requiring full-partition
   rewrites.
2. Backfill via `CREATE TABLE ... AS SELECT` (or `INSERT INTO` in date-ordered batches for a
   170M-row/day table, to bound the size of any single write/shuffle) from the old table into
   the new one, then run `OPTIMIZE` once to cluster the backfilled history.
3. Cut the Lakeflow pipeline's target over to the new table; keep the old one, untouched, for
   a defined retention window (I'd propose 30 days) as a rollback path.
4. Turn on `delta.enableDeletionVectors` and predictive optimization (auto-scheduled
   `OPTIMIZE`/`VACUUM`, no manual job) on the new table — both already reflected in the DDL's
   `TBLPROPERTIES`.
5. Drop the old table only after the retention window passes with no rollback needed.

**Risks:**
- **Query pattern mismatch.** Liquid Clustering optimizes for the columns declared at table
  creation; if the actual dominant query pattern turns out to be pure time-range scans without
  an equipment/tag filter, `reading_ts` should be first in the cluster key, not last. I'd
  validate against real query logs (`system.query.history`) before committing to the column
  order, not just intuition from the sample data.
- **Cost during the parallel-write window.** Running both old and new tables' `OPTIMIZE` /
  ingestion simultaneously during the backfill-and-validate period roughly doubles write
  amplification for that window. Worth explicitly budgeting for, not discovering on the bill.
- **No `ZORDER`-on-Liquid-Clustered-table fallback.** Once a table is Liquid Clustered you
  can't ALTER back to a `ZORDER` workflow — this is a one-way migration per table, so step 3
  (keeping the old table as rollback) is load-bearing, not optional.
- **Downstream dependents.** Anything hardcoded to the old table name (ad hoc analyst
  queries, external BI tool connections) breaks silently unless the cutover uses a view alias
  or the migration is communicated ahead of the cutover date.

## Q4. Semantic Disambiguation: "What's the status of Line 2?"

This question is deliberately underspecified by the user — it doesn't map to one column, it
maps to *four independent* Gold concepts (`equipment_operational_status`,
`run_status`, `work_order_status`, `alarm_state`), and picking the wrong one gives a
confidently wrong answer, not an error.

**Data engineering side** (already built, see `docs/02_data_quality.md` DQ-13 and
`docs/03_nl_analytics.md` §3): rename every source `status` column to a self-describing name
at the Silver boundary, so **no Gold column is ever literally named `status`**. This is the
foundation — it makes the ambiguity structurally visible instead of letting one column quietly
answer for all four meanings.

**Query interface side** (Genie instructions, `docs/03_nl_analytics.md` §4): the space-level
instruction explicitly tells Genie that a bare "status" question is ambiguous and should
trigger a **clarifying follow-up** unless context disambiguates it (e.g. "did the alarm on
Line 2 clear?" implies `alarm_state`, no follow-up needed). This is a judgment call I want to
flag rather than gloss over: an alternative design is to answer with *all four* at once
("here's operational/run/WO/alarm status for Line 2") rather than asking a follow-up question.
I chose "ask" over "dump everything" because plant managers asking in the middle of a shift
want one fast answer, not a four-part report to parse — but this is exactly the kind of
choice I'd validate with 2-3 real plant managers before locking in, not something to decide
from a take-home's context doc alone.

**Guardrail underneath both:** the disambiguation logic living in a prompt/instruction is a
soft control — LLMs don't reliably follow instructions 100% of the time. The hard control is
that there is no `status` column for Genie to *accidentally* select even if it ignores the
instruction; at worst it picks the wrong one of four named, meaningful columns, which is a
recoverable, debuggable failure mode, not a silent data-quality issue.

## Q5. Late-Arriving Data: PI historian, 2-6h delayed

This directly shapes the Silver `sensor_readings` transformation already in
`src/pipelines/apex_pipeline.py`:

```python
df = (df.withWatermark("reading_ts", "6 hours")
        .dropDuplicates(["reading_ts", "equipment_key", "sensor_tag"]))
```

**Why a watermark, and why 6 hours specifically:** Structured Streaming needs a watermark to
bound how much state it keeps for stateful operations (here, dedup) — without one, dedup state
grows unbounded forever. The watermark threshold has to be *at least* as large as the latest
documented delay (6h) or legitimately late-but-valid data gets treated as "too late" and
dropped from dedup consideration, which for a *dedup* operation specifically means a
late-arriving duplicate might not get caught, not that the row itself is rejected — dedup here
is a "best effort within the window" operation, not a correctness guarantee for arbitrarily
late replays.

**What else the late-arrival pattern means for pipeline design, beyond the watermark:**
- **No append-only assumption downstream.** Gold aggregates (`gold.oee_daily`, etc.) that read
  from Silver must tolerate a day's numbers changing hours after that day "closed" — a naive
  materialized view that computes once and never revisits a date range would report stale
  numbers for up to 6 hours after an outage-affected window. Databricks **Materialized Views**
  (used for Gold in this design) handle this correctly by design — they incrementally
  recompute against Silver changes, including late-arriving upserts into the time range they
  cover — which is precisely why Gold is built as MVs here rather than one-shot batch INSERTs.
- **Late data isn't the same as bad data.** The watermark/dedup logic only defends against
  *duplicate* replays from store-and-forward recovery. It doesn't validate that a 5-hour-late
  batch is internally consistent (e.g. no gaps) — I'd add a Silver-layer completeness check
  (expected tag count per time bucket vs. actual) as a follow-up DLT expectation, flagged as
  not yet built.
- **Alerting on the outage itself**, not just tolerating its data: a monitor on ingest lag
  (`current_timestamp() - max(reading_ts)` per source) would catch a PI network outage in
  progress rather than only handling its aftermath gracefully. Not built here — noted as a gap.

## Q6. Operational Write-Back: alarm acknowledgment + shift notes

A lakehouse (Delta tables, even with deletion vectors) is not built for the access pattern an
operator dashboard needs: sub-second single-row read-after-write consistency under concurrent
writers, which is an OLTP workload, not an OLAP one. Forcing it onto Delta directly — e.g.
`MERGE` on every alarm-ack click — works at small scale but is the wrong tool: write
amplification per small transaction, and Delta's optimistic concurrency degrades under
frequent concurrent single-row writers competing for the same table.

**Architecture: separate the transactional write path from the analytical read path, sync
between them.**

- **Write-back store:** a small operational database purpose-built for this — Postgres (the
  plant already operates one; extending it, or standing up a lightweight equivalent, is more
  proven than routing this through the lakehouse) holding `alarm_acknowledgments` and
  `shift_notes` tables, written directly by the operator dashboard's backend.
- **Sync into the lakehouse:** the same **Lakeflow Connect CDC** pattern already used for the
  production Postgres source (`docs/01_architecture.md` §B/C) picks up these writes into
  `bronze`/`silver` on a short interval, so `silver.alarms` can be joined against
  acknowledgment history for historical reporting and Genie questions ("which alarms are
  currently unacknowledged").
- **Dashboard reads:** the live operator view reads directly from the operational store (or a
  cache in front of it), *not* from the lakehouse — the lakehouse is for analysis and history,
  not for rendering the live "is this alarm acked yet" state an operator is staring at in real
  time. This keeps the two systems doing what each is actually good at instead of stretching
  one to cover both.

**Newer Databricks capability worth flagging, with an honest confidence caveat:** Databricks
has been moving in the direction of **Lakebase** (a managed operational Postgres offering
positioned for exactly this kind of low-latency transactional workload alongside a lakehouse)
and **Delta Lake's row-level concurrency / deletion vectors** improvements that narrow the gap
for higher-concurrency single-row updates directly on Delta. **I have not personally validated
Lakebase's current maturity/GA status or exact feature set** against this specific
write-back-plus-CDC-sync pattern — I'm naming it because it's directionally the right kind of
capability to evaluate, not because I'm certain it's the final answer today. If it's mature
enough by build time, it could collapse the "separate Postgres + CDC sync" design into "one
Lakebase instance, native to the same governance/catalog boundary" — worth a spike before
committing to the two-system design above.
