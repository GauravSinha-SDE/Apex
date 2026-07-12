# Loom Video Walkthrough Script

Not a script to read verbatim — a talking-points outline with what to show on screen, in the
order that tells the strongest story. Target ~15 min. The brief says: *"We value your
reasoning and design decisions over feature completeness"* — so narrate the *why* and the
*trade-offs*, don't just tour the file tree.

**The one thing to make sure comes through:** this wasn't only written, it was actually
deployed to a real Databricks workspace and run end-to-end, twice — once to get the platform
running, once to test it with real Genie questions — and both rounds found and fixed real bugs.
That's the single strongest differentiator in this submission. Don't bury it as a footnote;
it's worth its own beat (§5 below).

---

## 0. Framing (30 sec)

"Apex Manufacturing needs a lakehouse plant managers can query in plain English. I'll walk
through the data quality issues I found, the architecture that handles them, the NL analytics
approach, and — the part I think matters most — what happened when I actually deployed and
ran this against a real workspace instead of just writing it."

Show: `README.md` (the brief) for 5 seconds, just to frame scope, then move on fast.

---

## 1. Data Quality (2–3 min)

**Show:** `docs/02_data_quality.md`, maybe `notebooks/01_data_exploration.py` briefly to prove
findings were evidenced, not assumed.

Talking points — don't read all 14, pick the 3 most interesting to narrate:

- **DQ-11 (volume scale trap)** — the best one to lead with. `actual_volume` claims litres but
  is actually in *thousands* of litres, discovered by cross-checking against
  `total_units × SKU size`. "Fixing the unit label alone still leaves a 1000× error — that's
  the trap." Shows you dig past the surface.
- **DQ-01 (four equipment ID schemes)** — `EQ-101` / `EQUIP_201` / bare `301` / SAP `A-101`,
  resolved through one `ref.equipment_xref` conformed key. Say the assumption out loud: SAP
  suffix-matching is inferred, not confirmed — "I'd verify against the EQUI table in a real
  engagement."
- **DQ-13 ("status" means 5 things)** — this is the one that drives the whole NL-analytics
  design (§4). "No Gold column is ever literally named `status` — that's not a naming
  convention, it's the actual fix for semantic ambiguity."
- **Mention DQ-14 exists** but say you'll come back to it — it was found later, live (§5), and
  it's a stronger beat there than here.

---

## 2. Architecture & Physical Design (2–3 min)

**Show:** `src/ddl/01_catalog_and_ref.sql`, `docs/01_architecture.md` §Governance,
`src/pipelines/apex_pipeline.py`'s `silver.sensor_readings` decorator for Liquid Clustering.

Talking points:

- Catalog-per-environment (`apex_dev`/`apex_stg`/`apex_prod`), schema-per-layer
  (`bronze`/`silver`/`gold`/`ref`) — stable across envs, so a grant statement written once has
  the same shape everywhere.
- **Liquid Clustering, not partition+ZORDER**, on `sensor_readings` (170M rows/day, Part 1c) —
  cluster columns match the actual query pattern (equipment, tag, time), and it reclusters
  incrementally instead of needing scheduled full-partition `OPTIMIZE`. Mention the migration
  path is one-way (no going back to ZORDER once clustered) — that's why the DDL keeps the old
  table around during cutover.
- **Governance**: 3 personas (engineers/analysts/plant managers), row-level security by
  `line_id` enforced at the Gold view layer specifically — because that's the only layer Genie
  is allowed to touch (ties directly into §4).

---

## 3. Pipeline (bronze → silver) (2 min)

**Show:** `src/pipelines/apex_pipeline.py` — scroll through `parse_multi_ts`,
`with_line_id`, one `@dlt.table` with expectations.

Talking points:

- Bronze is byte-faithful, all-STRING, Auto Loader with rescued-data column — "nothing is
  rejected at this layer, that's a deliberate auditability choice."
- Silver is where every DQ fix lives, backed by `@dlt.expect_or_drop` — "violations are
  observable in the event log, not silently dropped."
- Quarantine pattern: `expect_or_drop` on the main table + an inverse-filtered quarantine
  table with a reason code. "Nothing is ever lost, just routed."
- One line worth calling out: `with_line_id()` originally produced a duplicate `line_id`
  column after the ref-table join — a real bug found during live testing (§5), not something
  visible from reading the code. Good bridge into that section.

---

## 4. Natural Language Analytics / Genie (3–4 min) — the semantic design

**Show:** `docs/03_nl_analytics.md` §1 and §3, the Gold schema listing.

Talking points:

- Genie is scoped to `gold` only — **two reasons**, say both: semantic safety (ambiguity is
  already resolved by the time data reaches Gold) and governance (RLS is enforced at that
  layer, so Genie inherits it for free instead of needing its own access logic).
- The "status" disambiguation (DQ-13) is the concrete payoff: renamed to
  `run_status`/`work_order_status`/`alarm_state`/`equipment_operational_status` — "a column
  that doesn't exist under the ambiguous name can't be misused, no matter what Genie decides
  to do with an ambiguous question."
- Mention the three real levers used: table/column **instructions**, **trusted example
  questions**, **trusted SQL** for the harder joins (OEE's 3-table downtime attribution).

**Then pivot hard into §5 — don't present this as two separate topics, it's one continuous
story: "here's the design, and here's what happened when I actually tested it."**

---

## 5. The live-testing story (3–4 min) — spend real time here

This is the differentiator. Don't rush it.

**Show:** the actual Genie conversation (screenshot or live if you still have workspace
access), `docs/03_nl_analytics.md` §2a's results table, `docs/02_data_quality.md` DQ-14.

Talking points, told as a narrative:

1. "I deployed the whole thing — bundle, pipeline, all 6 gold marts — to a real Databricks
   workspace, then asked Genie the brief's own 5 example questions."
2. "4 of 5 were handled correctly — Genie declined rather than guessed when data was out of
   range or a mart didn't exist yet. That's the Gold-only scoping working as designed."
3. **"One was wrong, and it's the most important finding in this whole exercise."**
   `equipment_reliability` had no time dimension. Asked 'unplanned stops this month,' Genie
   confidently returned the *all-time* total, mislabeled as monthly. Say explicitly: "that's
   worse than a decline — a plant manager would trust it."
4. "Fixing it (adding a `stop_month` grain) led to building `changeover_time` as a comparison
   point — and *that* surfaced a brand-new data quality issue, DQ-14: Line 3 has physically
   overlapping production runs in the sample data. Invisible from row-by-row profiling, only
   visible once I computed a derived cross-run metric and got a negative duration."
5. Close the loop: "Both are fixed, both are re-verified against real data, and I went on to
   close a 3rd of the 5 example questions (critical work orders by technician) the same way —
   build, deploy, ask Genie the real question, confirm the real answer."

**Why this matters for the evaluation criteria:** this is direct evidence of "data engineering
rigor... handling real-world data quality issues, not just the happy path" and "pragmatism:
realistic, deployable solutions over theoretical perfection" — the two hardest criteria to
demonstrate in a written-only submission.

---

## 6. Part 2: Ingestion Architecture (2–3 min)

**Show:** `docs/diagrams/ingestion_architecture.drawio`, `docs/01_architecture.md` §B/C table.

Talking points:

- Walk the diagram left to right: MQTT→Kafka bridge (Line 3, true streaming, sub-minute SLA)
  vs. PI historian (micro-batch, Auto Loader) vs. SAP/LIMS (batch, matches their own export
  cadence).
- State the principle once, clearly: "match ingestion mode to the source's actual data
  production cadence — three of five sources here are batch or micro-batch, not everything
  defaults to streaming."
- Flag the one honest uncertainty: Lakeflow Connect's SAP support wasn't independently
  verified for Apex's specific hosting — "I said so explicitly rather than presenting it as
  confirmed."

---

## 7. Part 2D: Deployment (`databricks.yml`) (2 min)

**Show:** `databricks.yml`, maybe the Databricks Workflows UI if still available (job graph
with 6 gold-mart tasks in sequence is visually convincing).

Talking points:

- dev/staging/prod targets, one bundle, catalog-per-env.
- The bootstrap job's dependency chain — schemas/ref → seeds → pipeline run → 6 gold marts in
  sequence — and *why* sequential, not parallel: "found live, this workspace tier caps
  concurrent materialized-view-backed pipelines at 1."
- One platform-knowledge beat worth including: `${catalog}` looks like it should substitute in
  a SQL file task the same way it does in a notebook widget — it doesn't. "That's the kind of
  thing you only find by actually running it, which is exactly what I did."

---

## 8. Part 3: Short Answers (1 min)

**Show:** `docs/04_short_answers.md` headings only, don't read them.

"Answered Q2, Q4, Q5, Q6 — storage optimization, semantic disambiguation, late-arriving data,
and operational write-back — each one ties directly to a decision made elsewhere in the repo,
so I'm demonstrating reasoning I already exercised rather than restating generic platform
knowledge cold."

---

## 9. Assumptions & Close (1–2 min)

**Show:** `README-SOLUTION.md`'s assumptions list.

- Name the two highest-stakes assumptions out loud: alarm severity direction (1=CRITICAL,
  FactoryTalk convention, unverified against Apex's actual SCADA config) and the timezone
  assumption on naked timestamps. "Both are flagged explicitly because getting either wrong
  silently corrupts a KPI rather than throwing an error."
- Close with 2-3 items from "what I'd do with more time" — the correlation mart and the
  batch-to-run SKU mapping are good ones, since they tie back to the still-open example
  questions from §4/§5.

"That's the walkthrough — happy to go deeper into any piece in the follow-up discussion."

---

## Quick prep for the follow-up discussion

The brief schedules a 15–20 min live discussion after submission. Things likely to come up,
worth having a crisp answer ready for:

- **"Why Liquid Clustering over ZORDER?"** — reclusters incrementally vs. requiring scheduled
  full-partition `OPTIMIZE`; migration is one-way, so the DDL keeps the old table during
  cutover as a rollback path (Q2, `docs/04_short_answers.md`).
- **"Walk me through what broke when you actually deployed this."** — you have six concrete,
  specific answers ready (`REPO_STRUCTURE.md`'s "Live-tested" section) — pick 2-3, don't list
  all six.
- **"How would you verify the SAP asset-number assumption for real?"** — SAP PM's `EQUI`
  table directly, not suffix pattern-matching (already stated as a limitation).
- **"What would you build next?"** — the correlation mart and the alarm_summary mart are the
  cleanest next steps; the batch-to-run SKU mapping is the one you'd want *real data* for
  rather than inferring.
