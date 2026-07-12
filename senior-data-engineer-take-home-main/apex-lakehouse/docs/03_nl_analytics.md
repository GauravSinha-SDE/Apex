# Natural Language Analytics — Genie Approach (Part 1b)

## 1. Approach: Genie space over Gold only

A **Genie space** scoped to the `gold` schema is the NL entry point for plant managers.
Genie is not pointed at `silver` or `bronze` — two reasons:

1. **Semantic safety.** Gold is the only layer where every ambiguous term from the source
   systems has already been resolved (see §3). Below Gold, `status` still means five things
   and IDs are still in four incompatible schemes.
2. **Governance.** RLS by `line_id` (Part 2E) is enforced at the Gold view layer. Genie
   inherits Unity Catalog permissions natively — a plant manager's Genie session can only
   ever see the rows their UC identity is allowed to see, without Genie-specific access logic.

Concretely: a **Genie space** per plant-manager audience (could be one shared space with
row-level security doing the scoping, rather than one space per line — see Part 2E), backed
by six marts covering 6 of the 7 plant KPIs (`docs/apex_plant_context.md`) —
`gold.oee_daily`, `gold.equipment_reliability`, `gold.scrap_rate_daily`,
`gold.changeover_time`, `gold.first_pass_yield`, `gold.critical_work_orders` — plus
`gold.alarm_summary` still on the "what's next" list (§5). CIP Cycle Time has no mart at
all: the sample extracts contain no CIP cycle timing signal anywhere (checked — see
`docs/02_data_quality.md`), so there's nothing to build it from, not a shortcut. Each
table/view gets **Genie table + column instructions** (comments), **trusted example
questions**, and (for the harder joins) **trusted SQL** — the three levers Genie actually
exposes today for steering NL-to-SQL, and I'm treating them as the whole toolkit rather than
assuming any other configuration surface exists.

## 2. Ensuring accurate results

Given this dataset's specific defects (`docs/02_data_quality.md`), accuracy risk is concentrated
in three places, and each has a specific mitigation:

**a. Never let Genie touch ungoverned quality.** `sensor_readings` is 11% `BAD`/`SUSPECT`
by OPC quality code (DQ-07). If Genie is ever allowed to `AVG(value)` over raw Silver
telemetry, a bad-quality burst silently corrupts the answer to "what's the average fill
weight trend." **Mitigation:** Gold marts pre-filter to `is_good_quality = true`, and Genie's
table instructions state explicitly: *"Only gold.* tables are valid for questions about
plant metrics; never aggregate silver.sensor_readings directly."* This is enforced by scope
(Genie has no read grant on `silver` at all — see Part 2E), not just by asking nicely in an
instruction; the instruction is a second layer, not the control.

**b. Give Genie the vocabulary the plant actually uses, not the schema's.** Plant managers
say "unplanned stop," not `planning_category = 'UNPLANNED'`; they say "critical alarm," not
`severity_rank = 1`. Genie **synonyms** and **column descriptions** map these directly:
`gold.equipment_reliability.mtbf_hours` gets the description *"Mean Time Between Failures —
hours between unplanned stops only. Target > 72h (plant KPI)."* Without this, Genie can
generate syntactically correct SQL that answers the wrong business question (e.g. counting
*all* stops, planned and unplanned, into an MTBF-labeled column).

**c. Verify with trusted assets, not blind trust in NL-to-SQL.** For the KPI questions from
the brief (OEE, MTBF, correlation, trend, top-technician), I'd hand-write and **pin as
trusted queries** the 5-10 canonical patterns rather than let Genie freehand a fresh SQL
join every time. Trusted queries are Genie's mechanism for "always run *this exact SQL* for
this class of question" — it removes join-path ambiguity for the OEE calculation in
particular, which has three tables and a non-obvious downtime-attribution join
(`silver.maintenance_work_orders` → `ref.equipment_xref` → line) that an LLM asked to
improvise SQL cold is more likely to get subtly wrong (e.g. joining on `equipment_id` instead
of the conformed `equipment_key`, silently dropping SAP-only assets).

**Verification loop I would NOT skip if I had more time**: a benchmark set of ~20 NL questions
(the 5 in the brief plus edge cases — ambiguous line references, out-of-range dates, questions
about decommissioned equipment) run against Genie on a schedule, diffed against known-correct
SQL answers. Genie has no built-in regression harness; this would be a small Workflows job.

### 2a. This verification loop is not hypothetical — I ran it

I deployed this design (databricks.yml, the Lakeflow pipeline, and the gold marts) to a real
Databricks workspace and asked Genie the brief's own 5 example questions against it. Results,
because they're more convincing than describing the methodology abstractly:

| # | Question | Result |
|---|---|---|
| 1 | OEE for Line 2 last Tuesday | **Correctly declined** — checked the actual date range, found no data for that specific date (sample data doesn't extend to "last Tuesday" relative to today), and said so instead of guessing. |
| 2 | Equipment with >3 unplanned stops this month | **Wrong on the first pass, then fixed.** `gold.equipment_reliability` originally had no time dimension — Genie answered with the all-time cumulative count, presented with full confidence, mislabeled as "this month." This is worse than a decline: a plant manager would trust it. Fixed by adding a `stop_month` grain to the mart (§5 has the before/after). |
| 3 | Quality parameter most correlated with scrap rate on Line 1 | **Correctly declined** — no correlation-ready join existed at the time; `gold.scrap_rate_daily` now exists but a proper parameter-vs-scrap-rate correlation mart still doesn't (§5). |
| 4 | Fill weight trend for SKU CSD-001 | **Correctly declined** — genuine data-model gap, not a missing mart: `quality_checks.batch_id` has no foreign key to `production_runs.product_sku` in the source extracts, so this can't be built without inferring a batch-to-run mapping (e.g. by date+line proximity), which I haven't done and wouldn't want Genie improvising either. |
| 5 | Technician who resolved the most critical work orders last week | **Correctly declined at the time, then closed.** No gold mart yet exposed `silver.maintenance_work_orders`' technician/priority columns; `docs/02_data_quality.md`'s DQ-01/DQ-10 fixes were already in Silver, so this was purely a missing Gold mart — `gold.critical_work_orders` now exists (per-technician, per-plant-local-week, filtered to `priority = 'Critical'` and `work_order_status = 'COMPLETED'`), re-verified against real data (e.g. T-Nguyen resolved 2 critical WOs the week of 2026-02-16). |

**Takeaway:** every decline observed was *correct* behavior (Gold-only scoping doing its job —
Genie had no ungoverned table to fall back on and guess from) except one, which was a genuine,
confidently-wrong answer caused by a real gap in the mart's design (no time dimension), not by
Genie misbehaving — and it's now fixed. Two of five questions (3 and 4) are still open gaps,
not bugs — see §5. This is exactly the class of issue a verification loop is supposed to
catch, and exactly why I ran one instead of only describing it.

## 3. The "status" ambiguity — architecture-level fix

This is deliberately **not** solved by prompting Genie to "be careful." It's solved before
Genie ever sees the data:

| Source file | What "status" meant | Gold column name |
|---|---|---|
| `sensor_readings` | OPC data-quality code | *(not exposed — filtered, not surfaced, in Gold)* |
| `production_runs` | run lifecycle | `run_status` |
| `maintenance_logs` | work-order lifecycle | `work_order_status` |
| `alarms_events` | alarm lifecycle | `alarm_state` |
| `equipment_registry` | operational state | `equipment_operational_status` |

**No Gold column is ever named `status`.** This means a question like *"what's the status of
Line 2?"* cannot resolve to a single ambiguous column even by accident — Genie is structurally
forced to pick among `run_status`, `work_order_status`, `alarm_state`, or
`equipment_operational_status`, which is exactly the disambiguation the VP needs surfaced
(see Part 3 Q4, `docs/04_short_answers.md`, for how Genie is instructed to handle the
resulting multi-domain question rather than just guess one).

This is the general pattern for "similar ambiguities": **rename at the Silver boundary, not
just document the collision.** A comment saying "status means X here" still leaves an LLM
free to write `SELECT status FROM ...` against the wrong table because the column exists and
parses. A column that doesn't exist under the ambiguous name can't be silently misused.

The same pattern was applied to every other multi-scheme concept: `line_id` (DQ-02),
`equipment_key` (DQ-01), `severity_rank` (DQ-09) are all single conformed columns, not
column-name collisions across tables with different meanings hiding behind a shared name.

## 4. Concrete configuration examples

**Genie space-level instructions** (general steering, applies to every question):
```
- Only query tables in the gold schema. Never query silver or bronze directly.
- "Status" is never a column name in this schema. If the user asks about "status" without
  qualification, ask a clarifying follow-up: operational status (is the equipment running),
  production status (is a run in progress), maintenance status (open work orders), or alarm
  status (active alarms)? If the question implies one of these from context, answer directly
  and state which interpretation you used.
- All sensor-derived metrics in gold.* already exclude OPC quality codes 0 (Bad) and 64
  (Uncertain). Do not caveat every answer with a data-quality disclaimer — it's already handled
  upstream. Only mention quality filtering if the user asks about it directly.
- Dates/times in gold tables are plant-local (America/Chicago) unless a column name ends
  in _utc. "Last Tuesday" means plant-local Tuesday.
- "Line 1/2/3" always maps to the conformed line_id column, never to raw line-name strings.
```

**Column-level instruction, `gold.oee_daily.oee`:**
```
OEE = availability x performance x quality, computed per line per day. Availability excludes
only UNPLANNED downtime (see equipment_reliability.mtbf_hours) — planned maintenance and
changeovers do not count against availability. Target is > 0.85 (85%). When asked "what was
OEE for Line 2 last Tuesday," filter line_id = 2 and production_date = that specific date;
do not average across a range unless the user asks for a trend.
```

**Column-level instruction, `gold.equipment_reliability` (added after the live-testing finding
in §2a — this is the guardrail, not a substitute for the mart fix):**
```
This table is PER CALENDAR MONTH (stop_month column), not all-time. "How many unplanned stops
did EQP-105 have" without a time qualifier means the CURRENT month unless the user says
otherwise — do not sum across all months and present it as if it were a single period. For an
explicit all-time total, SUM(unplanned_stop_count) across all stop_month rows for that
equipment, and say "all-time" in the answer so it isn't confused with a monthly figure.
mtbf_hours computed within one month from a small stop count is statistically noisy — flag
this if the underlying count is 3 or fewer.
```

**Trusted example question -> SQL** (pinned, not left to freehand generation), for the
brief's own example *"What was the OEE for Line 2 last Tuesday?"*:
```sql
SELECT line_id, production_date, availability, performance, quality, oee
FROM gold.oee_daily
WHERE line_id = 2
  AND production_date = date_sub(current_date(), pmod(dayofweek(current_date()) - 3, 7) + 7)
```
(Trusted SQL for relative-date phrases is worth pinning explicitly — "last Tuesday" is a
recurring plant-manager phrasing, and I'd rather hand-verify the date arithmetic once than
trust it fresh in every generated query.)

**Synonyms** (Genie's term-mapping mechanism):
```
unplanned stop, breakdown, failure  -> planning_category = 'UNPLANNED' rows in
                                        gold.equipment_reliability (per-month grain — see
                                        that column's instruction above)
critical alarm                       -> severity_rank = 1 in future gold.alarm_summary
scrap, rejects, waste                -> gold.scrap_rate_daily.scrap_rate
first-pass yield, right-first-time   -> gold.first_pass_yield.first_pass_yield
changeover                           -> gold.changeover_time.changeover_minutes (per-event
                                        grain, one row per SKU switch on a line)
critical work order                  -> priority = 'Critical' rows in
                                        gold.critical_work_orders (per-technician,
                                        per-plant-local-week grain, resolved_week is the
                                        completion week, not the creation week)
```

## 5. What I'd add with more time

Built and live-tested during this exercise: `gold.oee_daily`, `gold.equipment_reliability`
(now per-month), `gold.scrap_rate_daily`, `gold.changeover_time`, `gold.first_pass_yield`,
`gold.critical_work_orders` — 6 of the 7 plant KPIs, closing 3 of the brief's 5 example
questions (1, 2, 5), with `docs/02_data_quality.md`'s DQ-14 (Line 3's overlapping run windows)
found and handled along the way. What's still missing:

- **A quality-parameter-vs-scrap-rate correlation mart** — closes example question 3.
  `gold.scrap_rate_daily` exists now, but correlating it against `quality_checks.parameter`
  values needs a proper per-line-day pivot/join, not just two separate marts sitting next to
  each other. More involved than the others; genuinely not built yet.
- **A batch-to-run mapping for `quality_checks.batch_id` → `production_runs.product_sku`** —
  closes example question 4 ("fill weight trend for SKU CSD-001"). This is a data-model gap
  in the source extracts, not a missing mart: there's no FK between the two tables as given.
  Would need to either get LabWare to export the run/batch link, or infer it (e.g. batch's
  `check_timestamp` falls within a run's `[start_ts, end_ts]` on the same line) — inference is
  risky enough (overlapping runs exist per DQ-14) that I'd rather get the real link than guess.
- `gold.alarm_summary` mart.
- A Genie **certified answers** review pass with an actual plant supervisor — my synonym
  list and instructions are my best inference from `docs/apex_plant_context.md`, not verified
  domain language.
- The regression benchmark described in §2, formalized (§2a is one manual pass through it,
  not the scheduled/automated version).
