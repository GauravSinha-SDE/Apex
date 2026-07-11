# Data Quality Assessment — Apex Manufacturing Sample Data

Every issue below was found by profiling the actual sample files (row counts, distinct-value
counts, and cross-file joins), not by assumption. Each entry documents: **what**, **evidence**,
**impact if unhandled**, and **where the architecture fixes it**.

Severity legend: 🔴 breaks joins/answers · 🟠 corrupts metrics · 🟡 cosmetic/consistency

---

## 1. Identity & Conformance Issues

### DQ-01 🔴 Four equipment ID conventions across systems
| System | Convention | Example |
|---|---|---|
| SCADA/registry, Line 1 | `EQ-1xx` | `EQ-101` |
| SCADA/registry, Line 2 | `EQUIP_2xx` | `EQUIP_201` |
| SCADA/registry, Line 3 | bare numeric | `301` |
| SAP PM (maintenance_logs) | `A-xxx` asset number | `A-101` |

**Impact:** Joining work orders to sensor data or alarms is impossible on raw IDs. A question
like *"equipment with >3 unplanned stops"* silently returns wrong or empty results.
**Handling:** `ref.equipment_xref` conformed dimension assigns a surrogate `equipment_key`
(`EQP-101`…) and maps every source alias. All Silver tables join through it; Gold exposes only
the conformed key + friendly name.
**Assumption (documented):** SAP asset `A-xxx` ↔ registry ID with the same numeric suffix
(A-101 → EQ-101). Verified consistent for all 15 assets present in maintenance_logs; in production
I would confirm against SAP PM equipment master (EQUI table) instead of inferring.

### DQ-02 🔴 line_id encoded differently in every file
`1/2/3` (sensor_readings, production_runs) · `L1/L2/L3` (alarms_events) ·
`Line 1`, `Line-2`, `3` **mixed within one file** (quality_checks) · `Line 1` vs bare `2`
(equipment_registry).
**Impact:** Any per-line aggregation (OEE by line, scrap by line) fragments into phantom lines.
**Handling:** `ref.line_xref` lookup normalizes all observed variants to `line_id INT` in Silver.
Unmapped values fail a DLT expectation (`FAIL UPDATE`) rather than passing through silently.

### DQ-03 🟠 Decommissioned equipment still in registry
`EQ-110` (Depalletizer 1) is `DECOMMISSIONED`, replaced by `EQ-111` on 2025-12-15.
**Impact:** Counting it as active equipment skews availability and maintenance-density metrics.
**Handling:** `is_active` flag in the equipment dimension; dimension is modeled SCD Type 2 in
Silver so historical sensor/WO data still joins to the equipment that existed at the time.

---

## 2. Timestamp Issues

### DQ-04 🔴 Three timestamp formats in sensor_readings — including epoch-milliseconds
294 rows ISO-8601 with `Z`, 180 rows `yyyy-MM-dd HH:mm:ss` (no timezone), **42 rows raw
epoch-ms** (e.g. `1773566325000`). A naive `CAST(timestamp AS TIMESTAMP)` nulls the epoch rows.
**Impact:** ~8% of telemetry silently dropped or lands in 1970.
**Handling:** Silver applies a `coalesce`-style multi-format parser (see `src/pipelines/`):
try ISO → try `yyyy-MM-dd HH:mm:ss` → if value is 13-digit numeric, `timestamp_millis()`.
Rows failing all three are quarantined, not dropped.
**Timezone assumption:** plant is Austin, TX; `Z`-suffixed values are UTC, naked values are
plant-local (America/Chicago). Silver stores everything as UTC `TIMESTAMP` and Gold exposes a
plant-local view column. This must be confirmed with the historian team — a 5–6 h shift error
would corrupt every shift-level KPI.

### DQ-05 🟠 maintenance_logs mixes ISO and US-style `MM/DD/YYYY HH:MM`
33 of 80 work orders use slash format. `install_date` in the registry has **four** formats,
one of which (`15/03/2015`) is day-first — proof that format cannot be inferred per file, only
per value.
**Handling:** same multi-format parser; day-first vs month-first disambiguated by range check
(day > 12 ⇒ day-first) with quarantine when ambiguous.

---

## 3. Value-Level Issues

### DQ-06 🟠 Sentinel error values in sensor readings
8 rows contain `-999.99` (classic historian/PLC error code) and one physically impossible
negative vibration reading (`-1.3`). 11 additional rows have empty `value`.
**Impact:** One `-999.99` in a daily average shifts a temperature mean by several degrees —
enough to fire false process alerts.
**Handling:** Silver expectation: `value IS NOT NULL AND value > -999` routes failures to a
quarantine table with reason codes. Per-tag physical range rules (vibration ≥ 0) applied from a
`ref.sensor_tag_registry` rules table rather than hard-coded.

### DQ-07 🟠 OPC quality codes must gate every analytic
`quality` ∈ {192 Good (457 rows), 0 Bad (33), 64 Uncertain (25)} — 11% of telemetry is not
trustworthy. The text `status` column duplicates it (GOOD/BAD/SUSPECT) — redundant, and one more
overload of the word "status".
**Handling:** Bronze keeps everything; Silver keeps all rows but adds `is_good_quality` boolean;
**Gold aggregates filter to quality = 192 only**. Genie instructions state this rule explicitly
so NL answers never average bad-quality readings.

### DQ-08 🟡 Duplicate sensor rows
3 exact duplicate `(timestamp, equipment_id, sensor_tag)` keys — expected from historian
store-and-forward replays.
**Handling:** `dropDuplicates` on the natural key in Silver (deterministic: keep highest
quality, then latest ingest time). At production scale this becomes watermarked stateful dedupe.

### DQ-09 🔴 Alarm severity mixes two coding systems in one column
`severity` contains numeric `1/2/3/4` (75 rows) **and** text `LOW/MEDIUM/HIGH/CRITICAL`
(75 rows). Mapping is not guessable from data alone (is `1` critical or low?).
**Handling:** `ref.alarm_severity_xref` seed with the FactoryTalk convention
(1=CRITICAL … 4=LOW) — flagged as an **assumption to verify with controls engineering**, since
inverting it inverts every "critical alarms" answer.

### DQ-10 🟡 Maintenance type synonyms
`Preventive` (25) and `PM` (21) are the same thing; `Corrective` vs `Breakdown` overlap.
**Impact:** MTBF must count only *unplanned* stops; miscategorization corrupts it.
**Handling:** `ref.wo_type_xref` maps to normalized type + `planning_category`
(PLANNED/UNPLANNED). MTBF/MTTR in Gold use `planning_category` only.

### DQ-11 🟠 Volume units inconsistent AND on a different scale than they claim
`volume_unit` ∈ {liters, L, gal, gallons}. Cross-check: RUN-001 produced 38,312 units of a
355 ml SKU ≈ **13,600 L**, while `actual_volume` = 13.2 "liters" — the values are in
**thousands of liters (kL)**, mislabeled. Gallon rows need ×3.785 on top.
**Impact:** Volume KPIs wrong by 1000×; unit conversion alone still leaves 1000× error —
this is the trap: fixing the label without noticing the scale.
**Handling:** Silver computes `actual_volume_l = value × unit_factor × 1000` with the scale
inference validated against `units × sku_size_ml`; mismatch > 5% quarantines the row.
SKU sizes come from a `ref.product_sku` seed parsed from product names.

### DQ-12 🟡 Registry `specs` is polymorphic
Sometimes a JSON string, sometimes free text ("Max speed 600 BPM…"), sometimes null; also used
as a decommission note for EQ-110.
**Handling:** Silver `try_parse_json` into a `specs VARIANT` column; unparseable text preserved
in `specs_raw`. Criticality normalized to upper case (`c` → `C`); `type` casing normalized
(`labeler`/`CONVEYOR`/`case packer` → title case).

---

## 4. Semantic Issues

### DQ-13 🔴 "status" means five different things
| File | Meaning | Domain |
|---|---|---|
| sensor_readings | OPC data-quality status | GOOD/BAD/SUSPECT |
| production_runs | run lifecycle | COMPLETED/RUNNING/SCHEDULED/ABORTED/ON_HOLD |
| maintenance_logs | work-order lifecycle | COMPLETED/IN_PROGRESS/OPEN/CANCELLED |
| alarms_events | alarm lifecycle | ACTIVE/ACKNOWLEDGED/SHELVED/CLEARED |
| equipment_registry | operational state | RUNNING/STOPPED/MAINTENANCE/DECOMMISSIONED |

**Handling (architecture level, not just documentation):** Silver/Gold rename every one of
these to a self-describing column: `reading_quality_status`, `run_status`, `work_order_status`,
`alarm_state`, `equipment_operational_status`. No Gold column is ever named just `status`.
This is the foundation of the Genie disambiguation strategy (see `03_nl_analytics.md`).

---

## 5. Referential & Logical Checks (passed / verified)

- `good_units + scrap_units = total_units` holds for all completed runs ✅
- QC `result` is consistent with `lower_spec ≤ value ≤ upper_spec` for all parseable rows ✅
- No COMPLETED work order missing `completed_date` ✅; no CLEARED alarm missing acknowledgment ✅
- All maintenance assets (15) resolve through the crosswalk to registry equipment ✅

These are encoded as DLT expectations anyway — data that is clean today is not clean forever.

---

## Handling Summary by Layer

| Layer | Responsibility |
|---|---|
| **Bronze** | Byte-faithful landing. All columns STRING, plus `_ingest_ts`, `_source_file`, `_rescued_data`. Nothing rejected — auditability first. |
| **Silver** | All fixes above: multi-format timestamp parsing, xref conformance, sentinel/quality gating, dedupe, unit normalization, column renames. DLT expectations with `expect_or_drop` → quarantine tables carrying reason codes. |
| **Gold** | Business semantics only: OEE, MTBF/MTTR, scrap rate, first-pass yield as materialized views over already-trusted Silver. Quality-code filtering baked in. |
| **ref** | Seed-managed crosswalks (versioned in Git, loaded via DAB) — the single place identity assumptions live, so correcting one mapping re-flows everywhere. |
