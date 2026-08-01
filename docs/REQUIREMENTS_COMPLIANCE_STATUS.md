# SQL Server Raw/Curated Requirements — Compliance Re-Verification

**Commit:** `34c4aa7` · **Method:** three parallel read-only verification
agents re-checked every finding of `REQUIREMENTS_GAP_ANALYSIS.md` (written
against the same specification before the Phase 2/3 remediation) plus every
requirement area of the spec directly. All file:line citations were
verified against current sources. No code was modified.

---

## 1. Verdict summary

The framework has moved from **"partially — roughly half"** (the original
gap analysis) to **substantially compliant**:

| Population | Closed / Met | Partial | Still open |
|---|---|---|---|
| Critical findings C1–C12 | 7 | 4 | 1 (C9) |
| Significant findings S1–S16 | 10 (incl. 1 different-by-design) | 4 | 2 (S2, S7) |
| §22 acceptance criteria (18) | 16 pass | 2 pass-with-conditions | — |

**Fully closed criticals:** C1 freshness-compared merge (late-arriving
older records ignored; ties deterministically keep the target, `'T' > 'I'`
— `CuratedService.scala:582-589`, tests `CuratedMergeIntegrationSpec`
:132/:151/:171/:185), C4 FULL→INCR watermark seeding, C5 SOURCE_CLOCK
(`SYSUTCDATETIME`) upper bound (opt-in), C7 entity lease locking wired
before extraction and renewed at stage boundaries, C8 watermark gated on an
actual curated publish (`advance_after=RAW` opt-out), C10 no more silent
unbounded-window fallback (JDBC_006), C12 key hygiene in every mode.

**The one still-open critical — C9:** rows diverted to `ingest_rejects`
exit the committed watermark window permanently; the planned
`on_reject_watermark = ADVANCE|HOLD` mode and reject-replay entry point
(REMEDIATION_PLAN §"rejects") were never implemented. Mitigations exist
(full payload retained unless masked, opt-in reject thresholds fail the run
before the commit, counts reconciled) but recovery is manual.

---

## 2. Requirement-area scorecard (current)

| Spec section | Verdict | Notes |
|---|---|---|
| §2 Metadata-driven mapping | **Met** (near-complete) | ColumnContract now carries sourceType, transform, sensitivity, businessKey, incremental; keys/watermark columns derive from the contract (HDR_017); JDBC introspection bootstraps 350-column contracts. Missing: per-column source-identity (feed-level instead), spreadsheet import |
| §3 Raw layer & metadata | **Partial** (10 of 14 columns) | run_id, load_timestamp, ingest_dt (config-derived), extract_start/end_ts, source_system/database/schema/table, record_hash (opt-in) present; missing: `source_last_modified_ts` (role filled by contract-designated freshness column), `source_operation`, `source_primary_key`, `load_status`. Silent metadata-drop trap closed (RAW_001 FAIL default) |
| §4 Initial full load | **Met** except snapshot consistency | SOURCE_CLOCK cutoff, seed-commit-after-success, reseed guard, partitioned reads mode-independent. §4.3 isolation/RCSI/`applicationIntent` support absent (S7) |
| §5 Incremental extraction | **Met** with a documented deviation | Boundary convention is `(lower, upper]` (`> lower AND NOT (> upper)`), not the spec's `[start, end)`. Equivalent gap-free invariants hold: frozen pre-read upper, no gaps, no double-count beyond deliberate overlap. Record as an accepted deviation |
| §6 Curated layer | **Met** | Freshness contest with configurable tie-breakers; one active row per key enforced at publish; curated metadata (create/update ts, run id) maintained; partitioning explicitly rejected (correct for current-state). Caveat: freshness is per-feed config; unconfigured merge feeds fall back to last-write-wins with only a WARN |
| §7 Change detection | **Met** (opt-in) | record_hash: sorted column order, null≠empty, technical-exclusion via contract; merge genuinely skips unchanged rows. No value-level trim/case normalization (documented design limit) |
| §8 Source deletes | **Met for SOFT/IGNORE** | Declared strategy, tombstones compete via freshness, no resurrection, disjoint counts, tested. Missing: Change Tracking/CDC integration, reconciliation-based delete detection (explicitly future work) |
| §9 Late-arriving/backfill | **Met** | Freshness-based, not event-date; older versions land in raw but never overwrite newer curated (tested) |
| §10 Idempotency/restart | **Met** | Mandatory ledger (PIPE_002) with run_mode/window columns; run_id + window guards; drift detection; `--resume` slice replay. Rerun-window guard best-effort under SOURCE_CLOCK (differing uppers), documented |
| §11 Audit/reconciliation | **Largely met** | source=accepted+rejected, raw duplicates (opt-in key), curated uniqueness, null-key accounting, DQ validation — all present, default on_mismatch=FAIL. Missing: an explicit watermark-continuity *check*, any periodic source-vs-curated reconciliation job |
| §12 Rejects | **Met minus two fields** | Full lineage + reason + payload with sensitivity-driven redaction (FULL/MASKED/HASHED/KEYS_ONLY); thresholds decide fail-vs-continue. Missing: dedicated failed-column field, reprocessing-status field (ties into C9) |
| §13 Schema evolution | **Met** | Drift policies, name-based mapping, versioned contracts, BACKWARD enforcement, cast-loss guard (CUR_002), no silent positional mapping |
| §14 Timestamps | **Met** | UTC session default, mandatory clock_zone for SOURCE_CLOCK over zoneless columns, DATETIMEOFFSET(7) precision, DST-safe overlap arithmetic |
| §15 Performance | **Reviewed separately** | See `PERFORMANCE_REVIEW.md` — notable open items: O(table) curated publish, single-partition default JDBC read |
| §16 Security | **Met** (in-scope items) | Vault providers, TLS enforcement with explicit opt-outs, log sanitization (tested), contract sensitivity classification driving reject masking. At-rest encryption/RBAC/curated masking = platform concerns |
| §17 Retention | **Met** | RetentionService: raw partition drops, reject/audit purges, watermark keep-last-N, dry-run, `--stage retention` under lock (CLI bug fixed) |
| §18 Concurrency | **Met (best-effort)** | Claim-settle-reread lease with expiry + watermark version CAS; residual races on non-transactional Hive honestly documented; strict serialization needs a transactional store (the unwired `JdbcCheckpointStore` already implements true CAS) |
| §19 Empty runs | **Met for INCR** | Zero-row runs succeed, skip the merge, record zero counts; watermark advances on idle sources only with SOURCE_CLOCK (MAX_VALUE holds safely — spec-compliant advance requires SOURCE_CLOCK). Empty FULL requires `allow_empty` (deliberate wipe protection) |

## 3. §22 acceptance criteria: 16 of 18 PASS

Two carry conditions: #9 (deletes) passes for SOFT/IGNORE only —
reconciliation-based deletes absent; #14 (empty runs) passes with the
SOURCE_CLOCK qualifier above. Three (#3, #4, #15) are statically verified
but depend on non-transactional Hive race behavior at runtime —
documented, mitigated, not provable by code reading. The FULL→INCR handoff
has no end-to-end SQL Server test (H2 only).

---

## 4. Remaining gap backlog (priority order)

1. **C9 — reject-window recovery**: implement `on_reject_watermark = HOLD`
   or a reject-replay entry point; add the reject `reprocessing_status` and
   failed-column fields (§12).
2. **Watermark-continuity check** (§11): assert new lower == previous upper
   (unless overlap) at plan time; cheap, closes an audit-blind spot.
3. **Periodic reconciliation job** (§11): source vs curated counts/keys/
   sampled hashes; the declarative `reconcile/` engine (unwired) is a
   natural home.
4. **Snapshot consistency for initial loads** (§4.3 / S7): support
   isolation hints / `applicationIntent` / RCSI guidance per dialect.
5. **Delete completeness** (§8): SQL Server Change Tracking integration or
   reconciliation-based delete marking; curated CDC delete application.
6. **FULL-path shrink guard default** (C11 residue): `max_shrink_percent`
   defaults only for INCR; a truncated FULL extract can still overwrite.
7. **Raw metadata completeness** (§3.2): stamp `source_last_modified_ts`
   (copy of the designated freshness column), `source_operation`,
   `source_primary_key`, `load_status` where available.
8. **Strategic**: atomic publish (location swap → Delta/Iceberg MERGE) —
   also the top performance finding (PERFORMANCE_REVIEW F1); transactional
   watermark/lock store.

## 5. §21 decisions — answered by the framework vs still open

**Answered (mechanism exists; per-feed values still needed):** business key
(contract `businessKey`), null-key handling (quarantine/passthrough,
configurable), incremental column (contract `incremental`), tie-breakers
(`merge.freshness.tie_breakers`), delete representation (SOFT tombstones /
IGNORE), late-arrivals (expected, freshness-handled), raw duplicate policy
(at-least-once + `raw.idempotency_key` monitoring), target tech (Hive/ORC
now, Delta anticipated), schema changes (drift policies), sensitive columns
(contract `sensitivity`), reject policy (quarantine + thresholds),
historical layer (raw is the history), timestamps (UTC + clock_zone).

**Still requiring business/ops decisions per feed:** exact key values,
whether keys can change, timestamp precision of each source, physical
delete behavior at each source, expected volumes/change rates/frequencies,
initial-load isolation level, retention periods, whether curated is shared
across source systems, acceptable reconciliation thresholds, whether
rejected rows may block watermark advance (drives item 1).
