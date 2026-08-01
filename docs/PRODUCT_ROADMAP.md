# Product Roadmap — File Ingestion vs. Informatica and Databricks

Status: draft for stakeholder review.
Baseline: framework at `a3d7d44` (post core-review fixes, post prompt-program
merge; 754 tests green). Competitor capabilities reflect Informatica IDMC
(Cloud Mass Ingestion / PowerCenter + PowerExchange lineage of features) and
Databricks (Auto Loader / Delta Live Tables / Lakeflow) as generally
available in early 2026. This document covers the FILE ingestion path;
JDBC/CDC ingestion deserves its own comparison.

---

## 1. Executive summary

The framework's differentiated strength is **auditable correctness**:
content-checksum idempotency with crash-safe recovery, a contract-driven
header/content validation gate with a stable error catalog, sensitivity-aware
reject quarantine, a mandatory run ledger with enforced count-accounting
identities, deterministic replay, and entity-level concurrency leases. In
these areas it *exceeds* what either competitor gives out of the box —
Databricks has no native file lifecycle (landing/quarantine/processed) and
Informatica's reconciliation is configuration-heavy rather than
identity-enforced.

The gaps are **breadth and operability**, and they cluster into four themes:

1. **Formats** — the validation gate is delimited-text-only; no fixed-width,
   no COBOL/EBCDIC, no XML/JSON contract validation, no PGP, no archive
   handling. For a healthcare estate (mainframe extracts, B2B exchanges)
   this is the largest functional gap.
2. **Arrival management** — nothing watches for files that *should* have
   arrived; no SLA calendar, no event-driven trigger, no multi-file batch
   completeness (manifest/control files).
3. **Observability** — metrics stop at log lines; no lineage emission, no
   alerting channel, no operational console. The audit ledger already holds
   the data; nothing surfaces it.
4. **Product engineering** — no semver releases, no plugin SPI, no
   transactional table format (Delta/Iceberg — plan item #19).

The roadmap below sequences ~18 items so that each phase is independently
shippable and the differentiators (governance-first design) are preserved,
not diluted, as breadth is added.

---

## 2. Where the framework already leads

| Capability | Ours | Informatica | Databricks |
|---|---|---|---|
| Content-checksum idempotency + duplicate policy (SKIP / REJECT / REPROCESS_WITH_APPROVAL) with crash-safe recovery | ✅ built-in, tested | ⚠️ configurable, per-mapping effort | ⚠️ file-name based (Auto Loader tracks paths, not content) |
| Enforced count reconciliation (`insert+update+delete+ignored+nullKey+deduped+passthrough == accepted`, FAIL by default) | ✅ identity enforced | ⚠️ optional DQ rules | ❌ user-built |
| Header contract: canonical alias mapping, positional fallback, stable HDR_* catalog, per-file header audit trail | ✅ | ✅ (mapping-level) | ❌ (schema hints only) |
| Sensitivity-aware reject payloads (FULL/MASKED/HASHED/KEYS_ONLY, PII/PHI tags honored in every mode) | ✅ | ✅ (with CDQ/masking add-ons) | ❌ user-built |
| Deterministic freshness-compared merge with tombstone semantics and stale-update protection | ✅ | ⚠️ mapping logic | ⚠️ `APPLY CHANGES` (DLT) comparable |
| Governed replay (`--stage curated`) that never touches the watermark and refuses partial RAW slices | ✅ | ⚠️ | ⚠️ (Delta time travel helps, different model) |
| Config-as-code feeds (HOCON) with cross-section compatibility validation (CFG_001..009) and a generator wizard | ✅ | ❌ (GUI/repository model) | ⚠️ (notebooks/DABs) |

Keep these. Every roadmap item below must pass the same bar: fails loudly,
audited, replayable.

---

## 3. Gap matrix

Legend: ✅ have · 🟡 partial · ❌ missing. "Sev" = business severity for a
healthcare file estate.

### 3.1 Formats & payloads

| Feature | Ours | Informatica | Databricks | Sev |
|---|---|---|---|---|
| Delimited text (CSV/TSV, quote/escape/multiline, encoding validation HDR_012) | ✅ | ✅ | ✅ | — |
| Fixed-width layouts (offset/length spec per column) | ❌ | ✅ | ❌ (user parses) | **High** |
| COBOL copybook / EBCDIC (mainframe extracts) | ❌ | ✅ (PowerExchange) | ❌ (3rd-party libs) | **High** for mainframe estates |
| JSON/XML with contract validation (nested schema, required paths) | 🟡 Spark can read; contracts don't apply | ✅ (hierarchy parsing) | 🟡 (inference + rescue, no contracts) | Med |
| Avro/Parquet/ORC as *sources* | 🟡 (reader passthrough, no header gate) | ✅ | ✅ | Low-Med |
| EDI (834/837/835, HL7) | ❌ | ✅ (B2B Data Exchange) | ❌ | High *if in scope* — decide explicitly |
| Excel | ❌ | ✅ | ❌ | Low |
| Zip/archive intake (one archive → N logical files, lifecycle per member) | ❌ | ✅ | 🟡 | Med |
| Transparent codecs (gzip etc. via Hadoop) | ✅ | ✅ | ✅ | — |
| PGP/GPG decryption at intake | ❌ | ✅ | ❌ (user-built) | **High** for B2B exchange |

### 3.2 Arrival, triggering & completeness

| Feature | Ours | Informatica | Databricks | Sev |
|---|---|---|---|---|
| Expected-arrival SLAs with business calendar ("due 06:00 Mon–Fri, alert 06:15") | ❌ (ledger has the data; nothing watches) | ✅ | ❌ (external) | **High** |
| Event-driven triggering (object-store notification / watcher) | ❌ (pull via spark-submit) | ✅ | ✅ (Auto Loader file notification — its core feature) | Med-High |
| Incremental listing at scale (millions of landed files, checkpointed) | 🟡 (directory listing each run) | ✅ | ✅ (RocksDB checkpoint) | Med |
| Multi-file batch completeness (manifest/control file: N files, M records, control totals) | 🟡 (per-file trailer/sidecar only; FILE_ATOMIC groups but can't *wait*) | ✅ | ❌ | **High** |
| Late/early/partial delivery policies | ❌ | ✅ | ❌ | Med |
| Cross-feed dependencies & backfill orchestration | ❌ (external scheduler assumed) | ✅ (taskflows) | ✅ (Jobs/DLT) | Med |

### 3.3 Transport

| Feature | Ours | Informatica | Databricks | Sev |
|---|---|---|---|---|
| HDFS + cluster-configured object stores (s3a/abfs) | ✅ (Hadoop FS abstraction) | ✅ | ✅ | — |
| Native SFTP/FTPS pull (poll remote, land, lifecycle) | ❌ | ✅ | ❌ (external) | **High** |
| Managed file transfer / B2B gateway integration | ❌ | ✅ | ❌ | Med |
| Cross-region/cloud landing patterns | ❌ | ✅ | 🟡 | Low-Med |

### 3.4 Schema evolution & data quality

| Feature | Ours | Informatica | Databricks | Sev |
|---|---|---|---|---|
| Contract enforcement with BACKWARD check + escaped snapshot | ✅ | ✅ | 🟡 | — |
| Additive evolution mode + **rescue column** (unexpected columns quarantined into a column instead of failing the batch) | ❌ (fail/warn only) | 🟡 | ✅ (`rescuedDataColumn`) | **High** operator pain |
| Schema inference for onboarding (generate contract from sample) | 🟡 (config-gen introspects JDBC, not files) | ✅ | ✅ | Med |
| Per-column rules (regex/length/allowed/null-pct, sampled) | ✅ | ✅ (CDQ richer) | 🟡 (DLT expectations) | — |
| Historical anomaly detection (volume/row-count/null-rate drift vs. ledger history) | ❌ (ledger already stores every count) | ✅ (CLAIRE) | 🟡 (DLT metrics + user alerts) | **High**, cheap for us |
| Cross-field / cross-file rules, reusable rule libraries | ❌ | ✅ | 🟡 | Med |
| DQ scorecards over time | ❌ | ✅ | 🟡 (dashboards) | Med |

### 3.5 Observability, ops & self-service

| Feature | Ours | Informatica | Databricks | Sev |
|---|---|---|---|---|
| Metrics export (Prometheus/CloudWatch/StatsD) | ❌ (JdbcMetrics → log only) | ✅ | ✅ | **High** |
| Lineage emission (OpenLineage / Atlas / Purview) — we *stamp* lineage columns but never *publish* events | ❌ | ✅ (EDC/Axon) | ✅ (Unity Catalog) | Med-High |
| Alerting channels (email/Slack/PagerDuty on quarantine, reject threshold, SLA breach, reconciliation failure) | ❌ | ✅ | ✅ (via jobs/alerts) | **High** |
| Operational console (run search, feed health, quarantine browser) | ❌ (Hive queries by hand) | ✅ | ✅ | High (but large) |
| Self-service onboarding (templates, sample-file sandbox, promotion workflow) | 🟡 (CLI wizard + `--validate-only` are 80% of a sandbox) | ✅ | 🟡 | Med |
| Fleet operation: workload classes, connection budgets, priority | ❌ (perf review F7/F8) | ✅ | ✅ | Med |

### 3.6 Platform & product engineering

| Feature | Ours | Informatica | Databricks | Sev |
|---|---|---|---|---|
| Transactional table format (ACID merge, time travel, O(delta) publish) | ❌ (plan item #19; INSERT OVERWRITE today) | 🟡 (target-dependent) | ✅ (Delta core) | **High** strategic |
| Semver releases, migration notes, compatibility guarantees | ❌ (snapshot tags) | ✅ | ✅ | Med |
| Plugin SPI (custom formats/validators/notifiers without forking) | ❌ (SourceRegistry is close) | ✅ | 🟡 | Med |
| Published docs / API reference beyond repo | 🟡 | ✅ | ✅ | Med |
| Perf benchmarks & sizing guides | ❌ (perf review defines the plan) | ✅ | ✅ | Low-Med |

---

## 4. Phased roadmap

Effort: S ≤ 1 wk · M = 2–4 wk · L = 1–3 mo, single engineer familiar with
the codebase. Every item ships with regression tests and audit/replay parity.

### Phase A — Operational credibility (highest leverage per effort)

| # | Item | Effort | Notes |
|---|---|---|---|
| A1 | **Arrival SLA monitor**: expected-arrival calendar per feed (HOCON), a `--stage sla-check` (or scheduled job) that reads the audit ledger and flags misses | M | Ledger already has every run; this is a read-side feature. Gaps: business-day calendar, holiday list |
| A2 | **Notification hook**: pluggable notifier (SMTP + webhook first; Slack/PagerDuty via webhook) fired on quarantine, reject-threshold, reconciliation FAIL, SLA breach | S-M | One interface + two impls; wire into existing failure paths |
| A3 | **Volume anomaly checks**: per-feed rolling stats from ledger history (row count, reject rate, null-key rate) with configurable deviation thresholds → WARN/FAIL/notify | M | Pure ledger arithmetic; differentiator vs. Databricks, parity move vs. CLAIRE |
| A4 | **Metrics export**: generalize JdbcMetrics to a framework-wide registry with a Prometheus pushgateway/StatsD sink; per-stage durations from the ledger | S-M | Perf review F27 |
| A5 | **Manifest/control-file batch completeness**: sender-declared file count + control totals; intake HOLDs the batch until complete or deadline (reuses HOLD/reject-window machinery from the prompt program) | M | Closes the biggest B2B trust gap |

### Phase B — Format breadth (unlocks feed classes)

| # | Item | Effort | Notes |
|---|---|---|---|
| B1 | **Fixed-width reader** behind the existing contract gate (offsets/lengths in contract columns; header gate becomes layout gate) | M | Prereq for B2 |
| B2 | **COBOL copybook / EBCDIC** (evaluate Cobrix; wrap behind SourceRegistry so the dependency is optional) | M-L | Decide build-vs-integrate explicitly; mainframe estates only |
| B3 | **PGP/GPG decryption at intake** (Bouncy Castle; decrypt into inprogress, checksum the *ciphertext* for idempotency, audit both names) | M | Key management design needed (keystore/HSM/env) |
| B4 | **Zip/archive intake**: one archive → N logical files, each with its own lifecycle row; archive-level completeness | M | |
| B5 | **JSON/XML contract validation**: map contract columns to paths (JSONPath/XPath subset); required-path + type checks pre-RAW | M-L | Extends HDR_* catalog to hierarchical data |
| B6 | **EDI decision**: if 834/837 are in scope, integrate a translator (e.g., smooks/X12 lib) *upstream* of the framework rather than parsing in-line; document the boundary | L (or explicit non-goal) | Don't half-build this |

### Phase C — Evolution & self-service

| # | Item | Effort | Notes |
|---|---|---|---|
| C1 | **Rescue-column evolution mode**: `schema.evolution = FAIL \| ADDITIVE \| RESCUE` — unexpected columns land in a `_rescued` map column, audited, batch proceeds | M | Databricks parity; biggest operator-pain reducer |
| C2 | **File-based contract inference** in config-gen (sample file → draft contract, operator confirms) | S-M | Mirrors the existing JDBC introspection |
| C3 | **Onboarding sandbox**: promote `--validate-only` + `--explain-mapping` into a documented "certify a sample file" workflow with exit codes for CI | S | Mostly docs + polish |
| C4 | **Feed templates**: blessed HOCON templates per pattern (daily full file, incremental file, multi-file batch) shipped with config-gen | S | |

### Phase D — Strategic platform

| # | Item | Effort | Notes |
|---|---|---|---|
| D1 | **Delta/Iceberg publish** (plan item #19): O(delta) MERGE, time travel, removes the double-write and the shrink-guard read (perf F1 endgame) | L | Gated on target-technology decision — *the* strategic fork |
| D2 | **Event-driven triggering**: object-store notification / long-poll watcher daemon invoking the pipeline; checkpointed incremental listing | L | After D1 preferably; Auto Loader parity |
| D3 | **Operational console**: thin web UI over the audit ledger (run search, feed health, quarantine browser, SLA board) | L | Read-only first; the ledger schema is already the API |
| D4 | **Lineage emission**: OpenLineage events at stage boundaries (we already carry run/source/window lineage on every row) | M | Purview/Atlas compatible |
| D5 | **Productization**: semver + release notes, plugin SPI (formalize SourceRegistry/notifier/validator extension points), published docs site, perf benchmark suite (perf review §7) | M-L | Continuous |

### Sequencing rationale

- Phase A is deliberately first: it converts existing ledger data into
  visible operational value in weeks, which is what stakeholders compare
  against commercial consoles.
- B1→B2 and B3 unlock feed classes that currently *cannot* be onboarded at
  all; everything else improves feeds that already work.
- D1 (table format) is the only item that changes the architecture; every
  other item layers onto the current design. It should be decided, not
  drifted into.

---

## 5. Explicit non-goals (unless demanded)

- Building a GUI mapping designer (Informatica's moat; config-as-code is
  our model — invest in templates and inference instead).
- In-line EDI parsing (integrate a translator at the boundary, B6).
- Generic streaming ingestion (Kafka module exists separately; file
  micro-batching via D2 covers the file-latency need).
- Multi-engine portability (the framework is Spark-native by design).
