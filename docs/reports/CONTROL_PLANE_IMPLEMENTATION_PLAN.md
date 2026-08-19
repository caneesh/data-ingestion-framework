# Control Plane — Implementation Plan

**Date:** 2026-08-19
**Input:** [CONTROL_PLANE_DESIGN.md](../architecture/CONTROL_PLANE_DESIGN.md)
(the architecture and its rationale) and the prototyped
`confgen.service.FeedService` (§6 there, built and tested).
**Scope:** how another application uses the framework — the concrete
contracts, dependency mechanics, phasing and risks. Rationale lives in the
design doc and is not repeated here.

---

## 1. The two supported embedding modes

| Mode | What the app embeds | Where it runs |
|---|---|---|
| **Authoring** | `FeedService` as a library: render, validate, fingerprint, write | the app's JVM |
| **Execution** | nothing — the app *invokes* a run (`spark-submit` → `IngestMain`) and reads results from the ledger | a separate process, as today |

In-process pipeline execution is **not** a supported mode
(design doc §1/§4: `System.exit`, `OverrideContext`, one SparkSession per
JVM). Any request for it gets the execution mode instead.

## 2. Dependency mechanics (verified facts)

- The app depends on **`ingestion-config-gen`**, which pulls
  `ingestion-core` and `ingestion-jdbc` only. The file and kafka question
  flows live in config-gen itself — assembling and validating those feeds
  needs no further modules.
- **The host must add `spark-sql` explicitly.** Spark is `provided`-scope
  in the reactor, and the authoring path genuinely reaches it at runtime:
  `SchemaContract` (line 291) and `FeedCompatibilityValidator` (line 151)
  both call `org.apache.spark.sql.types.DataType.fromDDL` to validate
  declared types. This is pure parsing — no SparkSession, no cluster — but
  the classes must be on the classpath or contract validation dies with
  `NoClassDefFoundError`. Do not "fix" this by re-implementing DDL type
  parsing in the app; that is a validation rule, and duplicated validation
  is the one thing the design forbids.
- The host is therefore a **JVM application, Scala 2.12-binary-compatible,
  Java 11+**. A non-JVM stack gets the same surface via a thin HTTP wrapper
  (deferred — see §6).
- There is no artifact repository today (the zip workflow). Until one
  exists, the app builds against the reactor's locally-installed jars
  (`mvn install`), same as the modules do among themselves.

## 3. The five contracts

### C1 — Authoring (exists)

`FeedDefinition(entity, sourceType, settings)` in, artifacts out:

```scala
val (feed, report) = FeedService.renderAndValidate(definition)
// report.ok gates the save; report.sqlPreview is shown to the author
val fp = FeedService.fingerprint(feed)          // corroboration only
val paths = FeedService.write(entity, feed, dir) // schema + feed, one dir
```

Store `report` results on the version row (design §9). `write` already
refuses (CFG_020) to emit an artifact that does not re-parse to what was
validated — the app never bypasses it with its own file writing.

### C2 — Artifact placement

`write` emits the feed file and, when present, the schema file it
`include`s **by relative name** — both must land in one directory, and
that directory is what `--files` ships. Distribution decision (design
brainstorm): **the submission runner runs on an edge node**, so "placement"
is a local write followed by the standard submit. Formalizing a push/pull
artifact channel is deferred until a second submission host exists.

### C3 — Submission

1. App generates `run_id` (UUID) and **inserts the `feed_submission` row
   first** — a crash after this point still leaves the run attributable,
   and bypass detection stays sound.
2. App invokes the **existing** `run_ingest.sh` with the `INGEST_*`
   environment, passing `--run-id`. It does not rebuild the spark-submit
   command: the wrapper's list-cleaning, jar guards and `--files` assembly
   encode fixes for real failures and must not be forked.
3. Outcome capture — **the ledger is the truth, the exit code is a hint.**
   Exit codes (0 / 10 transient / 20 data-integrity / 30 config / 1)
   propagate only in **client mode**; in YARN cluster mode spark-submit
   reports the final application state, not the driver's code. So the
   runner records the exit code when meaningful, then confirms terminal
   stage status from the run ledger (`hasStageSuccess` semantics), which is
   correct in both modes. The `[Outcome] class=... exitCode=...` driver log
   line exists for exactly this and is the fallback where ledger access
   lags.

### C4 — Observation

The app reads the ledger over **Hive JDBC (HiveServer2), read-only
credentials** — no Spark needed for reads. The queries are the table in
design §9: config-of-this-row, diff-between-runs, bypass detection
(ledger `run_id`s with no submission row), override reconciliation
(`config_fingerprint LIKE '%+ovr:%'` vs `override_applied`), unapproved
versions.

### C5 — Store

Design §9's four tables, concretely relational (Postgres assumed; any
RDBMS with transactions works):

```sql
CREATE TABLE feed (
  feed_id        BIGSERIAL PRIMARY KEY,
  entity         TEXT NOT NULL,
  environment    TEXT NOT NULL,
  source_type    TEXT NOT NULL CHECK (source_type IN ('jdbc','file','kafka')),
  created_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by     TEXT NOT NULL,
  UNIQUE (entity, environment)
);

CREATE TABLE feed_version (
  version_id          BIGSERIAL PRIMARY KEY,
  feed_id             BIGINT NOT NULL REFERENCES feed,
  version_no          INT    NOT NULL,
  settings            TEXT   NOT NULL,  -- drafts encoding: {id, scalar|items|blocks}
  rendered_hocon      TEXT   NOT NULL,
  rendered_schema     TEXT,
  rendered_sha256     TEXT   NOT NULL,  -- THE change detector
  config_fingerprint  TEXT   NOT NULL,  -- corroboration only (collides on values)
  status              TEXT   NOT NULL DEFAULT 'DRAFT'
                      CHECK (status IN ('DRAFT','APPROVED','RETIRED')),
  validation_ok       BOOLEAN NOT NULL,
  validation_errors   TEXT,
  validation_warnings TEXT,
  sql_preview         TEXT,
  created_ts   TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by   TEXT NOT NULL,
  approved_ts  TIMESTAMPTZ,
  approved_by  TEXT,
  change_ref   TEXT,
  UNIQUE (feed_id, version_no)
);

CREATE TABLE feed_submission (
  run_id            TEXT PRIMARY KEY,     -- passed as --run-id; the ledger join
  version_id        BIGINT NOT NULL REFERENCES feed_version ON DELETE RESTRICT,
  mode              TEXT NOT NULL,
  stage             TEXT NOT NULL,
  extra_args        TEXT,
  artifact_path     TEXT NOT NULL,
  submitted_ts      TIMESTAMPTZ NOT NULL DEFAULT now(),
  submitted_by      TEXT NOT NULL,
  override_applied  BOOLEAN NOT NULL DEFAULT FALSE,
  override_digest   TEXT,
  exit_code         INT,
  failure_class     TEXT,
  finished_ts       TIMESTAMPTZ
);

CREATE TABLE feed_promotion (
  from_version_id BIGINT NOT NULL REFERENCES feed_version,
  to_version_id   BIGINT NOT NULL REFERENCES feed_version,
  promoted_ts     TIMESTAMPTZ NOT NULL DEFAULT now(),
  promoted_by     TEXT NOT NULL,
  change_ref      TEXT,
  PRIMARY KEY (from_version_id, to_version_id)
);
```

`ON DELETE RESTRICT` on submissions is the reproducibility guarantee in
schema form: a referenced version cannot be purged. Save-time rule (not a
column): reject any version whose `rendered_hocon` contains an inline
secret value rather than a `{provider, key}` reference.

## 4. Phases

| Phase | Delivers | Exit criterion | Size |
|---|---|---|---|
| **P0** (done) | `FeedService` + 10 tests | in `main` | — |
| **P1** | `feed-lint`: a headless CLI over `FeedService.validate` for CI — validates every stored definition / example feed on each commit | wired into `.github/workflows/build.yml`; a deliberately broken feed fails the build | S |
| **P2** | Store + service layer: C5 DDL, DAO, version immutability, save-time validation caching, inline-secret rejection | a definition survives store → load → render byte-identically (fidelity test against P0) | M |
| **P3** | Submission runner on an edge node: C2 + C3, submission-row-first ordering, ledger-confirmed outcomes | an e2e feed runs end-to-end from the store with `run_id` visible in both `feed_submission` and the ledger | M |
| **P4** | Observation + reconciliation: C4 Hive-JDBC reads, bypass report, override report, unapproved-version report | the reports run against the real ledger and a seeded violation is caught | S–M |
| **P5** | UI and approval flow | org stack; out of framework scope | — |

P1 before P2 deliberately: it delivers the single largest win in the design
(`CFG_*` failures move from 3am to commit time) with no store, no app and
no new infrastructure.

## 5. Framework changes required

**None are mandatory.** Verified during design: `--run-id` is already
caller-supplied; `FeedService` needs no new framework surface; the ledger
needs no new column (the join is `run_id`).

Deliberately deferred, each a separate decision:

- `override_expires` — self-expiring break-glass override (separate
  discussion; keep the override as break-glass either way).
- HTTP wrapper module for non-JVM hosts.
- Publishing reactor artifacts to a repository manager (removes the
  local-install coupling in §2).

## 6. Test strategy

- **Fidelity** (exists): headless render ≡ wizard render, byte-identical.
- **Golden contract test** (P3): an artifact rendered from a stored
  definition is executed by the real `IngestMain` in local mode against the
  H2/file fixtures the integration specs already use. This is the test that
  keeps "what the app ships" and "what the pipeline runs" the same thing.
- **Store invariants** (P2): version immutability (no UPDATE path),
  RESTRICT actually restricts, secret-reference rule rejects an inline
  password.
- **Reconciliation** (P4): seeded ledger with one bypassed run and one
  overridden run; both reports catch exactly them.

## 7. Risks

| Risk | Reality | Mitigation |
|---|---|---|
| Spark on the app classpath | `DataType.fromDDL` is reached at runtime (verified, §2) | add `spark-sql` to the app; parsing only, no session |
| Exit codes in cluster mode | spark-submit reports app state, not driver code | ledger is the outcome of record (C3); code is advisory |
| Edge-node auth | submission runner needs kerberos/cluster access like any operator | run it where operators run today; no new auth surface |
| Store availability | store down ⇒ no *new* submissions | acceptable by design — running jobs and scheduled Control-M jobs are unaffected; the framework never calls the app (design §5) |
| Host stack constraint | JVM / Scala 2.12 binary compat | HTTP wrapper if and when a non-JVM host appears |

## 8. Out of scope

Everything in design §8 (no second run store, no second config format, no
runtime config service, no scheduler, no duplicated validation, no
in-process runner) — plus, for this plan: UI technology choice, approval
policy content (who may approve is organizational), and multi-cluster
artifact distribution.
