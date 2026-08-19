# Embedding the Framework in an Application (Control Plane)

**Status:** design. §6's feed service API is now PROTOTYPED and tested
(`confgen.service.FeedService`); everything else — store, submit, UI — is
not built. Where this document and the code disagree, the code wins.

The framework today is a batch job configured by HOCON files on disk. This
describes how an application can own feed definitions, configuration and
run history around it — turning a per-feed deployment into a managed
onboarding surface — **without changing what a run is**.

The core claim: most of what a control plane needs already exists as
library code. What is missing is a store, an API surface and a UI, not
ingestion logic.

---

## 1. The decision that shapes everything else

"Embed the framework in an application" has two readings, and they cost
very different amounts.

| | **A — Control plane (recommended)** | **B — In-process runner** |
|---|---|---|
| The app | stores feeds, validates, renders config, submits runs, reads history | holds a SparkSession and executes feeds on request |
| A run is | a `spark-submit` process, as today | a method call in the app's JVM |
| Concurrency | whatever YARN allows | ~1 (one SparkSession per JVM) |
| Blast radius of a bad feed | one container | the whole service |
| Cluster mode | unchanged (`--files` ships a real file) | fights it — config lives in a heap, not a file |
| Work required | store + API + UI | the above, plus removing process-global state |

**Take A.** B trades the framework's most valuable property — every run is
an isolated, restartable process that reports a classified exit code — for
an API that only looks tidier. It also inherits every constraint in §4.

Everything below assumes A.

---

## 2. What already exists

This is the part worth internalising before designing anything: the seams
are already cut.

**The framework's entry point is a `Config` object, not a file path.**

```scala
final class IngestPipeline(spark: SparkSession, feedConf: Config, cli: Cli, logger: Logger)
```

`IngestMain` is a thin CLI shell over that class. Anything able to produce a
`Config` can drive the framework — the file on disk is a transport detail,
not an assumption baked into the pipeline.

**Feed configuration can already be built programmatically.**
`ingestion-config-gen` is not a script that prints text; it assembles typed
configuration and hands back objects:

| Existing API | Returns | Role in a control plane |
|---|---|---|
| `ConfigAssembler.assemble(flow, answers)` | `Config` | render a stored feed definition |
| `ConfigAssembler.wrapAsFeeds(feed)` | `Config` | nest under `feeds.<entity>` |
| `DryRunValidator.validate(feed)` | `Report(errors, warnings, sqlPreview)` | server-side validation on save |
| `FeedCompatibilityValidator.validate(feed)` | `Seq[String]` | the `CFG_001`–`CFG_019` matrix |
| `AuditService.fingerprint(conf)` | `String` | detect that a feed changed |

`DryRunValidator` validates **through the framework's own parsers** rather
than reimplementing rules, and substitutes secret placeholders first so it
works where vaults are unreachable. A feed that passes it is a feed the
pipeline accepts. That property is what makes server-side validation
trustworthy, and it must be preserved: the control plane never grows its
own copy of a validation rule.

**Run history already has a system of record.** The run ledger holds stage
status, counts, `config_fingerprint`, provenance and the extract window;
`AuditService.pendingBatches(entity)` and `hasStageSuccess(...)` answer
batch-state questions; `BatchControl` projects one row per batch. A portal
reads these. Do not build a second run store.

What `ingestion-config-gen` lacks is a **UI other than a terminal wizard**
and a **store other than a file**. Not logic.

---

## 3. Target architecture

```
┌─────────────────────────── Application ───────────────────────────┐
│                                                                    │
│  Feed store        Feed API                UI / API consumers      │
│  (versioned    ──▶ render → validate  ◀──  authoring, approval,    │
│   definitions)     → fingerprint            run history            │
│         │                                                          │
│         │ render + write                                           │
│         ▼                                                          │
│  Config artifact  ──▶  submit (spark-submit)  ──▶  exit code       │
│  (HOCON on disk)                                    10/20/30       │
└────────────────────────────────┬───────────────────────────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │   The framework, as-is    │
                    │   IngestPipeline, sources │
                    │   sinks, ledger, locks    │
                    └────────────┬─────────────┘
                                 │ writes
                    ┌────────────▼─────────────┐
                    │  Hive: raw, curated,      │
                    │  run ledger, rejects      │  ◀── the app READS these
                    └───────────────────────────┘
```

Five responsibilities, in order:

1. **Store** feed definitions as structured, **versioned** records. A feed
   change is a new version, never an edit — the ledger's
   `config_fingerprint` is only useful if the definition behind it is
   immutable and addressable.
2. **Render** a version to HOCON. See §5 for why HOCON stays.
3. **Validate** server-side on save, via `DryRunValidator` (which already
   subsumes the CFG_* matrix — see §6). This is the single largest win
   available: it moves `CFG_*` failures from 3am to the moment someone
   clicks Save.
4. **Submit**: write the rendered config to a staging directory,
   `spark-submit`, capture the exit code — `10` transient (retry is
   worthwhile), `20` data integrity (never retry), `30` configuration,
   `1` unclassified. These codes exist precisely so an orchestrator can
   decide without parsing logs.
5. **Observe** from the ledger. Nothing new to build.

---

## 4. Constraints, verified against the code

Facts confirmed while writing this, so they are not rediscovered later.

**Blocks in-process execution (Option B only):**

- `IngestMain.scala` calls `System.exit` after a classified failure.
  Correct for a CLI; fatal in a host process. It must never sit on an
  embedded path.
- `OverrideContext` is a process-global singleton with `@volatile` state,
  deliberately scoped "one driver JVM is one run". Two feeds sharing a JVM
  break that assumption.
- One SparkSession per JVM, realistically — so an in-process runner is
  single-flight regardless of how the API is shaped.

**Checked and *not* a problem:**

- `JdbcSource` / `KafkaSource` read windows are keyed by `entity|runId` and
  are explicitly safe for concurrent runs of one entity.
- `AuditService` extract-window state is instance-level, not global.
- `SourceRegistry` / `SinkRegistry` are idempotent register-once maps.

Under Option A none of the first group matters: each run is its own
process, which is exactly the isolation they assume.

---

## 5. Decisions and their reasons

### HOCON stays the wire format

Render stored definitions **to HOCON**; do not invent a second format.

Every validator, every `CFG_*`/`CUR_*`/`HDR_*` message, every runbook
procedure and every example already speaks HOCON. A second format means two
implementations of the same rules, and the moment they drift the error
messages start lying. YARN cluster mode also needs a real file shipped with
`--files`, so a file has to exist regardless.

### The framework must not fetch its own config at runtime

The obvious design is a `ConfigProvider` that calls the application's API
from `loadBaseConfig`. **Do not build it.**

- It makes every batch run depend on the application being reachable,
  converting a control-plane outage into a pipeline outage.
- It destroys reproducibility: you can no longer determine what
  configuration a past run used by inspecting an artifact.
- It gains nothing. Rendering to a file at submit time is strictly more
  robust and already supported.

Render and ship. The config that ran is a file that existed, and the
ledger's `config_fingerprint` ties the run to it.

### The application submits; it does not schedule

Control-M (or whatever schedules today) keeps owning *when*. The
application owns *what* and *whether it is valid*. Absorbing scheduling
means reimplementing calendars, dependencies, retry policy and on-call
integration that already exist and work.

### Validation is never duplicated

The control plane calls `FeedCompatibilityValidator` and `DryRunValidator`.
It never encodes a rule of its own. If a rule needs to exist, it belongs in
the framework where the pipeline enforces it — otherwise the app can bless
a feed the pipeline rejects, which is worse than no validation at all.

---

## 6. The first thing to build

Not the UI. Extract a small, tested **feed service API** from
`ingestion-config-gen`, with no dependency on a terminal, a wizard or a
file:

```scala
render(definition: FeedDefinition): Config        // stored record -> config
validate(feed: Config): ValidationReport          // DryRunValidator (see below)
fingerprint(feed: Config): String                 // ledger's recipe
write(entity: String, feed: Config, dir: Path)    // artifacts to ship
```

**Built:** `com.hcsc.generic.ingest.confgen.service.FeedService`, with
`FeedDefinition(entity, sourceType, settings)` as the storable record —
the same ordered answer set drafts already round-trip, so the model gains a
store without gaining a second definition of what a feed is.

Two things the prototype settled that this document originally got wrong or
left implicit:

- **Validation is one call, not two.** `DryRunValidator` already invokes
  `FeedCompatibilityValidator` internally for the CFG_* matrix. Calling both
  — the obvious reading of "merged" — reports every CFG_* twice. There is a
  test pinning this, because the mistake is invisible on any feed that
  happens to have zero CFG errors.
- **`write` must refuse to ship what it did not validate.** Rendering and
  parsing are separate code paths, so the written file is re-parsed and
  compared to the validated configuration; a mismatch raises CFG_020 rather
  than producing a deployable artifact. A control plane that validates X and
  ships Y is worse than one that does not validate, because its report says
  the feed is safe.

The tested guarantee is **fidelity**: a feed rendered headlessly from a
stored definition is byte-identical to what the interactive generator
produces from the same answers. Without that the wizard's test suite stops
being evidence about the service.

Everything else — store, UI, approval workflow, scheduler integration —
sits on top of that and can change without touching the framework. It is
also independently useful before any UI exists: CI can validate every feed
definition in a repository on every commit.

Sequence after that: store and versioning → validation on save → submit and
exit-code capture → history views over the ledger → authoring UI.

---

## 7. Risks and open questions

**The override layer becomes a governance hole.** `--override-path` is a
deliberate operator escape hatch: values beat the deployed feed, logged
loudly, marked in `config_fingerprint` with `+ovr:`. Once an application
owns configuration, an unversioned file that silently beats the managed
feed is precisely the drift the application exists to eliminate. Either
surface overrides *in* the application (visible, attributed, expiring) or
disable the flag in managed deployments. Do not leave both paths live and
unreconciled.

**Control-table multi-tenancy is hard to reverse.** Feeds currently share a
database with explicitly named control tables. Decide *before* onboarding
many feeds whether they share one ledger or get their own — once there is
history, changing this means migrating it.

**Secrets must not enter the store.** Feed definitions reference secrets by
provider and key; the store holds references, never values.
`DryRunValidator`'s placeholder substitution already assumes this. A feed
store that accumulates credentials is a breach waiting for an audit.

**Resolved in §9:** how feed *versions* map to runs. Not through
`config_fingerprint` — it hashes key structure and collides on value-only
edits (demonstrated by a test in `FeedServiceTest`, so it fails in CI
rather than surprising an auditor). The join is `run_id`, which the caller
pins via `--run-id`, recorded in `feed_submission`. The fingerprint keeps
the narrower job of corroboration.

**Open:**
whether approval workflow belongs in the app or the existing change
process; whether schema contracts are authored in the app or continue to
be included files.

---

## 8. What NOT to build

- **A second run store.** The ledger is the system of record.
- **A second configuration format.** See §5.
- **A runtime config service the framework calls.** See §5.
- **A scheduler.** See §5.
- **A reimplementation of any validation rule.** See §5.
- **An in-process multi-feed runner** unless §4's global state is removed
  first and single-flight execution is acceptable.

---

## 9. Feed store schema

Sketch, not built. Four tables. The reasoning matters more than the column
names, so each table states what it exists to make possible.

### The join key is `run_id`, not `config_fingerprint`

This dissolves §7's open question rather than answering it.

The run ledger has no feed-version column, and adding one would change the
framework. It does not need one: **`--run-id` is caller-supplied** —
`IngestPipeline` generates a UUID only when the CLI omits it — and the
ledger keys every row by it. So the control plane pins the run id at submit
time and records `run_id -> version_id` in its own table. One join, no
framework change, no reliance on a digest that cannot tell two versions
apart.

`config_fingerprint` keeps a narrower and better job: **corroboration**. If
the ledger's fingerprint for a run does not match what the store recorded
for that version, something bypassed the control plane or an override
applied. That is a reconciliation query, not an identity.

### Tables

**`feed`** — stable identity. One row per entity, forever.

```
feed_id          PK
entity           the framework's key everywhere: lock, ledger, RAW lineage,
                 table names. Unique within environment. Never edited.
environment      dev | test | prod
source_type      jdbc | file | kafka
created_ts, created_by
```

**`feed_version`** — the immutable definition. A change is a new row.

```
version_id       PK  <- the identity config_fingerprint cannot provide
feed_id          FK
version_no       monotonic per feed
settings         the ordered answer set (see encoding below) — the EDITABLE source
rendered_hocon   the exact artifact that ships     <- see "store the artifact"
rendered_schema  the split schema contract, when the feed has one
rendered_sha256  content hash of rendered_hocon    <- the real change detector
config_fingerprint  the framework's digest, for corroboration only
status           DRAFT | APPROVED | RETIRED
validation_status, validation_errors, validation_warnings, sql_preview
                 cached from FeedService.validate at save time
created_ts, created_by, approved_ts, approved_by, change_ref
```

**`feed_submission`** — the join to the ledger, and the reason this design
needs no framework change.

```
run_id           PK  <- passed to the framework as --run-id
version_id       FK
mode, stage, extra_args
artifact_path    where rendered_hocon was written for this run
submitted_ts, submitted_by
override_applied BOOLEAN, override_digest     <- cross-checks the ledger's +ovr:
exit_code        0 | 10 transient | 20 data integrity | 30 config | 1 unclassified
failure_class, finished_ts
```

**`feed_promotion`** — optional, for traceability across environments.

```
from_version_id, to_version_id, promoted_ts, promoted_by, change_ref
```

### Decisions

**Store the rendered artifact, not only the definition.** Render logic
lives in the framework and changes with it, so re-rendering a two-year-old
`settings` row under today's code may not reproduce what ran. Keeping
`rendered_hocon` makes a past run reproducible by construction; keeping
`settings` keeps it editable. Both, not either.

**`rendered_sha256` is the change detector.** `config_fingerprint` hashes
key structure only and collides on value-only edits — pinned by a test in
`FeedServiceTest`, so the limitation cannot quietly disappear. Never branch
on the fingerprint to decide whether a feed changed.

**Encode `settings` the way drafts already do.** `Drafts.save` serializes
each answer as `{id, scalar | items | blocks}`, with blocks unwrapped from
`Config`. That encoding round-trips today and handles the awkward case
(structured blocks like schema columns and filters). Reuse it rather than
inventing a JSON mapping that has to be kept in step with `AnswerValue`.

**Do not span environments with one version.** `entity` is load-bearing —
lock key, ledger key, stamped on every RAW row, and embedded in table
names. Today `smartiq_pdp` and `smartiq_pdp_e2e` are separate entities in
separate files, which is correct. Making one definition render into many
environments turns `entity` into a derived value and quietly changes the
meaning of every lock and ledger row. Keep feeds per environment and link
them with `feed_promotion`.

**Relational, not Hive.** The store needs updates, uniqueness constraints
and transactions. Hive is append-only and is the wrong tool — which is also
why this does not live beside the control tables.

**Secrets never enter the store.** Feeds already reference them
(`password = { provider = "env", key = "SMARTIQ_DB_PASSWORD" }`), so
holding the definition holds only the reference. Reject any version whose
rendered artifact contains an inline secret at save time; the store must
not become a credential archive that an audit discovers.

### What this makes answerable

| Question | Query |
|---|---|
| What configuration produced this curated row? | RAW `run_id` -> `feed_submission` -> `feed_version.rendered_hocon` |
| What changed between two runs? | diff `rendered_hocon` of their two versions |
| Did anything run outside the control plane? | ledger `run_id`s with no `feed_submission` row |
| Was an unmanaged override in effect? | ledger `config_fingerprint LIKE '%+ovr:%'` vs `submission.override_applied` |
| Which feeds are running unapproved definitions? | `feed_submission` joined to `feed_version.status != APPROVED` |

The last two are the governance holes §7 names. Neither is solvable by the
framework alone; both fall out of this schema for free.

### Retention

Submissions accumulate and can be trimmed on the same policy as the ledger.
**Versions must never be purged while a submission references them** —
that is exactly the reproducibility the store exists to provide.

---

## Related

- [CONTROL_PLANE_IMPLEMENTATION_PLAN.md](../reports/CONTROL_PLANE_IMPLEMENTATION_PLAN.md)
  — the phased implementation of this design: contracts C1–C5, store DDL,
  test strategy, risks

- [ARCHITECTURE.md](ARCHITECTURE.md) — module map and extension points
- [CONFIGURATION_MODEL.md](CONFIGURATION_MODEL.md) — the config surface this
  renders to, and the `CFG_*` matrix validation enforces
- [DECOUPLING_DESIGN.md](DECOUPLING_DESIGN.md) — the ledger-as-checkpoint
  reasoning this reuses for run history
- [CONFIG_GENERATOR.md](../development/CONFIG_GENERATOR.md) — what
  `ingestion-config-gen` produces today
