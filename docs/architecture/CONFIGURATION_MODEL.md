# Configuration Model

One feed = one HOCON block selecting a strategy per pipeline stage. Every
section is optional except `source` and `raw`; omitted sections keep the
legacy defaults. Cross-section compatibility is validated at startup by
`FeedCompatibilityValidator` (CFG_001..CFG_008) and by the config
generator's dry run, so jointly-invalid combinations fail before Spark
starts.

## Full example (JDBC incremental, Type 1 curated)

```hocon
feeds.claims {
  source {
    type = "jdbc"
    url = "jdbc:sqlserver://myserver.database.windows.net:1433;databaseName=ClaimsDB"
    auth {
      user     = { provider = "cyberark", url = "https://ccp.example.com",
                   app_id = "APP_Ingest", safe = "AAM_DB", object = "sql-svc",
                   attribute = "UserName" }
      password = { provider = "cyberark", url = "https://ccp.example.com",
                   app_id = "APP_Ingest", safe = "AAM_DB", object = "sql-svc" }
    }

    # Extraction strategy (jdbc only — CFG_001)
    extraction {
      strategy = "TIMESTAMP_KEY"       # FULL_SNAPSHOT | TIMESTAMP | TIMESTAMP_KEY
                                       # | INCREASING_KEY | PARTITION_RANGE
      table = "dbo.claims"
      boundary {                       # required for incremental strategies (CFG_002)
        columns = [ { name = "modified_ts", type = "TIMESTAMP" },
                    { name = "claim_id",    type = "NUMERIC" } ]
        initial = "1900-01-01 00:00:00|0"
        overlap = 300                  # forbidden on FULL_SNAPSHOT (CFG_003)
      }
      partition { column = "claim_id", num_partitions = 8 }
    }

    # Checkpoint persistence: hive (default) | jdbc | memory (tests)
    checkpoint {
      store = "jdbc"
      url   = "jdbc:sqlserver://control-db;databaseName=ingest"
      table = "ingest_checkpoints"
      retry { max_attempts = 3, backoff_ms = 1000 }
    }
  }

  schema {                             # validation: contract + content rules
    version = "1.0"
    columns = [ { name = "claim_id", type = "string", nullable = false },
                { name = "amount",   type = "string" } ]
  }

  raw {
    strategy = "APPEND_BATCH"          # APPEND_BATCH | SNAPSHOT | CDC_EVENTS
    database = "claims_raw"
    table = "claims"
    partitioning { keys = ["ingest_dt"]
      derive { ingest_dt = "date_format(load_timestamp, 'yyyy-MM-dd')" } }
  }

  rejects {
    database = "ingest_audit"
    use_contract_nullability = true    # requires the schema block (CFG_006)
    max_reject_percent = 5.0
  }

  curated {
    strategy = "TYPE1_MERGE"           # APPEND | TYPE1_MERGE | SNAPSHOT_REPLACE
    database = "claims_curated"
    table = "claims"
    merge { keys = ["claim_id"] }      # required by TYPE1_MERGE (CFG_005)
    dedup { order_by = ["modified_ts"] }
    publish { validation_query = "SELECT * FROM {table} WHERE claim_id IS NULL" }
  }

  audit {
    enabled = true                     # required when reconciliation FAILs (CFG_007)
    database = "ingest_audit"          # ANY database the job can write to; the
                                       # name is not special. Point it at an
                                       # existing shared database when the site
                                       # cannot create one — the framework
                                       # issues CREATE DATABASE only when the
                                       # database is genuinely absent. Table
                                       # names are configurable too
                                       # (run_table, file_table,
                                       # reconciliation_table, header_table);
                                       # state them explicitly when the
                                       # database is shared. See ../operations/DEPLOYMENT.md
                                       # "Control tables in a shared database".
    reconciliation {
      on_mismatch = "FAIL"             # WARN | FAIL | FAIL_ON equations
      tolerance { raw_equals_accepted = { percent = 0.0 } }
    }
  }

  notifications {                      # absent = silent (existing feeds unchanged)
    enabled = true
    on      = ["FAILURE"]              # FAILURE (default) | SUCCESS
    webhook { url = "https://hooks.example.com/ingest", timeout_ms = 5000 }
    command = "/opt/ingest/notify.sh"  # receives outcome/entity/run/stage/message as ARGV
  }
}
```

## Incompatibility matrix (enforced)

| Code | Combination rejected |
|------|----------------------|
| CFG_001 | `extraction` on a non-jdbc source |
| CFG_002 | incremental extraction strategy without `boundary` |
| CFG_003 | `FULL_SNAPSHOT` with `boundary.overlap` |
| CFG_004 | `raw.strategy = CDC_EVENTS` without a keyed curated merge |
| CFG_005 | `TYPE1_MERGE` without `merge.keys` |
| CFG_006 | `rejects.use_contract_nullability` without a `schema` block |
| CFG_007 | `reconciliation.on_mismatch = FAIL` with audit disabled |
| CFG_008 | JDBC `source.incremental` watermarks on a file source |
| CFG_009 | incremental feed into a keyless state-deriving curated layer |
| CFG_010 | ingestion pattern declaring an unsupported capability |
| CFG_011 | ingestion pattern contradicting the feed configuration |
| CFG_012 | BACKFILL / RAW_REPLAY combined with `watermark.advance_after = RAW` |
| CFG_013 | invalid `raw.delivery_mode`, or DEDUPLICATED_APPEND without a source-version identity |
| CFG_014 | FULL_SNAPSHOT_ABSENCE with source-side filtering / without `confirm_complete_extract` / unimplemented delete capabilities |
| CFG_015 | invalid `curated.pending.on_failure` or negative `max_batches` |
| CFG_016 | `ingestion.execution = DECOUPLED` without `watermark.advance_after = RAW` (incremental sources) or without the run ledger; DECOUPLED feed invoked with `--stage all` |
| CFG_017 | `freshness.compare_as` not a valid Spark type; `compare_as` and `compare_format` both declared; unparseable `tie_breakers` / `dedup.order_by` ordering syntax |
| CFG_018 | `--conf-path` names a file that does not exist (typically: not shipped with `--files`) |
| CFG_021 | `audit.reconciliation.min_accepted_rows` is negative, or set with `audit.enabled = false` (a floor that can never trip reads as protection while detecting nothing) |
| CUR_010 | `curated.merge.normalize` targets a column absent from the incoming data (skipping it would leave business keys un-normalized and insert duplicates instead of merging) |
| CFG_019 | `--override-path` names a file that does not exist (fail-closed: an override that silently did not apply is worse than a failed run) |

Self-healing knobs: `concurrency.stale_heartbeat_intervals` (default 3; 0 disables crashed-holder takeover).

## Logical comparison types

Physical storage keeps whatever type the user chooses (e.g. an all-string
layout); the configuration declares the type every COMPARISON uses:

```hocon
curated.merge.freshness {
  column         = "last_modified"
  compare_as     = "timestamp"      # bare cast, comparison only — OR:
  # compare_format = "M/d/yyyy"     # datetime parse pattern (mutually exclusive)
  tie_breakers   = ["seq desc as bigint"]   # 'as <type>' per entry
}
curated.dedup { order_by = ["batch_no asc as bigint"] }  # same syntax
```

The declared type governs the merge contest, the same-hash version-advance
check and in-batch dedup uniformly. A value the logical type cannot parse
fails the run (CUR_008) — it never silently becomes a NULL that loses
every contest. `column_types` remains the separate option that CHANGES the
stored type.

## Execution shape (Control-M)

```hocon
ingestion {
  execution = COUPLED          # default: one job runs raw then curated
  # execution = DECOUPLED      # two jobs: --stage raw / --stage curated --pending
}
curated {
  pending {                    # decoupled curated driver knobs
    max_batches = 0            # 0 = unlimited per pass
    on_failure  = "STOP"       # STOP | CONTINUE (PIPE_005 summarizes either way)
  }
}
```

DECOUPLED requires `watermark { advance_after = RAW }` for incremental
sources and the run ledger (`audit.database`) — the ledger is the curated
job's batch checkpoint. See DECOUPLING_DESIGN.md and the runbook's
Control-M section; wrapper scripts live in `scripts/`.

CDC-events example and a file-feed example live in `application.conf`'s
commented blocks; the interactive generator (`ingestion-config-gen`)
produces feeds that pass this validator by construction.


## Reject thresholds are UNBOUNDED unless set

`rejects.max_reject_percent` and `rejects.max_reject_count` are both
optional, and absent means **no ceiling**. A feed can reject every row and
still report SUCCESS — curated simply does not advance, and nothing alerts.

```hocon
rejects {
  max_reject_percent = 1.0    # systemic breakage trips this
  max_reject_count   = 100    # small-batch guard: 1% of 20 rows is 0.2 rows
}
```

Set both. Percent alone never fires on small windows; count alone scales
badly as volume grows. Rows still land in the reject table below the
ceiling — the threshold is the alarm, not the acceptance test.

## Detecting the run that did nothing (`min_accepted_rows`)

Every reconciliation check except this one is an **identity** — it proves
no rows vanished between two stages. Identities pass trivially on an empty
batch: `0 = 0 + 0`, and zero curated rows account for zero accepted rows.
So a run that extracted nothing is, to every other check, perfectly
healthy, and the first person to notice is the consumer.

```hocon
audit.reconciliation.min_accepted_rows = 1
```

Fewer accepted rows than the floor emits a failing `accepted_meets_minimum`
check, which obeys the existing `reconciliation.on_mismatch` policy
(`FAIL` by default) and is recorded in `ingest_reconciliation` whether it
passes or fails.

**Opt-in on purpose.** An empty incremental window is legitimate for many
feeds — nothing changed at source. Declaring a floor is a feed asserting
"fewer rows than this means the extract is broken, not that the world was
quiet". Choose it by looking at real history and setting it below the
quietest legitimate window:

```sql
SELECT accepted_count, event_ts FROM <audit_db>.ingest_run_audit
WHERE entity = '<entity>' AND stage = 'raw' AND status = 'SUCCESS'
ORDER BY event_ts DESC LIMIT 20;
```

Measured on **accepted** rows, not curated mutations: a batch where every
row is unchanged (`record_hash` match) publishes nothing and is healthy.

What it catches that nothing else does: a source outage returning empty, a
watermark advanced past real data, a filter that stopped matching, a table
renamed by a source migration.

## Operational override layer (`--override-path`)

A deployed feed config sits behind change control. Operational values —
a lock lease, a reject threshold, an alert webhook, a table name — often
need to change faster than a change order can be raised. `--override-path`
supplies a second HOCON file whose values **always win** over the feed:

```bash
spark-submit ... \
  --files /opt/ingest/conf/feed-smartiq-pdp.conf,/opt/ingest/conf/smartiq-override.conf \
  ... --conf-path ./feed-smartiq-pdp.conf --override-path ./smartiq-override.conf
```

Via the wrappers, set `INGEST_OVERRIDE_FILE` in the env file — the script
ships it with `--files` and passes `--override-path` automatically.

### Semantics

| Behaviour | Rule |
|-----------|------|
| Precedence | The override wins for **every path it declares**, at any depth. There is no path the feed can protect. |
| Objects | Merge — overriding `concurrency.lease_minutes` leaves the rest of `concurrency` untouched. |
| Lists | Replaced wholesale, never appended. |
| New paths | Permitted — an override may introduce a setting the feed never declared. |
| Scope | The whole config root, not just `feeds.*` — `app.spark.session_time_zone` is overridable too. Spark's own `--conf` settings are not: they are the submit command, not this file. |
| Substitutions | Resolved **after** the merge, so `${...}` in either file sees the combined view. |
| Missing file | Hard failure, **CFG_019**. An override that silently did not apply is the worst outcome. |
| Empty / blank path | Treated as "no override"; wrapper variables that expand to empty do not fail the run. |

### Auditability

The layer is deliberately loud, because an unversioned file that changes
behaviour is exactly the drift this framework exists to catch:

- Every overridden path is logged **by name** at WARN under `[Override]`,
  marked `(replaces the feed's value)` or `(new)`.
- Paths that reduce a safety guarantee — `allow_insecure_tls`,
  `on_mismatch`, `max_reject_percent`, `audit.enabled`, `concurrency.lock`,
  `confirm_complete_extract` and similar — are additionally logged as an
  explicit WARNING. Overriding them is permitted; doing so quietly is not.
- The ledger's `config_fingerprint` gains an `+ovr:<digest>` suffix, so an
  overridden run is never indistinguishable from a normal one. The base
  fingerprint hashes key **structure** only and cannot see a changed value —
  this suffix closes that gap.
- **Values are never logged or stored**, only path names and a digest: an
  override file may legitimately carry a credential.
