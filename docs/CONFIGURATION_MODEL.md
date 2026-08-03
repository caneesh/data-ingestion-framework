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
    database = "ingest_audit"
    reconciliation {
      on_mismatch = "FAIL"             # WARN | FAIL | FAIL_ON equations
      tolerance { raw_equals_accepted = { percent = 0.0 } }
    }
  }

  notifications {                      # reserved: design-level section, not yet wired
    on_failure = ["ops-alerts"]
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
job's batch checkpoint. See docs/DECOUPLING_DESIGN.md and the runbook's
Control-M section; wrapper scripts live in `scripts/`.

CDC-events example and a file-feed example live in `application.conf`'s
commented blocks; the interactive generator (`ingestion-config-gen`)
produces feeds that pass this validator by construction.
