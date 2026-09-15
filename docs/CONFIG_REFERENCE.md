# Configuration Reference

All configurable options for the ingestion framework, extracted from source code.

**Legend:**
- `<required>` — must be set, no default
- `<optional>` — can be omitted (typically null/empty)
- Value shown — default if omitted

---

## App-Level

Global settings outside the `feeds.*` block.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `app.spark.session_time_zone` | string | `UTC` | Timezone for all framework timestamps (`last_modified_ts`, `create_timestamp`, `load_timestamp`, `ingest_dt`) |

---

## Source — Common

Options under `feeds.<entity>.source`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source.type` | string | `file` | Source type: `file`, `jdbc`, or `kafka` |
| `source.mode` | string | — | Extraction mode (overrides feed-level `mode`) |

---

## Source — File

Options for `source.type = "file"`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source.path` | string | <required> | HDFS/S3 glob pattern for input files |
| `source.file_type` | string | — | `csv`, `json`, `parquet`, `orc` |
| `source.header` | boolean | `false` | Whether CSV has a header row |
| `source.delimiter` | string | `,` | CSV field delimiter |
| `source.quote` | string | `"` | CSV quote character |
| `source.escape` | string | `\\` | CSV escape character |
| `source.multiline` | boolean | `false` | Allow multiline CSV fields |
| `source.drop_trailer` | boolean | `false` | Drop trailer record |

### File Validation (`source.validation.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source.validation.filename_pattern` | string | — | Regex for allowed filenames |
| `source.validation.allowed_extensions` | list | — | Allowed file extensions |
| `source.validation.encoding` | string | — | Expected file encoding (e.g., `UTF-8`) |
| `source.validation.min_size_bytes` | long | — | Minimum file size |
| `source.validation.max_size_bytes` | long | — | Maximum file size |
| `source.validation.delimiter` | string | `,` | Delimiter for header validation |
| `source.validation.header.expected_columns` | int | — | Expected column count |
| `source.validation.header.expected_names` | list | — | Expected column names |
| `source.validation.checksum.sidecar_suffix` | string | `.sha256` | Checksum sidecar file suffix |
| `source.validation.checksum.algorithm` | string | `SHA-256` | Checksum algorithm |
| `source.validation.checksum.required` | boolean | `false` | Fail if checksum missing |
| `source.validation.trailer.required` | boolean | `false` | Require trailer record |
| `source.validation.trailer.marker` | string | — | Trailer line prefix |

### Managed Folders (`source.folders.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source.folders.landing` | string | <required> | Landing folder path |
| `source.folders.inprogress` | string | <required> | In-progress folder path |
| `source.folders.processed` | string | <required> | Processed folder path |
| `source.folders.quarantine` | string | <required> | Quarantine folder path |
| `source.folders.archive` | string | — | Archive folder path |

---

## Source — JDBC

Options for `source.type = "jdbc"`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source.url` | string | <required> | JDBC connection URL |
| `source.dialect` | string | — | SQL dialect (auto-detected from URL) |
| `source.driver` | string | dialect default | JDBC driver class |
| `source.table` | string | — | Source table name |
| `source.query` | string | — | Custom SQL query (alternative to table) |
| `source.sql` | string | — | Alias for `query` |
| `source.where` | string | — | WHERE clause filter (pushed to source) |
| `source.fetchsize` | int | `1000` | JDBC fetch size per partition |
| `source.numPartitions` | int | — | Number of parallel partitions |
| `source.partitionColumn` | string | — | Column for range partitioning |
| `source.lowerBound` | long | — | Partition lower bound |
| `source.upperBound` | long | — | Partition upper bound |
| `source.max_partitions` | int | `64` | Maximum partitions allowed |
| `source.partition_strategy` | string | — | Partitioning strategy |
| `source.skew_metrics` | boolean | `false` | Collect partition skew metrics |
| `source.allow_insecure_tls` | boolean | `false` | Allow insecure TLS connections |
| `source.allow_null_watermark` | boolean | `false` | Allow null watermark values |
| `source.allow_string_watermark` | boolean | `false` | Allow string watermark columns |
| `source.bound_full_load` | boolean | `true` | Apply bounds on full loads |
| `source.clock_drift_warn_ms` | long | `30000` | Warn if source clock drifts beyond this |
| `source.clock_zone` | string | — | Source database timezone |
| `source.companion_timeout_seconds` | int | `300` | Companion query timeout |
| `source.executor_probe` | boolean | `false` | Probe connectivity from executors |
| `source.probe_partitions` | int | `2` | Partitions for executor probe |
| `source.log_sql` | boolean | — | Log generated SQL queries |

### JDBC Auth (`source.auth.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source.auth.user` | string/object | <required> | Username or secret reference |
| `source.auth.password` | string/object | <required> | Password or secret reference |
| `source.auth.client_id` | string | — | Client ID (for Entra auth) |

#### Secret Provider (when `user`/`password` is an object)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `provider` | string | `inline` | `inline`, `env`, `file`, `sysprop`, `cyberark`, `conjur`, `azure_keyvault` |
| `key` | string | <required> | Env var, file path, or property name |
| `path` | string | <required> | File path (for `file` provider) |

#### CyberArk CCP (`provider = "cyberark"`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `url` | string | <required> | CyberArk CCP URL |
| `app_id` | string | <required> | Application ID |
| `safe` | string | <required> | Safe name |
| `object` | string | <required> | Object name |
| `attribute` | string | `Content` | Attribute to retrieve |
| `folder` | string | — | Folder path |
| `cache_ttl_ms` | long | `300000` | Secret cache TTL (5 min) |
| `connect_timeout_ms` | int | `5000` | Connection timeout |
| `read_timeout_ms` | int | `10000` | Read timeout |

#### Conjur (`provider = "conjur"`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `url` | string | <required> | Conjur API URL |
| `account` | string | <required> | Conjur account |
| `variable_id` | string | <required> | Variable ID path |
| `api_key` | string | <required> | API key |
| `max_attempts` | int | `3` | Retry attempts |

#### Azure Key Vault (`provider = "azure_keyvault"`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `vault_url` | string | <required> | Key Vault URL |
| `secret_name` | string | <required> | Secret name |
| `secret_version` | string | — | Specific version (latest if omitted) |

### JDBC Incremental (`source.incremental.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source.incremental.watermark_type` | string | <required> | `TIMESTAMP`, `NUMERIC`, or `COMPOSITE` |
| `source.incremental.watermark_columns` | list | <required> | Column(s) for watermark |
| `source.incremental.initial_value` | string | <required> | Starting watermark value |
| `source.incremental.overlap` | string | — | Seconds to re-read behind watermark |
| `source.incremental.on_unprotected_watermark` | string | `WARN` | Action when watermark unprotected |
| `source.incremental.boundary_convention` | string | — | Boundary comparison convention |

### JDBC Retry (`source.retry.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source.retry.max_attempts` | int | `3` | Maximum retry attempts |
| `source.retry.backoff_ms` | long | — | Backoff between retries |

### JDBC Health Check (`source.health_check.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `source.health_check.enabled` | boolean | — | Enable pre-flight health check |

---

## Schema Contract

Options under `feeds.<entity>.schema`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `schema.version` | string | <required> | Schema version identifier |
| `schema.compatibility` | string | `BACKWARD` | Compatibility mode |
| `schema.on_missing_column` | string | — | Action on missing column |
| `schema.on_extra_column` | string | — | Action on extra column |
| `schema.on_type_change` | string | — | Action on type change |
| `schema.on_order_change` | string | — | Action on column order change |
| `schema.on_duplicate_header` | string | — | Action on duplicate header |
| `schema.on_nullability_violation` | string | — | Action on null in non-null column |
| `schema.on_version_mismatch` | string | — | Action on version mismatch |
| `schema.fail_on_all_null_required_column` | boolean | `false` | Fail if required column all nulls |

### Column Definition (`schema.columns[]`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `name` | string | <required> | Canonical column name |
| `type` | string | <required> | Data type |
| `nullable` | boolean | `true` | Allow nulls |
| `required` | boolean | `false` | Column must be present |
| `aliases` | list | — | Alternative column names |
| `position` | int | — | Expected column position |
| `default` | string | — | Default value if missing |
| `regex` | string | — | Content validation regex |
| `min_length` | int | — | Minimum string length |
| `max_length` | int | — | Maximum string length |
| `nonblank` | boolean | `false` | Reject blank strings |
| `numeric_parse` | boolean | `false` | Validate numeric parsing |
| `transform` | string | — | Transformation expression |
| `category` | string | `business` | Column category |
| `data_type` | string | — | Explicit data type override |
| `sensitivity` | string | — | Data sensitivity level |
| `business_key` | boolean | `false` | Part of business key |
| `incremental` | boolean | `false` | Watermark column |

### Header Validation (`schema.header_validation.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `schema.header_validation.strategy` | string | `NAME_WITH_ALIASES` | Matching strategy |
| `schema.header_validation.on_missing_required` | string | — | Action on missing required |
| `schema.header_validation.on_missing_optional` | string | — | Action on missing optional |
| `schema.header_validation.on_extra_columns` | string | — | Action on extra columns |
| `schema.header_validation.on_duplicate_columns` | string | — | Action on duplicates |
| `schema.header_validation.quarantine_on_failure` | boolean | — | Move bad files to quarantine |
| `schema.header_validation.batch_policy` | string | — | Batch-level handling |
| `schema.header_validation.header_only_policy` | string | — | Header-only file handling |
| `schema.header_validation.repeated_header_policy` | string | — | Repeated header handling |

### Content Validation (`schema.content_validation.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `schema.content_validation.enabled` | boolean | — | Enable content validation |
| `schema.content_validation.mode` | string | — | `SAMPLE` or `FULL` |
| `schema.content_validation.sample_rows` | int | `1000` | Rows to sample |
| `schema.content_validation.maximum_failure_percentage` | string | — | Allowed failure rate |

---

## Raw

Options under `feeds.<entity>.raw`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `raw.database` | string | <required> | Hive database name |
| `raw.table` | string | <required> | Hive table name |
| `raw.path` | string | <required> | HDFS/S3 path for data |
| `raw.format` | string | `orc` | Storage format (`orc`, `parquet`) |
| `raw.strategy` | string | — | `APPEND_BATCH`, `SNAPSHOT`, `CDC_EVENTS` |
| `raw.delivery_mode` | string | — | Delivery mode |

### Raw Partitioning (`raw.partitioning.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `raw.partitioning.keys` | list | — | Partition column names |
| `raw.partitioning.derive.<col>.kind` | string | <required> | `expr` or `literal` |
| `raw.partitioning.derive.<col>.expr` | string | — | SQL expression for derived partition |
| `raw.partitioning.derive.<col>.value` | string | — | Literal value for partition |

---

## Curated

Options under `feeds.<entity>.curated`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.enabled` | boolean | `true` | Enable curated layer |
| `curated.database` | string | <required> | Hive database name |
| `curated.table` | string | <required> | Hive table name |
| `curated.path` | string | <required> | HDFS/S3 path for data |
| `curated.format` | string | `orc` | Storage format |
| `curated.strategy` | string | — | `APPEND`, `TYPE1_MERGE`, `SNAPSHOT_REPLACE` |
| `curated.metadata_columns` | string | — | Metadata column handling |
| `curated.retain_source_metadata` | boolean | `false` | Keep source metadata columns |

### Curated Merge (`curated.merge.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.merge.keys` | list | <required> | Business key columns |
| `curated.merge.allow_empty` | boolean | — | Allow empty key values |
| `curated.merge.enforce_unique_keys` | boolean | — | Validate key uniqueness |
| `curated.merge.require_ordering` | boolean | — | Require deterministic ordering |
| `curated.merge.allow_unsafe_legacy_merge` | boolean | — | Allow legacy merge behavior |
| `curated.merge.max_shrink_percent` | double | — | Max allowed row count reduction |
| `curated.merge.validation_query` | string | — | Post-merge validation SQL |
| `curated.merge.confirm_complete_extract` | boolean | `false` | Confirm full extract for deletes |

### Curated Freshness (`curated.merge.freshness.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.merge.freshness.column` | string | <required> | Freshness comparison column |
| `curated.merge.freshness.compare_as` | string | — | Cast type for comparison |
| `curated.merge.freshness.compare_format` | string | — | Datetime parse pattern |
| `curated.merge.freshness.tie_breakers` | list | — | Secondary ordering columns |

### Curated Deletes (`curated.merge.deletes.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.merge.deletes.mode` | string | `IGNORE` | `IGNORE`, `SOFT`, `HARD`, `FULL_SNAPSHOT_ABSENCE` |
| `curated.merge.deletes.indicator_column` | string | — | Source soft-delete flag column |

### Curated Null Handling (`curated.merge.null_handling.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.merge.null_handling.policy` | string | — | Null key policy |
| `curated.merge.null_handling.drop_null_keys` | boolean | — | Drop rows with null keys |
| `curated.merge.null_handling.treat_blank_as_null` | boolean | — | Treat blanks as nulls |
| `curated.merge.null_handling.acknowledge_unmerged_growth` | boolean | — | Acknowledge unmerged row growth |

### Curated Partitioning (`curated.partitioning.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.partitioning.keys` | list | — | Partition column names |
| `curated.partitioning.null_values` | string | `REJECT` | Null partition key handling |
| `curated.partitioning.default_value` | string | — | Default for null partitions |
| `curated.partitioning.max_affected_partitions` | int | — | Max partitions per run |

### Curated Type Casting (`curated.column_types.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.column_types.<col>` | string | — | Target type for column |
| `curated.on_cast_error` | string | `FAIL` | Action on cast failure |

### Curated Pending (Decoupled) (`curated.pending.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.pending.max_batches` | int | `0` | Max batches per curated run (0=unlimited) |
| `curated.pending.on_failure` | string | `STOP` | `STOP` or `CONTINUE` on batch failure |

### Curated Publish (`curated.publish.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.publish.validation_query` | string | — | Post-publish validation SQL |

### Curated Micro-Batch (`curated.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `curated.allow_full_overwrite_micro_batches` | boolean | — | Allow full overwrite in micro-batch |

---

## Audit

Options under `feeds.<entity>.audit`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `audit.enabled` | boolean | `true` | Enable audit logging |
| `audit.database` | string | `ingest_audit` | Audit database name |
| `audit.run_table` | string | `ingest_run_audit` | Run audit table name |
| `audit.control_total_expr` | string | — | Control total SQL expression |

### Reconciliation (`audit.reconciliation.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `audit.reconciliation.on_mismatch` | string | — | `WARN`, `FAIL`, or expression |
| `audit.reconciliation.min_accepted_rows` | long | — | Minimum rows to accept |

---

## Rejects

Options under `feeds.<entity>.rejects`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `rejects.database` | string | — | Reject table database |
| `rejects.table` | string | — | Reject table name |
| `rejects.use_contract_nullability` | boolean | `false` | Generate rules from schema |
| `rejects.max_reject_count` | long | — | Max rejected rows allowed |
| `rejects.max_reject_percent` | double | — | Max reject percentage allowed |
| `rejects.on_reject_watermark` | string | — | Watermark handling for rejects |
| `rejects.payload` | string | — | Payload column for rejected data |

### Reject Rules (`rejects.rules[]`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `name` | string | <required> | Rule name |
| `condition` | string | <required> | SQL condition (true=reject) |
| `error_code` | string | — | Error code (defaults to name) |
| `message` | string | — | Error message |

---

## Idempotency

Options under `feeds.<entity>.idempotency`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `idempotency.database` | string | — | Registry database |
| `idempotency.registry_table` | string | `ingest_file_registry` | File registry table |
| `idempotency.duplicate_policy` | string | — | `SKIP`, `REJECT`, `REPROCESS_WITH_APPROVAL` |

---

## Concurrency

Options under `feeds.<entity>.concurrency`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `concurrency.lock` | string | `REQUIRED` | `REQUIRED`, `OPTIONAL`, `DISABLED` |
| `concurrency.provider` | string | `HIVE` | Lock provider: `HIVE` or `JDBC` |
| `concurrency.database` | string | — | Lock database (defaults to audit.database) |
| `concurrency.table` | string | `ingest_run_locks` | Lock table name |
| `concurrency.lease_minutes` | long | `240` | Lock lease duration (minutes) |
| `concurrency.settle_ms` | long | `2000` | Settlement wait after acquire |
| `concurrency.stale_heartbeat_intervals` | int | `3` | Intervals before stale takeover (0=disable) |
| `concurrency.wait_ms` | long | `0` | Wait time for lock acquisition |

### JDBC Lock (`concurrency.jdbc.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `concurrency.jdbc.url` | string | <required> | JDBC URL for lock database |
| `concurrency.jdbc.user` | string | — | Username |
| `concurrency.jdbc.password` | string | — | Password |
| `concurrency.jdbc.driver` | string | — | JDBC driver class |
| `concurrency.jdbc.table` | string | `ingestion_lock` | Lock table name |
| `concurrency.jdbc.lease_minutes` | long | `240` | Lease duration |

---

## Notifications

Options under `feeds.<entity>.notifications`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `notifications.enabled` | boolean | — | Enable notifications |
| `notifications.on` | list | `["FAILURE"]` | Trigger events: `FAILURE`, `SUCCESS` |
| `notifications.command` | string | — | Shell command to execute |

### Webhook (`notifications.webhook.*`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `notifications.webhook.url` | string | — | Webhook URL |
| `notifications.webhook.timeout_ms` | long | — | Request timeout |

---

## Source Reconciliation

Options under `feeds.<entity>.reconcile`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `reconcile.enabled` | boolean | — | Enable source reconciliation |
| `reconcile.on_mismatch` | string | `REPORT` | `REPORT` or `FAIL` |

---

## Watermark

Options under `feeds.<entity>.watermark`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `watermark.advance_after` | string | — | `RAW` or `CURATED` |
| `watermark.watermark_commit` | boolean | — | Commit watermark after stage |
| `watermark.watermark_name` | string | — | Custom watermark identifier |

---

## Ingestion Execution

Options under `feeds.<entity>.ingestion`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `ingestion.execution` | string | `COUPLED` | `COUPLED` or `DECOUPLED` |
| `ingestion.pattern` | string | — | Ingestion pattern name |
| `ingestion.offsets.track` | boolean | `false` | Track Kafka offsets |

---

## Retention

Options under `feeds.<entity>.retention`.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `retention.raw` | string | — | Raw retention policy |
| `retention.curated` | string | — | Curated retention policy |
| `retention.rejects` | string | — | Rejects retention policy |
| `retention.database` | string | `ingest_audit` | Retention metadata database |
| `retention.watermarks_keep_last` | int | — | Watermark versions to keep |

---

## Record Hash

Options for change detection.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `record_hash` | boolean | `false` | Enable record hash for change detection |
| `record_hash_options.trim` | boolean | — | Trim strings before hashing |
| `record_hash_options.uppercase` | boolean | — | Uppercase strings before hashing |
| `source_key_json` | boolean | `false` | JSON-encode composite keys |
| `source_modified_column` | string | — | Source modification timestamp column |
| `source_operation_default` | string | — | Default operation code |
| `source_operation_legacy_insert` | boolean | `false` | Legacy insert operation handling |
| `lineage_extended` | boolean | `false` | Extended lineage tracking |

---

*Auto-generated from source. Run `docs/scripts/extract_config.py` to regenerate.*
