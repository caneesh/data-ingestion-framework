# SmartIQ_PDP mapping v2 — all source columns to raw AND curated

**Change (2026-08-04, consumer request):** curated now carries **all 364**
source columns (was 357). The 7 columns the original business tab dropped —
`FileName`, `FormGuid`, `Form`, `UserEmailId`, `FirstName`, `LastName`,
`Matrix` — are restored. Raw was already all 364 and is unchanged.

| Artifact | Change |
|---|---|
| `hive_raw_curated_mapping_v2_all_columns.xlsx` | 7 rows flipped `In_Curated` N→Y with curated column/type/cast rule filled in, `Disposition` OOS→PASSTHROUGH, per-row notes; ReadMe ZONES corrected to 364; open items 1 (grain/key) and 2 (PII) updated; change-log row appended |
| `curated_ddl.sql` | all 364 data columns + `record_hash` + framework audit columns; unpartitioned |
| `smartiq-pdp-schema.conf` | contract v2.0 — trim/empty→NULL transform now on every varchar column (361 of 364; the 3 typed source columns are the exceptions) |
| `feed-smartiq-pdp.conf` | header rewritten: nothing is dropped, so alignment discards nothing; privacy note |
| `raw_ddl.sql` | unchanged (already all 364 + framework metadata) |

**Unchanged decisions:** `file_name` is the business key;
`last_modified_datetime` is the freshness column, compared as `timestamp`
via `freshness.compare_as` regardless of physical storage type.

## Privacy — needs confirmation

Carrying every column means **submitter identity (`user_email_id`,
`first_name`, `last_name`) now persists in curated**, alongside the
pre-existing preparer / marketing-rep / underwriter contact columns.
12 columns are tagged `sensitivity = "PII"` in the contract, so reject
payloads mask them — but the curated **table** holds them in the clear.
Confirm with Privacy and restrict table access accordingly.

## Guard

`SmartIqMappingConfigTest` (ingestion-core) pins this: every contract
column must exist in both DDLs, the identity columns must be PII-tagged,
the merge key and freshness column must exist in the curated target,
`record_hash` must be present, and the curated table must stay
unpartitioned. The example cannot silently drift from the framework.

## 2026-08-05 — real Hive database names + table-identity fix

- Databases set to the actual targets: `membership_common_raw` and
  `membership_common_curated` (both spellings confirmed as *membership*).
- **Defect fixed:** the DDLs created `raw_smartiq_pdp` / `curated_smartiq_pdp`
  while the feed config pointed at table `smartiq_pdp` — they would never
  have matched, leaving the pre-created tables unused while the framework
  silently created its own. Both DDLs now create `smartiq_pdp` (the zone
  already lives in the database name, so the prefixes were redundant too).
  `SmartIqMappingConfigTest` now asserts DDL table identity == feed config
  `database.table`, so this cannot regress.
- Storage decision recorded: **EXTERNAL, Parquet, non-ACID** for both zones.
  Hive ACID is not viable here — Spark 3.5 cannot write Hive transactional
  tables without the Hive Warehouse Connector, full-ACID requires ORC (these
  are Parquet), and the framework issues no row-level DML for ACID to serve.
  EXTERNAL also protects raw (the replay source) from an accidental DROP and
  avoids the Hive 3 default where a plain CREATE TABLE is managed+transactional.
  Accepted trade-off: readers can see a partial table during the curated
  `INSERT OVERWRITE` swap. Real ACID/snapshot isolation is the Delta/Iceberg
  decision (plan item #19), not Hive ACID.
- `ingest_audit` (ledger, rejects, watermark store) left unchanged — confirm
  whether it should also move under the membership_common_* naming.
